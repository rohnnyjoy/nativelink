// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! What this worker can see of its own CPU and memory, and what it should
//! advertise from that.
//!
//! Every deployment so far typed the advertisement by hand next to the pod's
//! limits, and the two drifted: chinchilla advertised 16 cores and 60 GiB
//! inside 15-core, 56 GiB limits. The limit is the number the kernel
//! enforces, so the advertisement is derived from it here.

use core::hash::BuildHasher;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nativelink_config::cas_server::{
    CapacityConfig, CpuUnit, MemoryEnforcement, ResourceEnforcementConfig, WorkerProperty,
};
use nativelink_error::{Code, Error, make_err};
use tracing::{info, warn};

/// CPU and memory as the cgroup (or, failing a limit, the host) reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedCapacity {
    pub cpu_millicores: u64,
    pub memory_kb: u64,
}

/// `cpu.max` is `"<quota> <period>"` in microseconds, or `"max <period>"` when
/// unlimited; unlimited means the host's cores.
pub fn parse_cpu_max(cpu_max: &str, host_cpus: u64) -> Option<u64> {
    let mut fields = cpu_max.split_whitespace();
    let quota = fields.next()?;
    if quota == "max" {
        return Some(host_cpus.saturating_mul(1000));
    }
    let quota: u64 = quota.parse().ok()?;
    let period: u64 = fields.next()?.parse().ok()?;
    if period == 0 {
        return None;
    }
    Some(quota.saturating_mul(1000) / period)
}

/// `memory.max` is bytes, or `"max"` when unlimited; unlimited means the
/// host's memory.
pub fn parse_memory_max(memory_max: &str, host_memory_kb: u64) -> Option<u64> {
    let value = memory_max.trim();
    if value == "max" {
        return Some(host_memory_kb);
    }
    value.parse::<u64>().ok().map(|bytes| bytes / 1024)
}

/// The number after `key` on the first line that starts with it, as
/// `/proc/meminfo` (`MemTotal:  65536000 kB`) and a cgroup's `memory.stat`
/// (`inactive_file 18672467968`) both lay them out. A key without its own
/// colon must be followed by whitespace, so `inactive_file` does not match
/// `inactive_file_extra`.
fn keyed_value(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?;
        if !key.ends_with(':') && !rest.starts_with(char::is_whitespace) {
            return None;
        }
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// `MemTotal` from `/proc/meminfo`, in KiB.
pub fn parse_meminfo_total_kb(meminfo: &str) -> Option<u64> {
    keyed_value(meminfo, "MemTotal:")
}

/// The headroom the advertisement is divided by: the block's own figure
/// when set, else the enforcement headroom while memory enforcement is on,
/// so the two numbers cannot drift apart, else nothing.
pub fn memory_headroom_percent(
    config: &CapacityConfig,
    enforcement: Option<&ResourceEnforcementConfig>,
) -> u64 {
    config.memory_headroom_percent.unwrap_or_else(|| {
        enforcement
            .filter(|enforcement| enforcement.memory == MemoryEnforcement::Soft)
            .map_or(0, |enforcement| enforcement.memory_headroom_percent)
    })
}

/// The observed capacity less the worker's own share, memory divided by
/// `memory_headroom_percent`. Returns `(cpu, memory_kb)` with the CPU on
/// the scale `cpu_unit` names: whole cores rounded down, so a 14-core pod
/// keeping one core back advertises `13`, not `13000`, to a scheduler
/// whose actions ask for `cpu_count=1`.
pub const fn advertised(
    observed: ObservedCapacity,
    config: &CapacityConfig,
    memory_headroom_percent: u64,
) -> (u64, u64) {
    let cpu_millicores = observed
        .cpu_millicores
        .saturating_sub(config.overhead_cpu_millicores);
    let cpu = match config.cpu_unit {
        CpuUnit::Cores => cpu_millicores / 1000,
        CpuUnit::Millicores => cpu_millicores,
    };
    let memory = observed
        .memory_kb
        .saturating_sub(config.overhead_memory_kb)
        .saturating_mul(100)
        / (100 + memory_headroom_percent);
    (cpu, memory)
}

/// What to do when the cgroup cannot be read: the configured properties
/// stand if they carry both numbers, since an operator who kept them has a
/// worker that still takes work; a worker with neither would register and
/// then satisfy no action that asks for CPU or memory, idling for good on
/// one warning line, so that is refused.
pub fn without_cgroup<S: BuildHasher>(
    config: &CapacityConfig,
    properties: &HashMap<String, WorkerProperty, S>,
) -> Result<(), Error> {
    let missing: Vec<&str> = [&config.cpu_property_name, &config.memory_property_name]
        .into_iter()
        .filter(|name| !properties.contains_key(name.as_str()))
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        warn!(
            "capacity is configured but no cgroup v2 limit applies to the worker (see the line above for the cgroup looked at); advertising the configured platform_properties instead"
        );
        return Ok(());
    }
    Err(make_err!(
        Code::FailedPrecondition,
        "capacity is configured but no cgroup v2 limit applies to the worker (not Linux, cgroup v1, no permission, or no limit at any level; see the line above), and platform_properties carries no {}: set them, or drop the capacity block",
        missing.join(" or ")
    ))
}

/// The cgroup v2 path of this process from `/proc/self/cgroup`: the `0::`
/// line, which is `/` inside a container with its own cgroup namespace and
/// the full `/kubepods.slice/.../cri-containerd-<id>.scope` path in one that
/// shares the host's, as a privileged container does.
pub fn parse_self_cgroup(self_cgroup: &str) -> Option<&str> {
    self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
        .filter(|path| path.starts_with('/'))
}

/// Where this process's own `cpu.max`, `memory.max` and `memory.current`
/// live: the mount root joined with the path from `/proc/self/cgroup` when
/// that directory exists under the mount, the mount root otherwise. A
/// container with a private cgroup namespace sees itself at the root, so its
/// path is `/` and both agree. A privileged container sees the host's tree,
/// where the root's files are the host's (or absent, as on a systemd host),
/// and its own numbers sit further down.
pub fn resolve_cgroup_dir(
    mount_root: &Path,
    self_cgroup: Option<&str>,
    is_dir: impl Fn(&Path) -> bool,
) -> PathBuf {
    if let Some(path) = self_cgroup.and_then(parse_self_cgroup) {
        let own = mount_root.join(path.trim_start_matches('/'));
        if is_dir(&own) {
            return own;
        }
    }
    mount_root.to_path_buf()
}

/// This process's cgroup directory on the live system.
fn cgroup_dir() -> PathBuf {
    let self_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok();
    resolve_cgroup_dir(Path::new("/sys/fs/cgroup"), self_cgroup.as_deref(), |dir| {
        dir.join("memory.max").is_file()
    })
}

/// Whether a `cpu.max` or `memory.max` value is "no limit": `max`, or for
/// `cpu.max` `max <period>`.
fn is_unlimited(value: &str) -> bool {
    value.split_whitespace().next() == Some("max")
}

/// The first limit for `file` from `dir` up to and including `mount_root`,
/// with the directory it was found in. The process's own cgroup is often
/// an unlimited child of a limited parent (a systemd scope in a pod, a
/// split cgroup, a privileged container under `kubepods.slice`), and the
/// parent's limit is the one the kernel enforces on it. `None` when every
/// level says `max` or nothing is readable: no limit applies.
pub fn find_limit(
    dir: &Path,
    mount_root: &Path,
    file: &str,
    read: impl Fn(&Path) -> Option<String>,
) -> Option<(PathBuf, String)> {
    let mut at = dir;
    loop {
        if let Some(value) = read(&at.join(file))
            && !is_unlimited(value.trim())
        {
            return Some((at.to_path_buf(), value));
        }
        if at == mount_root || !at.starts_with(mount_root) {
            return None;
        }
        at = at.parent()?;
    }
}

fn read_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Reads the limits that apply to the worker: for CPU and for memory, the
/// nearest limit from its own cgroup up to the mount root. `None` where no
/// limit applies at any level (not Linux, cgroup v1, no permission, or a
/// host where nothing is limited), so the configured properties stand.
pub fn observe_cgroup() -> Option<ObservedCapacity> {
    let host_cpus = std::thread::available_parallelism().map_or(1, |n| n.get() as u64);
    let host_memory_kb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|meminfo| parse_meminfo_total_kb(&meminfo))
        .unwrap_or(0);
    let mount_root = Path::new("/sys/fs/cgroup");
    let dir = cgroup_dir();
    let cpu = find_limit(&dir, mount_root, "cpu.max", read_file);
    let memory = find_limit(&dir, mount_root, "memory.max", read_file);
    if cpu.is_none() && memory.is_none() {
        warn!(
            cgroup = %dir.display(),
            "no cgroup v2 cpu.max or memory.max limit from this cgroup up to the mount root; keeping the configured platform_properties"
        );
        return None;
    }
    let cpu_max = cpu.map_or_else(|| "max".to_string(), |(_, value)| value);
    let memory_max = memory.map_or_else(|| "max".to_string(), |(_, value)| value);
    Some(ObservedCapacity {
        cpu_millicores: parse_cpu_max(&cpu_max, host_cpus)?,
        memory_kb: parse_memory_max(&memory_max, host_memory_kb)?,
    })
}

/// Sets the CPU and memory properties to what the cgroup allows. When
/// nothing can be read, the configured properties stand if they carry both,
/// and the worker fails to start if they do not. Returns what was
/// advertised.
pub fn apply<S: BuildHasher>(
    config: &CapacityConfig,
    memory_headroom_percent: u64,
    properties: &mut HashMap<String, WorkerProperty, S>,
) -> Result<Option<(u64, u64)>, Error> {
    let Some(observed) = observe_cgroup() else {
        without_cgroup(config, properties)?;
        return Ok(None);
    };
    let (cpu, memory_kb) = advertised(observed, config, memory_headroom_percent);
    properties.insert(
        config.cpu_property_name.clone(),
        WorkerProperty::Values(vec![cpu.to_string()]),
    );
    properties.insert(
        config.memory_property_name.clone(),
        WorkerProperty::Values(vec![memory_kb.to_string()]),
    );
    info!(
        observed_cpu_millicores = observed.cpu_millicores,
        observed_memory_kb = observed.memory_kb,
        cpu,
        cpu_unit = ?config.cpu_unit,
        memory_kb,
        memory_headroom_percent,
        "Advertising capacity from the cgroup"
    );
    Ok(Some((cpu, memory_kb)))
}

pub fn parse_memory_current_kb(memory_current: &str) -> Option<u64> {
    memory_current
        .trim()
        .parse::<u64>()
        .ok()
        .map(|bytes| bytes / 1024)
}

/// `MemAvailable` from `/proc/meminfo`, in KiB.
pub fn parse_meminfo_available_kb(meminfo: &str) -> Option<u64> {
    keyed_value(meminfo, "MemAvailable:")
}

/// The page cache in a cgroup's `memory.stat` the kernel can drop at once,
/// in KiB: `inactive_file`, the cache it reclaims first and what kubelet
/// leaves out of the working set it evicts on, less `file_dirty` and
/// `file_writeback`, which cannot be dropped until they reach the disk
/// (kubelet does not take those off, so this reads up to that much lower).
/// Dirty pages may sit on the active list instead; taking all of them off
/// the inactive one errs towards less free, never more. `None` without an
/// `inactive_file` line.
pub fn parse_memory_stat_reclaimable_kb(memory_stat: &str) -> Option<u64> {
    let inactive_file = keyed_value(memory_stat, "inactive_file")?;
    let dirty = keyed_value(memory_stat, "file_dirty").unwrap_or(0);
    let writeback = keyed_value(memory_stat, "file_writeback").unwrap_or(0);
    Some(
        inactive_file
            .saturating_sub(dirty)
            .saturating_sub(writeback)
            / 1024,
    )
}

/// Memory in a cgroup's `memory.stat` the kernel cannot drop to make room,
/// in KiB: `anon` (the processes' own pages), `shmem` (tmpfs and shared
/// memory, file pages with nowhere to go without swap), `kernel` (stacks,
/// page tables, unreclaimable slab and the rest the kernel charges),
/// and `sock` (network buffers). Page cache is left out, dirty pages
/// included: the kernel reclaims clean cache before it gives up and kills,
/// and holds writers back for dirty pages rather than killing over them,
/// while the worker's own CAS writes would count against every action.
/// Kernels
/// before 5.18 have no `kernel` line; there the parts it sums
/// (`kernel_stack`, `pagetables`, `percpu`, `slab_unreclaimable`) are
/// added instead. `None` without an `anon` line.
pub fn parse_memory_stat_unreclaimable_kb(memory_stat: &str) -> Option<u64> {
    let anon = keyed_value(memory_stat, "anon")?;
    let value = |key: &str| keyed_value(memory_stat, key).unwrap_or(0);
    let kernel = keyed_value(memory_stat, "kernel").unwrap_or_else(|| {
        ["kernel_stack", "pagetables", "percpu", "slab_unreclaimable"]
            .into_iter()
            .map(value)
            .fold(0, u64::saturating_add)
    });
    let bytes = [anon, value("shmem"), kernel, value("sock")]
        .into_iter()
        .fold(0, u64::saturating_add);
    Some(bytes / 1024)
}

/// A cgroup's `memory.oom.group`: `true` when the kernel's OOM killer takes
/// every process in the cgroup at once (what Kubernetes sets on a container
/// unless the kubelet's `singleProcessOOMKill` is on), `false` when it
/// takes one. `None` when the file does not parse.
pub fn parse_memory_oom_group(memory_oom_group: &str) -> Option<bool> {
    match memory_oom_group.trim() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

/// The `oom_kill` count from a cgroup's `memory.events`: how many
/// processes in it, or below it, any OOM killer has taken. `None` without
/// the line.
pub fn parse_memory_events_oom_kill(memory_events: &str) -> Option<u64> {
    keyed_value(memory_events, "oom_kill")
}

/// The room the worker gives its actions: the cgroup's memory limit less
/// what it keeps for itself.
pub const fn memory_room_kb(limit_kb: u64, overhead_memory_kb: u64) -> u64 {
    limit_kb.saturating_sub(overhead_memory_kb)
}

/// The memory an action is killed at: its limit plus `headroom_percent`,
/// and never past `room_kb`, the worker's room when it is known. Without
/// the cap, the line for an action booked at the whole worker would sit
/// past the cgroup's own limit, so the kernel would take the worker before
/// the worker took the action.
pub fn memory_kill_line_kb(limit_kb: u64, headroom_percent: u64, room_kb: Option<u64>) -> u64 {
    let line = limit_kb.saturating_mul(100 + headroom_percent) / 100;
    room_kb.map_or(line, |room_kb| line.min(room_kb))
}

/// The nearest cgroup with a memory limit, from the worker's own up to the
/// mount root, and that limit: the level whose usage the kernel holds to
/// it, and so the one whose `memory.stat` and `memory.events` matter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitedCgroup {
    pub dir: PathBuf,
    pub limit_kb: u64,
}

impl LimitedCgroup {
    /// From what `find_limit` returned for `memory.max`.
    pub fn from_limit((dir, max): (PathBuf, String)) -> Option<Self> {
        let limit_kb = parse_memory_current_kb(max.trim())?;
        Some(Self { dir, limit_kb })
    }

    /// What the cgroup cannot reclaim now, in KiB.
    pub fn unreclaimable_kb(&self, read: impl Fn(&Path) -> Option<String>) -> Option<u64> {
        read(&self.dir.join("memory.stat"))
            .and_then(|stat| parse_memory_stat_unreclaimable_kb(&stat))
    }

    /// Memory the worker could still give an action against this cgroup's
    /// limit, in KiB: the limit less the working set (usage less the page
    /// cache the kernel can drop), as `free_memory_kb_from` reads a limited
    /// cgroup. For a cgroup whose limit is stated rather than carried, this
    /// is the figure the keepalive and admission report, so the scheduler's
    /// veto and the guard measure against the same number.
    pub fn free_memory_kb(&self, read: impl Fn(&Path) -> Option<String>) -> Option<u64> {
        let current_kb = read(&self.dir.join("memory.current"))
            .and_then(|current| parse_memory_current_kb(&current))?;
        let reclaimable_kb = read(&self.dir.join("memory.stat"))
            .and_then(|stat| parse_memory_stat_reclaimable_kb(&stat));
        free_memory_kb_from(Some(self.limit_kb), Some(current_kb), reclaimable_kb, None)
    }

    /// Whether the kernel's OOM killer would take this cgroup whole
    /// (`memory.oom.group`).
    pub fn oom_group(&self, read: impl Fn(&Path) -> Option<String>) -> Option<bool> {
        read(&self.dir.join("memory.oom.group")).and_then(|value| parse_memory_oom_group(&value))
    }

    /// The cgroup's OOM kills so far.
    pub fn oom_kills(&self, read: impl Fn(&Path) -> Option<String>) -> Option<u64> {
        read(&self.dir.join("memory.events"))
            .and_then(|events| parse_memory_events_oom_kill(&events))
    }
}

/// The worker's [`LimitedCgroup`] on the live system; `None` where no
/// memory limit applies, and always off Linux.
#[cfg(target_os = "linux")]
pub fn limited_cgroup() -> Option<LimitedCgroup> {
    find_limit(
        &cgroup_dir(),
        Path::new("/sys/fs/cgroup"),
        "memory.max",
        read_file,
    )
    .and_then(LimitedCgroup::from_limit)
}

#[cfg(not(target_os = "linux"))]
pub const fn limited_cgroup() -> Option<LimitedCgroup> {
    None
}

/// The cgroup the pressure guard measures: the worker's [`LimitedCgroup`]
/// where a memory limit applies, else its own cgroup against `limit_kb`
/// when that is set (a container without a memory limit, whose cgroup
/// still reports `memory.stat` and `memory.events`: the pod's budget on
/// its node, which the configuration states). `None` with neither, and
/// always off Linux.
#[cfg(target_os = "linux")]
pub fn guarded_cgroup(limit_kb: u64) -> Option<LimitedCgroup> {
    limited_cgroup().or_else(|| {
        (limit_kb > 0).then(|| LimitedCgroup {
            dir: cgroup_dir(),
            limit_kb,
        })
    })
}

#[cfg(not(target_os = "linux"))]
pub const fn guarded_cgroup(_limit_kb: u64) -> Option<LimitedCgroup> {
    None
}

/// What the worker reports free: against the guard's cgroup when the guard
/// has one (the limited cgroup, or the worker's own against a stated
/// limit), else `free_memory_kb`.
pub fn free_memory_kb_guarded(guard: Option<&LimitedCgroup>) -> Option<u64> {
    match guard {
        Some(cgroup) => cgroup.free_memory_kb(read_cgroup_file),
        None => free_memory_kb(),
    }
}

/// Reads a cgroup file on the live system, for the readers above.
pub fn read_cgroup_file(path: &Path) -> Option<String> {
    read_file(path)
}

/// Memory the worker could still give an action: the cgroup limit less its
/// working set (current usage less the page cache the kernel can drop)
/// when there is a limit, the host's `MemAvailable` when there is not,
/// nothing when neither can be read. `memory.current` charges every page
/// of file cache the worker's I/O has touched, and the kernel only reclaims
/// it near the limit, so a busy worker's usage sits just under its limit
/// with little of it in use.
pub const fn free_memory_kb_from(
    limit_kb: Option<u64>,
    current_kb: Option<u64>,
    reclaimable_kb: Option<u64>,
    available_kb: Option<u64>,
) -> Option<u64> {
    match (limit_kb, current_kb) {
        (Some(limit), Some(current)) => {
            let reclaimable = match reclaimable_kb {
                Some(kb) => kb,
                None => 0,
            };
            Some(limit.saturating_sub(current.saturating_sub(reclaimable)))
        }
        _ => available_kb,
    }
}

/// Free memory measured against `limited`, the nearest limited cgroup and
/// its `memory.max` as `find_limit` returns them, with `read` for the
/// files; the host's `MemAvailable` when there is no limit.
pub fn free_memory_kb_with(
    limited: Option<&(PathBuf, String)>,
    read: impl Fn(&Path) -> Option<String>,
) -> Option<u64> {
    let Some((dir, max)) = limited else {
        return read(Path::new("/proc/meminfo"))
            .and_then(|meminfo| parse_meminfo_available_kb(&meminfo));
    };
    let read_current =
        || read(&dir.join("memory.current")).and_then(|current| parse_memory_current_kb(&current));
    // The usage is read on both sides of the cache, and the larger taken.
    // That mitigates the common races, cache that grows during the read or
    // is reclaimed during it, but the three reads are not one snapshot:
    // cache that grows at the `memory.stat` read and is reclaimed before
    // the second usage read is missed by both usage reads yet subtracted,
    // so a transient cache peak can report more free than there is. The
    // cache figure also comes from per-CPU counters the kernel flushes in
    // batches, so it can run ahead of what is there by a few MiB per CPU.
    let current_before = read_current();
    let reclaimable_kb =
        read(&dir.join("memory.stat")).and_then(|stat| parse_memory_stat_reclaimable_kb(&stat));
    let current_kb = current_before.max(read_current());
    match (parse_memory_current_kb(max.trim()), current_kb) {
        (Some(limit_kb), Some(current_kb)) => {
            free_memory_kb_from(Some(limit_kb), Some(current_kb), reclaimable_kb, None)
        }
        _ => read(Path::new("/proc/meminfo"))
            .and_then(|meminfo| parse_meminfo_available_kb(&meminfo)),
    }
}

/// Whether `dir`, the worker's own cgroup, carries a memory limit of its
/// own with readable usage: then nothing outside the worker gives memory
/// back, so what it reads free while idle is the most it will read. A limit
/// only on an ancestor (a shared `kubepods.slice`, say) is shared with
/// other workloads, which can release memory, and without readable usage
/// free memory falls back to the host's `MemAvailable`; neither counts.
pub fn own_memory_limit_in(dir: &Path, read: impl Fn(&Path) -> Option<String>) -> bool {
    let limited = read(&dir.join("memory.max"))
        .is_some_and(|max| parse_memory_current_kb(max.trim()).is_some());
    let usage = read(&dir.join("memory.current"))
        .is_some_and(|current| parse_memory_current_kb(&current).is_some());
    limited && usage
}

/// `own_memory_limit_in` for this process's cgroup.
#[cfg(target_os = "linux")]
pub fn memory_is_limited() -> bool {
    own_memory_limit_in(&cgroup_dir(), read_file)
}

#[cfg(not(target_os = "linux"))]
pub const fn memory_is_limited() -> bool {
    false
}

/// What the worker reports on each keepalive. Linux only; elsewhere the
/// worker reports nothing and is never vetoed.
#[cfg(target_os = "linux")]
pub fn free_memory_kb() -> Option<u64> {
    // The limit and the usage counted against it come from the same level:
    // the nearest limited ancestor, which charges everything below it.
    let limited = find_limit(
        &cgroup_dir(),
        Path::new("/sys/fs/cgroup"),
        "memory.max",
        read_file,
    );
    free_memory_kb_with(limited.as_ref(), read_file)
}

#[cfg(not(target_os = "linux"))]
pub const fn free_memory_kb() -> Option<u64> {
    None
}
