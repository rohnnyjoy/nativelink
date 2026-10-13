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

//! The worker's memory pressure guard.
//!
//! The scheduler places an action by its request, its expected peak, and
//! the worker kills an action that passes its own limit. Requests are
//! expectations, so the actions on a worker can together use more than the
//! sum of their requests while each is within its limit, and the cgroup's
//! limit would then take the whole worker with every action on it. The
//! guard reads what the cgroup cannot reclaim and, near the limit, kills
//! one action: the one furthest above its own request, else the youngest,
//! which loses the least work. An action whose request is at or above its
//! limit was booked at its kill line by the scheduler (an escalated or
//! requeued run) and is never chosen.

use core::time::Duration;
use std::sync::Arc;
use std::time::Instant;

use nativelink_config::cas_server::{MemoryEnforcement, MemoryPressureConfig};
use nativelink_util::action_messages::OperationId;
use tracing::{debug, info, warn};

use crate::capacity::LimitedCgroup;
use crate::running_actions_manager::RunningActionsManagerImpl;

/// The longest the guard waits after a kill for the victim to leave before
/// it judges again: a multi-GiB tree takes a moment to die and give its
/// pages back, and judging before that would take a second action for the
/// first one's pages. The guard waits until the victim is no longer among
/// the running actions, or this long.
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether the guard may judge again after a kill at `since`: once the
/// victim is no longer running (`victim_running`, the manager's word: its
/// tree reaped and its result handled, not merely chosen, which takes it
/// out of the candidates at once), or `SETTLE_TIMEOUT` has passed.
pub fn settled(victim_running: bool, since: Instant, now: Instant) -> bool {
    !victim_running || now.saturating_duration_since(since) >= SETTLE_TIMEOUT
}

/// One running action as the guard weighs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PressureCandidate {
    pub operation_id: OperationId,
    /// When its command started.
    pub started: Instant,
    /// KiB the scheduler placed it by; 0 when it carries none.
    pub request_kb: u64,
    /// KiB its kill line is computed from; 0 when it carries none.
    pub limit_kb: u64,
    /// Its process tree at the last sample, in KiB.
    pub last_memory_kb: u64,
}

impl PressureCandidate {
    /// An action whose request is at or above its limit, the scheduler
    /// having booked it at its kill line (a class, an escalation or a
    /// pressure requeue), while it uses no more than that request: what it
    /// was booked for is paid, so it is never the guard's. Past its request
    /// (in its own headroom) it competes like any other, furthest above
    /// its request first.
    pub const fn is_protected(&self) -> bool {
        self.limit_kb > 0
            && self.request_kb >= self.limit_kb
            && self.last_memory_kb <= self.request_kb
    }

    /// KiB the action is above its request; 0 when it is not.
    pub const fn over_request_kb(&self) -> u64 {
        self.last_memory_kb.saturating_sub(self.request_kb)
    }
}

/// The action the guard kills: of those not protected, the one furthest
/// above its own request; when none is above it, the youngest. `None` when
/// every action is protected or none is running.
pub fn choose_victim(candidates: &[PressureCandidate]) -> Option<&PressureCandidate> {
    let unprotected = || candidates.iter().filter(|c| !c.is_protected());
    unprotected()
        .filter(|c| c.over_request_kb() > 0)
        .max_by_key(|c| c.over_request_kb())
        .or_else(|| unprotected().max_by_key(|c| c.started))
}

/// Whether `unreclaimable_kb` has reached `limit_kb` less `watermark_kb`.
pub const fn over_watermark(unreclaimable_kb: u64, limit_kb: u64, watermark_kb: u64) -> bool {
    unreclaimable_kb >= limit_kb.saturating_sub(watermark_kb)
}

/// Memory the running actions are still expected to take, in KiB: the sum
/// over `(request_kb, last_memory_kb)` of the request less the last sample,
/// where the request is the larger.
pub fn owed_kb(actions: impl IntoIterator<Item = (u64, u64)>) -> u64 {
    actions
        .into_iter()
        .map(|(request_kb, last_memory_kb)| request_kb.saturating_sub(last_memory_kb))
        .fold(0, u64::saturating_add)
}

/// The guard's configuration when it is to run: `memory_pressure` set,
/// memory enforcement soft, and a limit to measure against: either
/// `memory_limited`, the worker's own cgroup carrying one
/// (`crate::capacity::memory_is_limited`), or the configuration's
/// `limit_kb` for a cgroup that carries none.
pub fn configured(
    enforcement: Option<&nativelink_config::cas_server::ResourceEnforcementConfig>,
    memory_limited: bool,
) -> Option<MemoryPressureConfig> {
    let enforcement = enforcement?;
    let pressure = enforcement.memory_pressure?;
    (enforcement.memory == MemoryEnforcement::Soft && (memory_limited || pressure.limit_kb > 0))
        .then_some(pressure)
}

/// What the cgroup cannot reclaim and its limit, in KiB, read now.
pub fn read_pressure(cgroup: &LimitedCgroup) -> Option<(u64, u64)> {
    let unreclaimable_kb = cgroup.unreclaimable_kb(crate::capacity::read_cgroup_file)?;
    Some((unreclaimable_kb, cgroup.limit_kb))
}

/// Runs the guard for the worker's life: every `interval_ms`, read the
/// cgroup and, past the watermark, kill one action, then judge again only
/// once that action is no longer running or [`SETTLE_TIMEOUT`] has passed
/// (`settled`).
pub async fn run_guard(
    manager: Arc<RunningActionsManagerImpl>,
    cgroup: LimitedCgroup,
    config: MemoryPressureConfig,
) {
    let interval = Duration::from_millis(config.interval_ms.max(1));
    info!(
        cgroup = %cgroup.dir.display(),
        limit_kb = cgroup.limit_kb,
        watermark_kb = config.watermark_kb,
        interval_ms = interval.as_millis(),
        "Memory pressure guard started"
    );
    // A cgroup the kernel would take whole on OOM has no backstop under this
    // guard: a container made before its kubelet learned to kill one process
    // (singleProcessOOMKill) keeps memory.oom.group until it is recreated.
    match cgroup.oom_group(crate::capacity::read_cgroup_file) {
        Some(true) => warn!(
            cgroup = %cgroup.dir.display(),
            "memory.oom.group is 1: a kernel OOM kill would take every action with the worker; recreate the pod once the kubelet sets singleProcessOOMKill"
        ),
        Some(false) => {}
        None => debug!(cgroup = %cgroup.dir.display(), "memory.oom.group unreadable"),
    }
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The last victim and when it was killed, until it has left.
    let mut settling: Option<(OperationId, Instant)> = None;
    // Said once per episode, not every tick.
    let mut warned_nothing_to_kill = false;
    loop {
        ticks.tick().await;
        let Some((unreclaimable_kb, limit_kb)) = read_pressure(&cgroup) else {
            continue;
        };
        if !over_watermark(unreclaimable_kb, limit_kb, config.watermark_kb) {
            warned_nothing_to_kill = false;
            settling = None;
            continue;
        }
        if let Some((victim, since)) = &settling {
            if !settled(manager.is_running(victim), *since, Instant::now()) {
                continue;
            }
            settling = None;
        }
        let candidates = manager.pressure_candidates();
        let Some(victim) = choose_victim(&candidates) else {
            if !warned_nothing_to_kill {
                warn!(
                    unreclaimable_kb,
                    limit_kb,
                    running = candidates.len(),
                    "Worker memory is past the pressure watermark and no running action may be killed for it"
                );
                warned_nothing_to_kill = true;
            }
            continue;
        };
        warn!(
            operation_id = ?victim.operation_id,
            unreclaimable_kb,
            limit_kb,
            watermark_kb = config.watermark_kb,
            request_kb = victim.request_kb,
            last_memory_kb = victim.last_memory_kb,
            "Worker memory is past the pressure watermark, killing one action"
        );
        if manager.kill_for_pressure(&victim.operation_id) {
            settling = Some((victim.operation_id.clone(), Instant::now()));
        } else {
            debug!(
                operation_id = ?victim.operation_id,
                "Chosen action ended before the pressure kill reached it"
            );
        }
    }
}
