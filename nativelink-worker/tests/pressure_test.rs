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

use core::time::Duration;
use std::time::Instant;

use nativelink_config::cas_server::{
    DiskEnforcement, MemoryEnforcement, MemoryPressureConfig, ResourceEnforcementConfig,
};
use nativelink_worker::pressure::{
    PressureCandidate, SETTLE_TIMEOUT, choose_victim, configured, over_watermark, owed_kb, settled,
};
use pretty_assertions::assert_eq;

const GIB: u64 = 1024 * 1024;

fn candidate(
    name: &str,
    started: Instant,
    request_kb: u64,
    limit_kb: u64,
    last_memory_kb: u64,
) -> PressureCandidate {
    PressureCandidate {
        operation_id: name.into(),
        started,
        request_kb,
        limit_kb,
        last_memory_kb,
    }
}

fn victim(candidates: &[PressureCandidate]) -> Option<String> {
    choose_victim(candidates).map(|c| c.operation_id.to_string())
}

/// Of the actions over their request, the one furthest over goes, in KiB
/// and not in proportion: freeing the most is the point.
#[test]
fn furthest_over_its_request_is_chosen_first() {
    let t0 = Instant::now();
    let candidates = [
        // 1 GiB over a 1 GiB request: double, but only 1 GiB.
        candidate("small", t0, GIB, 4 * GIB, 2 * GIB),
        // 2 GiB over a 4 GiB request.
        candidate(
            "large",
            t0 + Duration::from_secs(1),
            4 * GIB,
            8 * GIB,
            6 * GIB,
        ),
        // Under its request, and the youngest.
        candidate("young", t0 + Duration::from_secs(5), 2 * GIB, 4 * GIB, GIB),
    ];
    assert_eq!(victim(&candidates).as_deref(), Some("large"));
}

/// With none over its request, the youngest goes: it has the least work
/// to lose.
#[test]
fn youngest_is_chosen_when_none_is_over() {
    let t0 = Instant::now();
    let candidates = [
        candidate("old", t0, 4 * GIB, 8 * GIB, 4 * GIB),
        candidate("young", t0 + Duration::from_secs(9), GIB, 2 * GIB, 0),
        candidate("middle", t0 + Duration::from_secs(3), 2 * GIB, 4 * GIB, GIB),
    ];
    assert_eq!(victim(&candidates).as_deref(), Some("young"));
}

/// An action whose request is at or above its limit was booked at its
/// kill line, so it is never chosen, however far over its request or
/// however young.
#[test]
fn protected_actions_are_never_chosen() {
    let t0 = Instant::now();
    let candidates = [
        // At its request, which is its limit: booked at its kill line.
        candidate("escalated", t0, 8 * GIB, 8 * GIB, 8 * GIB),
        // The youngest, but request > limit.
        candidate("requeued", t0 + Duration::from_secs(9), 4 * GIB, 2 * GIB, 0),
        candidate("plain", t0 + Duration::from_secs(1), GIB, 2 * GIB, GIB),
    ];
    assert!(candidates[0].is_protected());
    assert!(candidates[1].is_protected());
    assert!(!candidates[2].is_protected());
    assert_eq!(victim(&candidates).as_deref(), Some("plain"));
    assert_eq!(victim(&candidates[..2]), None);
}

/// An action without a limit property is not protected: there is no kill
/// line it was booked at.
#[test]
fn no_limit_is_not_protected() {
    let t0 = Instant::now();
    let candidates = [candidate("bare", t0, 0, 0, GIB)];
    assert!(!candidates[0].is_protected());
    assert_eq!(victim(&candidates).as_deref(), Some("bare"));
}

#[test]
fn nothing_running_chooses_nothing() {
    assert_eq!(victim(&[]), None);
}

#[test]
fn watermark_is_the_limit_less_the_margin() {
    assert!(!over_watermark(16 * GIB, 18 * GIB, GIB));
    assert!(over_watermark(17 * GIB, 18 * GIB, GIB));
    assert!(over_watermark(18 * GIB, 18 * GIB, GIB));
    // Admission's margin is twice the guard's.
    assert!(over_watermark(16 * GIB, 18 * GIB, 2 * GIB));
    // A watermark past the limit is always over, never an underflow.
    assert!(over_watermark(0, GIB, 2 * GIB));
}

/// Owed is what each action has yet to take of its request: an action
/// just started owes all of it, one past its request owes nothing.
#[test]
fn owed_is_request_less_use_where_the_request_is_larger() {
    assert_eq!(owed_kb([]), 0);
    assert_eq!(owed_kb([(4 * GIB, 0)]), 4 * GIB);
    assert_eq!(
        owed_kb([(4 * GIB, GIB), (2 * GIB, 3 * GIB), (GIB, GIB)]),
        3 * GIB
    );
    // Nothing sampled and nothing requested owes nothing.
    assert_eq!(owed_kb([(0, 0), (0, GIB)]), 0);
}

fn enforcement(
    memory: MemoryEnforcement,
    memory_pressure: Option<MemoryPressureConfig>,
) -> ResourceEnforcementConfig {
    ResourceEnforcementConfig {
        memory,
        disk: DiskEnforcement::None,
        disk_property_name: "disk_kb".to_string(),
        memory_property_name: "memory_limit_kb".to_string(),
        memory_request_property_name: "memory_kb".to_string(),
        memory_headroom_percent: 20,
        disk_headroom_percent: 20,
        memory_pressure,
    }
}

/// The guard runs only when configured, with soft memory enforcement, on a
/// worker whose own cgroup is limited.
#[test]
fn guard_runs_only_configured_soft_and_limited() {
    let pressure = MemoryPressureConfig::default();
    assert_eq!(pressure.watermark_kb, 1_048_576);
    assert_eq!(pressure.interval_ms, 100);
    let soft = enforcement(MemoryEnforcement::Soft, Some(pressure));
    assert_eq!(configured(Some(&soft), true), Some(pressure));
    assert_eq!(configured(Some(&soft), false), None);
    // A cgroup without a limit is guarded against the configured one.
    let stated = MemoryPressureConfig {
        limit_kb: 8_388_608,
        ..pressure
    };
    assert_eq!(
        configured(
            Some(&enforcement(MemoryEnforcement::Soft, Some(stated))),
            false
        ),
        Some(stated)
    );
    assert_eq!(
        configured(
            Some(&enforcement(MemoryEnforcement::None, Some(pressure))),
            true
        ),
        None
    );
    assert_eq!(
        configured(Some(&enforcement(MemoryEnforcement::Soft, None)), true),
        None
    );
    assert_eq!(configured(None, true), None);
}

/// Unset fields take their defaults, and an unknown one is refused.
#[test]
fn pressure_config_defaults_and_refuses_unknown_fields() {
    let parsed: MemoryPressureConfig = serde_json5::from_str("{}").expect("empty block parses");
    assert_eq!(parsed, MemoryPressureConfig::default());
    let parsed: MemoryPressureConfig =
        serde_json5::from_str("{ watermark_kb: 2097152 }").expect("one field parses");
    assert_eq!(parsed.watermark_kb, 2_097_152);
    assert_eq!(parsed.interval_ms, 100);
    assert_eq!(parsed.limit_kb, 0);
    let parsed: MemoryPressureConfig =
        serde_json5::from_str(r#"{ limit_kb: "62914560" }"#).expect("the limit parses as a string");
    assert_eq!(parsed.limit_kb, 62_914_560);
    // Strings are taken too, so a deployment can fill them from the
    // environment (`"${FL_MEMORY_PRESSURE_WATERMARK_KB}"`).
    let parsed: MemoryPressureConfig =
        serde_json5::from_str(r#"{ watermark_kb: "524288", interval_ms: "50" }"#)
            .expect("numeric strings parse");
    assert_eq!(parsed.watermark_kb, 524_288);
    assert_eq!(parsed.interval_ms, 50);
    assert!(serde_json5::from_str::<MemoryPressureConfig>("{ watermark: 1 }").is_err());
}

#[test]
fn the_guard_settles_once_the_victim_is_gone_or_the_timeout_passes() {
    let since = Instant::now();
    assert!(!settled(true, since, since + Duration::from_secs(1)));
    assert!(settled(true, since, since + SETTLE_TIMEOUT));
    assert!(settled(false, since, since + Duration::from_millis(1)));
    assert!(settled(false, since, since));
}

/// An action booked at its kill line is spared only while it uses no more
/// than its request; past it, it is the one furthest over and is chosen.
#[test]
fn a_protected_action_past_its_request_competes() {
    let now = Instant::now();
    let candidates = [
        candidate("booked-at-line-over", now, 2 * GIB, 2 * GIB, 2 * GIB + 512),
        candidate("within-request", now, GIB, 4 * GIB, GIB - 1),
    ];
    assert!(!candidates[0].is_protected());
    assert_eq!(victim(&candidates).as_deref(), Some("booked-at-line-over"));
}
