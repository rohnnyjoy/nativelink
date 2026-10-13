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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use mock_instant::thread_local::MockClock;
use nativelink_config::schedulers::{MemoryEscalationSpec, PropertyType, SimpleSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, WorkerLoad, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::{MemoryCharge, Worker};
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::metrics::ActiveCountAttributes;
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, mpsc};
use utils::scheduler_utils::make_base_action_info;

mod utils {
    pub(crate) mod scheduler_utils;
}

const NOW_TIME: u64 = 10000;

fn make_scheduler(spec: &SimpleSpec) -> Arc<SimpleScheduler> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        spec,
        memory_awaited_action_db_factory(
            0,
            &task_change_notify,
            MockInstantWrapped::default,
            ActiveCountAttributes::default(),
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
    );
    scheduler
}

async fn add_worker(
    scheduler: &SimpleScheduler,
    name: &str,
    properties: HashMap<String, PlatformPropertyValue>,
) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    add_worker_guarded(scheduler, name, properties, false).await
}

async fn add_worker_guarded(
    scheduler: &SimpleScheduler,
    name: &str,
    properties: HashMap<String, PlatformPropertyValue>,
    memory_guarded: bool,
) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::channel(64);
    let mut worker = Worker::new(
        WorkerId(name.to_string()),
        PlatformProperties::new(properties),
        tx,
        NOW_TIME,
        /* max_inflight_tasks */ 4,
    );
    worker.memory_charge = if memory_guarded {
        MemoryCharge::Request
    } else {
        MemoryCharge::Limit
    };
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    // The connection result comes first; drop it so only dispatches remain.
    let first = rx.try_recv().expect("worker got no connection result");
    assert!(matches!(
        first.update,
        Some(update_for_worker::Update::ConnectionResult(_))
    ));
    Ok(rx)
}

async fn add_action(
    scheduler: &SimpleScheduler,
    digest_byte: u8,
    platform_properties: HashMap<String, String>,
) -> Result<(), Error> {
    let mut action_info = make_base_action_info(
        UNIX_EPOCH + MockClock::time(),
        DigestInfo::new([digest_byte; 32], 512),
    );
    Arc::make_mut(&mut action_info).platform_properties = platform_properties;
    scheduler
        .add_action(OperationId::default(), action_info)
        .await?;
    tokio::task::yield_now().await;
    Ok(())
}

fn dispatched(rx: &mut mpsc::Receiver<UpdateForWorker>) -> usize {
    let mut count = 0;
    while let Ok(msg) = rx.try_recv() {
        if matches!(msg.update, Some(update_for_worker::Update::StartAction(_))) {
            count += 1;
        }
    }
    count
}

fn memory_worker(kb: u64) -> HashMap<String, PlatformPropertyValue> {
    HashMap::from([("memory_kb".to_string(), PlatformPropertyValue::Minimum(kb))])
}

fn memory_action(kb: u64) -> HashMap<String, String> {
    HashMap::from([("memory_kb".to_string(), kb.to_string())])
}

/// Two workers with the same advertised memory. One reports on its keepalive
/// that it has almost nothing left; with the veto on, an action asking for
/// more than that goes to the other. A worker that never reports is never
/// vetoed, and a keepalive without a load keeps the last report.
#[nativelink_test]
async fn live_memory_veto_skips_a_worker_that_reports_no_room() -> Result<(), Error> {
    let scheduler = make_scheduler(&SimpleSpec {
        supported_platform_properties: Some(HashMap::from([(
            "memory_kb".to_string(),
            PropertyType::Minimum,
        )])),
        live_memory_veto: Some("memory_kb".to_string()),
        ..SimpleSpec::default()
    });
    let mut full = add_worker(&scheduler, "full", memory_worker(100_000)).await?;
    let mut roomy = add_worker(&scheduler, "roomy", memory_worker(100_000)).await?;
    // The ledger sees 100 GB free on both; the workers say otherwise.
    scheduler
        .worker_keep_alive_received(
            &WorkerId("full".to_string()),
            NOW_TIME + 1,
            Some(WorkerLoad {
                free_memory_kb: 1_000,
                owed_kb: 0,
            }),
        )
        .await?;
    scheduler
        .worker_keep_alive_received(
            &WorkerId("roomy".to_string()),
            NOW_TIME + 1,
            Some(WorkerLoad {
                free_memory_kb: 50_000,
                owed_kb: 0,
            }),
        )
        .await?;

    add_action(&scheduler, 1, memory_action(2_000)).await?;
    assert_eq!(
        (dispatched(&mut full), dispatched(&mut roomy)),
        (0, 1),
        "the worker reporting 1 MB free must not get a 2 MB action"
    );

    // An action that fits what `full` reports is fine there; the default
    // strategy sends it to the worker used least recently, which is `full`.
    add_action(&scheduler, 2, memory_action(500)).await?;
    assert_eq!((dispatched(&mut full), dispatched(&mut roomy)), (1, 0));

    // A worker that never reported anything is not vetoed, and a keepalive
    // without a load leaves `full` at its last report.
    let mut silent = add_worker(&scheduler, "silent", memory_worker(100_000)).await?;
    scheduler
        .worker_keep_alive_received(&WorkerId("full".to_string()), NOW_TIME + 2, None)
        .await?;
    add_action(&scheduler, 3, memory_action(2_000)).await?;
    assert_eq!(
        dispatched(&mut full) + dispatched(&mut roomy) + dispatched(&mut silent),
        1
    );
    assert_eq!(
        dispatched(&mut full),
        0,
        "a keepalive without a load keeps the last report"
    );
    Ok(())
}

/// A worker that reported no room and then reports room again has to wake
/// the matcher itself. With the fallback pass off nothing else would, and
/// the vetoed action would wait for an unrelated event.
/// The connection result names the property the veto reads, so the worker
/// declines for load against the same one the scheduler would have vetoed.
#[nativelink_test]
async fn the_connection_result_names_the_veto_property() -> Result<(), Error> {
    let scheduler = make_scheduler(&SimpleSpec {
        supported_platform_properties: Some(HashMap::from([(
            "memory_kb".to_string(),
            PropertyType::Minimum,
        )])),
        live_memory_veto: Some("memory_kb".to_string()),
        ..SimpleSpec::default()
    });
    let (tx, mut rx) = mpsc::channel(64);
    scheduler
        .add_worker(Worker::new(
            WorkerId("told".to_string()),
            PlatformProperties::new(memory_worker(100_000)),
            tx,
            NOW_TIME,
            /* max_inflight_tasks */ 4,
        ))
        .await?;
    let connected = rx.recv().await.unwrap();
    let Some(update_for_worker::Update::ConnectionResult(result)) = connected.update else {
        panic!("expected a ConnectionResult, got {connected:?}");
    };
    assert_eq!(result.memory_property, "memory_kb");
    Ok(())
}

#[nativelink_test]
async fn a_load_report_with_room_again_wakes_the_matcher() -> Result<(), Error> {
    let scheduler = make_scheduler(&SimpleSpec {
        supported_platform_properties: Some(HashMap::from([(
            "memory_kb".to_string(),
            PropertyType::Minimum,
        )])),
        live_memory_veto: Some("memory_kb".to_string()),
        fallback_match_interval_s: 0,
        ..SimpleSpec::default()
    });
    let mut full = add_worker(&scheduler, "full", memory_worker(100_000)).await?;
    scheduler
        .worker_keep_alive_received(
            &WorkerId("full".to_string()),
            NOW_TIME + 1,
            Some(WorkerLoad {
                free_memory_kb: 1_000,
                owed_kb: 0,
            }),
        )
        .await?;
    add_action(&scheduler, 1, memory_action(2_000)).await?;
    assert_eq!(
        dispatched(&mut full),
        0,
        "vetoed while the worker has no room"
    );

    // The resident process exits and the next keepalive says so.
    scheduler
        .worker_keep_alive_received(
            &WorkerId("full".to_string()),
            NOW_TIME + 2,
            Some(WorkerLoad {
                free_memory_kb: 50_000,
                owed_kb: 0,
            }),
        )
        .await?;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        dispatched(&mut full),
        1,
        "the report with room again must wake the matcher on its own"
    );
    Ok(())
}

/// Without `live_memory_veto` the report is recorded and ignored: the
/// ledger alone decides, as before.
#[nativelink_test]
async fn without_the_veto_a_worker_reporting_no_room_still_gets_actions() -> Result<(), Error> {
    let scheduler = make_scheduler(&SimpleSpec {
        supported_platform_properties: Some(HashMap::from([(
            "memory_kb".to_string(),
            PropertyType::Minimum,
        )])),
        ..SimpleSpec::default()
    });
    let mut full = add_worker(&scheduler, "full", memory_worker(100_000)).await?;
    scheduler
        .worker_keep_alive_received(
            &WorkerId("full".to_string()),
            NOW_TIME + 1,
            Some(WorkerLoad {
                free_memory_kb: 1_000,
                owed_kb: 0,
            }),
        )
        .await?;
    add_action(&scheduler, 1, memory_action(2_000)).await?;
    assert_eq!(dispatched(&mut full), 1);
    Ok(())
}

fn veto_spec(margin_kb: u64) -> SimpleSpec {
    SimpleSpec {
        supported_platform_properties: Some(HashMap::from([(
            "memory_kb".to_string(),
            PropertyType::Minimum,
        )])),
        live_memory_veto: Some("memory_kb".to_string()),
        live_memory_margin_kb: margin_kb,
        fallback_match_interval_s: 0,
        ..SimpleSpec::default()
    }
}

async fn report_load(
    scheduler: &SimpleScheduler,
    worker: &str,
    at: u64,
    free_memory_kb: u64,
    owed_kb: u64,
) -> Result<(), Error> {
    scheduler
        .worker_keep_alive_received(
            &WorkerId(worker.to_string()),
            NOW_TIME + at,
            Some(WorkerLoad {
                free_memory_kb,
                owed_kb,
            }),
        )
        .await?;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// What the scheduler sends a worker is debited from its last report until
/// the next: two actions waiting on a report whose free memory fits one get
/// one dispatch from the pass it wakes, not two. The next report starts the
/// debit again from nothing.
#[nativelink_test]
async fn one_report_with_room_for_one_action_places_one() -> Result<(), Error> {
    let scheduler = make_scheduler(&veto_spec(0));
    let mut worker = add_worker(&scheduler, "w", memory_worker(100_000)).await?;
    report_load(&scheduler, "w", 1, 1_000, 0).await?;
    add_action(&scheduler, 1, memory_action(2_000)).await?;
    add_action(&scheduler, 2, memory_action(2_000)).await?;
    assert_eq!(dispatched(&mut worker), 0, "vetoed while it has no room");

    // Room for one: the pass places one and the debit keeps the other out.
    report_load(&scheduler, "w", 2, 3_000, 0).await?;
    assert_eq!(
        dispatched(&mut worker),
        1,
        "3000 KiB free fits one 2000 KiB action, not two"
    );

    // The next report knows of the first action; the debit is spent.
    report_load(&scheduler, "w", 3, 2_500, 0).await?;
    assert_eq!(dispatched(&mut worker), 1, "a new report resets the debit");
    Ok(())
}

/// What the worker says its running actions are still owed counts against
/// what it reports free: an action just started has used little of its
/// request, so free memory alone would admit a burst onto it.
#[nativelink_test]
async fn memory_owed_to_running_actions_blocks_a_burst() -> Result<(), Error> {
    let scheduler = make_scheduler(&veto_spec(0));
    let mut worker = add_worker(&scheduler, "w", memory_worker(100_000)).await?;
    report_load(&scheduler, "w", 1, 10_000, 9_000).await?;
    add_action(&scheduler, 1, memory_action(2_000)).await?;
    assert_eq!(
        dispatched(&mut worker),
        0,
        "10000 KiB free with 9000 owed leaves 1000"
    );

    report_load(&scheduler, "w", 2, 10_000, 0).await?;
    assert_eq!(dispatched(&mut worker), 1, "nothing owed: it fits");
    Ok(())
}

/// `live_memory_margin_kb` is what the worker must have to spare beyond the
/// action.
#[nativelink_test]
async fn the_live_memory_margin_must_be_left_over() -> Result<(), Error> {
    let scheduler = make_scheduler(&veto_spec(1_000));
    let mut worker = add_worker(&scheduler, "w", memory_worker(100_000)).await?;
    report_load(&scheduler, "w", 1, 2_500, 0).await?;
    add_action(&scheduler, 1, memory_action(2_000)).await?;
    assert_eq!(
        dispatched(&mut worker),
        0,
        "2000 KiB plus a 1000 KiB margin is more than 2500 free"
    );
    add_action(&scheduler, 2, memory_action(1_500)).await?;
    assert_eq!(
        dispatched(&mut worker),
        1,
        "1500 KiB plus the margin is exactly 2500 free"
    );
    Ok(())
}

/// With a request and a limit apart, a worker that did not register as
/// guarding its memory is charged the limit: it fits and books one action
/// whose limit is most of its memory where a guarded worker, charged the
/// request, books several. `Worker::charged` is the rule the ledger and
/// the fit both read.
#[nativelink_test]
async fn an_unguarded_worker_is_charged_the_limit() -> Result<(), Error> {
    const GIB: u64 = 1024 * 1024;
    let spec = SimpleSpec {
        supported_platform_properties: Some(HashMap::from([
            ("memory_kb".to_string(), PropertyType::Minimum),
            ("memory_limit_kb".to_string(), PropertyType::Ignore),
        ])),
        memory_escalation: Some(MemoryEscalationSpec {
            property: "memory_kb".to_string(),
            percent: 200,
            max_kb: 0,
            ladder_kb: vec![GIB, 2 * GIB, 4 * GIB],
            cpu_property: "cpu_count".to_string(),
            max_steps: 0,
            limit_property: Some("memory_limit_kb".to_string()),
        }),
        fallback_match_interval_s: 0,
        ..SimpleSpec::default()
    };
    let action = |digest: u8| {
        (
            digest,
            HashMap::from([
                ("memory_kb".to_string(), GIB.to_string()),
                ("memory_limit_kb".to_string(), (3 * GIB).to_string()),
            ]),
        )
    };

    let scheduler = make_scheduler(&spec);
    let mut unguarded =
        add_worker_guarded(&scheduler, "unguarded", memory_worker(4 * GIB), false).await?;
    for (digest, properties) in [action(1), action(2)] {
        add_action(&scheduler, digest, properties).await?;
    }
    assert_eq!(
        dispatched(&mut unguarded),
        1,
        "4 GiB less a 3 GiB limit leaves no room for a second 3 GiB limit"
    );

    let scheduler = make_scheduler(&spec);
    let mut guarded =
        add_worker_guarded(&scheduler, "guarded", memory_worker(4 * GIB), true).await?;
    for (digest, properties) in [action(1), action(2)] {
        add_action(&scheduler, digest, properties).await?;
    }
    assert_eq!(
        dispatched(&mut guarded),
        2,
        "two 1 GiB requests fit in 4 GiB"
    );

    // The rule itself: the request raised to the limit off a guarded worker,
    // untouched on one, and never lowered.
    let (tx, _rx) = mpsc::channel(1);
    let mut worker = Worker::new(
        WorkerId("w".to_string()),
        PlatformProperties::new(memory_worker(4 * GIB)),
        tx,
        NOW_TIME,
        4,
    );
    worker.limit_charge = Some(("memory_kb".to_string(), "memory_limit_kb".to_string()));
    let props = PlatformProperties::new(HashMap::from([
        ("memory_kb".to_string(), PlatformPropertyValue::Minimum(GIB)),
        (
            "memory_limit_kb".to_string(),
            PlatformPropertyValue::Ignore((3 * GIB).to_string()),
        ),
    ]));
    assert_eq!(
        worker.charged(&props).properties["memory_kb"],
        PlatformPropertyValue::Minimum(3 * GIB)
    );
    worker.memory_charge = MemoryCharge::Request;
    assert_eq!(
        worker.charged(&props).properties["memory_kb"],
        PlatformPropertyValue::Minimum(GIB)
    );
    worker.memory_charge = MemoryCharge::Limit;
    let below = PlatformProperties::new(HashMap::from([
        (
            "memory_kb".to_string(),
            PlatformPropertyValue::Minimum(3 * GIB),
        ),
        (
            "memory_limit_kb".to_string(),
            PlatformPropertyValue::Ignore(GIB.to_string()),
        ),
    ]));
    assert_eq!(
        worker.charged(&below).properties["memory_kb"],
        PlatformPropertyValue::Minimum(3 * GIB),
        "a limit below the request never lowers it"
    );
    Ok(())
}
