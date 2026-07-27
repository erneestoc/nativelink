// Copyright 2024 The NativeLink Authors. All rights reserved.
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
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_util::action_messages::{
    ActionResult, ActionStage, ExecutionMetadata, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, UpdateOperationType,
};
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

mod utils {
    pub(crate) mod scheduler_utils;
}
use utils::scheduler_utils::make_base_action_info;

const NOW_TIME: u64 = 10000;

fn make_action_result(worker_id: &WorkerId) -> ActionResult {
    let t = UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME))
        .unwrap();
    ActionResult {
        output_files: vec![],
        output_folders: vec![],
        output_file_symlinks: vec![],
        output_directory_symlinks: vec![],
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: t,
            worker_start_timestamp: t,
            worker_completed_timestamp: t,
            input_fetch_start_timestamp: t,
            input_fetch_completed_timestamp: t,
            execution_start_timestamp: t,
            execution_completed_timestamp: t,
            output_upload_start_timestamp: t,
            output_upload_completed_timestamp: t,
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    }
}

struct Fixture {
    scheduler: Arc<SimpleScheduler>,
    worker_scheduler: Arc<dyn nativelink_scheduler::worker_scheduler::WorkerScheduler>,
    worker_id: WorkerId,
    rx: mpsc::UnboundedReceiver<UpdateForWorker>,
    listeners: Vec<Box<dyn ActionStateResult>>,
}

async fn setup(allowance: u64, num_actions: usize) -> Result<Fixture, Error> {
    let worker_id = WorkerId("worker1".to_string());
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            experimental_max_overlapping_uploads_per_worker: allowance,
            ..Default::default()
        },
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(
        worker_id.clone(),
        PlatformProperties::default(),
        tx,
        NOW_TIME,
        1, // One execution slot.
    );
    worker_scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "add_worker")?;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    // Consume the connection message.
    drop(rx.recv().await);

    let mut listeners: Vec<Box<dyn ActionStateResult>> = Vec::new();
    for i in 0..num_actions {
        let mut digest_bytes = [42u8; 32];
        digest_bytes[0] = u8::try_from(i).unwrap();
        let action_info = make_base_action_info(
            SystemTime::now(),
            DigestInfo::new(digest_bytes, 512 + i as u64),
        );
        listeners.push(
            scheduler
                .add_action(OperationId::default(), action_info)
                .await?,
        );
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
    Ok(Fixture {
        scheduler,
        worker_scheduler,
        worker_id,
        rx,
        listeners,
    })
}

async fn try_recv_start_action(
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) -> Option<OperationId> {
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::task::yield_now().await;
    }
    match rx.try_recv() {
        Ok(UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(start_execute)),
        }) => Some(OperationId::from(start_execute.operation_id.as_str())),
        _ => None,
    }
}

// With the overlap allowance, the second action must be dispatched as soon
// as the first reports ExecutionComplete (its result still uploading), and
// a third must NOT be dispatched (executing=1, total=2 caps the worker).
#[nativelink_test]
async fn overlap_allowance_dispatches_next_action_during_upload() -> Result<(), Error> {
    let mut fixture = setup(1, 3).await?;
    let first_op = try_recv_start_action(&mut fixture.rx)
        .await
        .expect("first action should dispatch");
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_none(),
        "second action must wait while first executes"
    );

    fixture
        .worker_scheduler
        .update_action(
            &fixture.worker_id,
            &first_op,
            UpdateOperationType::ExecutionComplete,
        )
        .await?;
    let second_op = try_recv_start_action(&mut fixture.rx)
        .await
        .expect("second action must dispatch while first uploads");
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_none(),
        "third action must wait: one executing plus one uploading fills the allowance"
    );

    // Completing the first (upload done) frees an overlap slot; the second
    // is still executing so the third must still wait.
    fixture
        .worker_scheduler
        .update_action(
            &fixture.worker_id,
            &first_op,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(make_action_result(
                &fixture.worker_id,
            ))),
        )
        .await?;
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_none(),
        "third action must wait for the second's execution slot"
    );

    fixture
        .worker_scheduler
        .update_action(
            &fixture.worker_id,
            &second_op,
            UpdateOperationType::ExecutionComplete,
        )
        .await?;
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_some(),
        "third action must dispatch once the second starts uploading"
    );
    drop(fixture.listeners);
    Ok(())
}

// Without the allowance (default 0), behavior is unchanged: the second
// action is not dispatched until the first fully completes.
#[nativelink_test]
async fn no_allowance_holds_slot_through_upload() -> Result<(), Error> {
    let mut fixture = setup(0, 2).await?;
    let first_op = try_recv_start_action(&mut fixture.rx)
        .await
        .expect("first action should dispatch");

    fixture
        .worker_scheduler
        .update_action(
            &fixture.worker_id,
            &first_op,
            UpdateOperationType::ExecutionComplete,
        )
        .await?;
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_none(),
        "second action must NOT dispatch during upload when allowance is 0"
    );

    fixture
        .worker_scheduler
        .update_action(
            &fixture.worker_id,
            &first_op,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(make_action_result(
                &fixture.worker_id,
            ))),
        )
        .await?;
    assert!(
        try_recv_start_action(&mut fixture.rx).await.is_some(),
        "second action dispatches after full completion"
    );
    drop(fixture.listeners);
    Ok(())
}
