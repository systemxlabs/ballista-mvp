// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ballista_core::error::Result;
use ballista_core::event_loop::EventLoop;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::serde::BallistaCodec;

use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use log::{error, warn};

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::query_stage_scheduler::QueryStageScheduler;

use crate::state::executor_manager::ExecutorManager;

use crate::state::task_manager::TaskLauncher;
use crate::state::SchedulerState;

pub mod event;
mod grpc;
pub(crate) mod query_stage_scheduler;

pub(crate) type SessionBuilder = fn(SessionConfig) -> SessionState;

#[derive(Clone)]
pub struct SchedulerServer {
    pub scheduler_name: String,
    pub start_time: u128,
    pub state: Arc<SchedulerState>,
    pub(crate) query_stage_event_loop: EventLoop<QueryStageSchedulerEvent>,
    #[allow(dead_code)]
    config: Arc<SchedulerConfig>,
}

impl SchedulerServer {
    pub fn new(
        scheduler_name: String,
        cluster: BallistaCluster,
        codec: BallistaCodec,
        config: Arc<SchedulerConfig>,
        task_launcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(
            cluster,
            codec,
            scheduler_name.clone(),
            config.clone(),
            task_launcher,
        ));
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), config.clone()));
        let query_stage_event_loop = EventLoop::new(
            "query_stage".to_owned(),
            config.event_loop_buffer_size as usize,
            query_stage_scheduler.clone(),
        );

        Self {
            scheduler_name,
            start_time: timestamp_millis() as u128,
            state,
            query_stage_event_loop,
            config,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.state.init().await?;
        self.query_stage_event_loop.start()?;
        self.expire_dead_executors()?;

        Ok(())
    }

    pub fn running_job_number(&self) -> usize {
        self.state.task_manager.running_job_number()
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::JobQueued {
                job_id: job_id.to_owned(),
                session_ctx: ctx,
                plan: Box::new(plan.clone()),
                queued_at: timestamp_millis(),
            })
            .await
    }

    /// It just send task status update event to the channel,
    /// and will not guarantee the event processing completed after return
    pub(crate) async fn update_task_status(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<()> {
        // We might receive buggy task updates from dead executors.
        if self.state.executor_manager.is_dead_executor(executor_id) {
            let error_msg = format!(
                "Receive buggy tasks status from dead Executor {executor_id}, task status update ignored."
            );
            warn!("{}", error_msg);
            return Ok(());
        }
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::TaskUpdating(
                executor_id.to_owned(),
                tasks_status,
            ))
            .await
    }

    pub(crate) async fn revive_offers(&self) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::ReviveOffers)
            .await
    }

    /// Spawn an async task which periodically check the active executors' status and
    /// expire the dead executors
    fn expire_dead_executors(&self) -> Result<()> {
        let state = self.state.clone();
        tokio::task::spawn(async move {
            loop {
                let expired_executors = state.executor_manager.get_expired_executors();
                for expired in expired_executors {
                    let executor_id = expired.executor_id.clone();

                    let terminating = matches!(
                        expired
                            .status
                            .as_ref()
                            .and_then(|status| status.status.as_ref()),
                        Some(
                            ballista_core::serde::protobuf::executor_status::Status::Terminating(_)
                        )
                    );

                    let stop_reason = if terminating {
                        format!(
                            "TERMINATING executor {executor_id} heartbeat timed out after {}s",
                            state.config.executor_termination_grace_period,
                        )
                    } else {
                        format!(
                            "ACTIVE executor {executor_id} heartbeat timed out after {}s",
                            state.config.executor_timeout_seconds,
                        )
                    };

                    warn!("{stop_reason}");

                    // If executor is expired, remove it immediately
                    Self::remove_executor(
                        state.executor_manager.clone(),
                        &executor_id,
                        Some(stop_reason.clone()),
                        0,
                    );
                }
                tokio::time::sleep(Duration::from_secs(
                    state.config.expire_dead_executor_interval_seconds,
                ))
                .await;
            }
        });
        Ok(())
    }

    pub(crate) fn remove_executor(
        executor_manager: ExecutorManager,
        executor_id: &str,
        reason: Option<String>,
        wait_secs: u64,
    ) {
        let executor_id = executor_id.to_owned();
        tokio::spawn(async move {
            // Wait for `wait_secs` before removing executor
            tokio::time::sleep(Duration::from_secs(wait_secs)).await;

            // Update the executor manager immediately here
            if let Err(e) = executor_manager
                .remove_executor(&executor_id, reason.clone())
                .await
            {
                error!("error removing executor {executor_id}: {e:?}");
            }
        });
    }

    async fn do_register_executor(&self, metadata: ExecutorMetadata) -> Result<()> {
        let executor_data = ExecutorData {
            executor_id: metadata.id.clone(),
            total_task_slots: metadata.specification.task_slots,
            available_task_slots: metadata.specification.task_slots,
        };

        // Save the executor to state
        self.state
            .executor_manager
            .register_executor(metadata, executor_data)
            .await?;

        self.revive_offers().await?;

        Ok(())
    }
}

pub fn timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

pub fn timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, LogicalPlan};

    use datafusion::test_util::scan_empty;

    use ballista_core::error::Result;

    use crate::config::SchedulerConfig;

    use ballista_core::serde::protobuf::{
        job_status, task_status, FailedTask, JobStatus, MultiTaskDefinition, SuccessfulJob, TaskId,
        TaskStatus,
    };

    use crate::scheduler_server::timestamp_millis;

    use crate::test_utils::{ExplodingTableProvider, SchedulerTest, TaskRunnerFn};

    #[tokio::test]
    async fn test_push_scheduling() -> Result<()> {
        let plan = test_plan();

        let mut test = SchedulerTest::new(SchedulerConfig::default(), 4, 1, None).await?;

        let status = test.run("job", &plan).await.expect("running plan");

        match status.status {
            Some(job_status::Status::Successful(SuccessfulJob {
                partition_location, ..
            })) => {
                assert_eq!(partition_location.len(), 4);
            }
            other => {
                panic!("Expected success status but found {:?}", other);
            }
        }

        Ok(())
    }

    // Simulate a task failure and ensure the job status is updated correctly
    #[tokio::test]
    async fn test_job_failure() -> Result<()> {
        let plan = test_plan();

        let runner = Arc::new(TaskRunnerFn::new(
            |_executor_id: String, task: MultiTaskDefinition| {
                let mut statuses = vec![];

                for TaskId {
                    task_id,
                    partition_id,
                    ..
                } in task.task_ids
                {
                    let timestamp = timestamp_millis();
                    statuses.push(TaskStatus {
                        task_id,
                        job_id: task.job_id.clone(),
                        stage_id: task.stage_id,
                        partition_id,
                        launch_time: timestamp,
                        start_exec_time: timestamp,
                        end_exec_time: timestamp,
                        metrics: vec![],
                        status: Some(task_status::Status::Failed(FailedTask {
                            error: "ERROR".to_string(),
                        })),
                    });
                }

                statuses
            },
        ));

        let mut test = SchedulerTest::new(SchedulerConfig::default(), 4, 1, Some(runner)).await?;

        let status = test.run("job", &plan).await.expect("running plan");

        assert!(
            matches!(
                status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_)),
                    ..
                }
            ),
            "{}",
            "Expected job status to be failed but it was {status:?}"
        );

        Ok(())
    }

    // If the physical planning fails, the job should be marked as failed.
    // Here we simulate a planning failure using ExplodingTableProvider to test this.
    #[tokio::test]
    async fn test_planning_failure() -> Result<()> {
        let mut test = SchedulerTest::new(SchedulerConfig::default(), 4, 1, None).await?;

        let ctx = test.ctx().await?;

        ctx.register_table("explode", Arc::new(ExplodingTableProvider))?;

        let plan = ctx
            .sql("SELECT * FROM explode")
            .await?
            .into_optimized_plan()?;

        // This should fail when we try and create the physical plan
        let status = test.run("job", &plan).await?;

        assert!(
            matches!(
                status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_)),
                    ..
                }
            ),
            "{}",
            "Expected job status to be failed but it was {status:?}"
        );

        Ok(())
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }
}
