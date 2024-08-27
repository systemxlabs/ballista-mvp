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

use async_trait::async_trait;
use log::{debug, error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};

use crate::config::SchedulerConfig;
use crate::scheduler_server::timestamp_millis;
use tokio::sync::mpsc;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler {
    state: Arc<SchedulerState>,
    #[allow(dead_code)]
    config: Arc<SchedulerConfig>,
}

impl QueryStageScheduler {
    pub(crate) fn new(state: Arc<SchedulerState>, config: Arc<SchedulerConfig>) -> Self {
        Self { state, config }
    }
}

#[async_trait]
impl EventAction<QueryStageSchedulerEvent> for QueryStageScheduler {
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: &mpsc::Sender<QueryStageSchedulerEvent>,
        _rx_event: &mpsc::Receiver<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let event_sender = EventSender::new(tx_event.clone());
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                session_ctx,
                plan,
                queued_at,
            } => {
                info!("Job {} queued", job_id);

                if let Err(e) = self.state.task_manager.queue_job(&job_id, queued_at) {
                    error!("Fail to queue job {} due to {:?}", job_id, e);
                    return Ok(());
                }

                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(&job_id, session_ctx, &plan, queued_at)
                        .await
                    {
                        let fail_message = format!("Error planning job {job_id}: {e:?}");
                        error!("{}", &fail_message);
                        QueryStageSchedulerEvent::JobPlanningFailed {
                            job_id,
                            fail_message,
                            queued_at,
                            failed_at: timestamp_millis(),
                        }
                    } else {
                        QueryStageSchedulerEvent::JobSubmitted {
                            job_id,
                            queued_at,
                            submitted_at: timestamp_millis(),
                        }
                    };
                    if let Err(e) = event_sender.post_event(event).await {
                        error!("Fail to send event due to {}", e);
                    }
                });
            }
            QueryStageSchedulerEvent::JobSubmitted { job_id, .. } => {
                info!("Job {} submitted", job_id);

                event_sender
                    .post_event(QueryStageSchedulerEvent::ReviveOffers)
                    .await?;
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                fail_message,
                ..
            } => {
                error!("Job {} failed: {}", job_id, fail_message);
                if let Err(e) = self
                    .state
                    .task_manager
                    .fail_unscheduled_job(&job_id, fail_message)
                    .await
                {
                    error!(
                        "Fail to invoke fail_unscheduled_job for job {} due to {:?}",
                        job_id, e
                    );
                }
            }
            QueryStageSchedulerEvent::JobFinished { job_id, .. } => {
                info!("Job {} success", job_id);
                if let Err(e) = self.state.task_manager.succeed_job(&job_id).await {
                    error!(
                        "Fail to invoke succeed_job for job {} due to {:?}",
                        job_id, e
                    );
                }
                self.state.clean_up_successful_job(job_id);
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                ..
            } => {
                error!("Job {} running failed", job_id);
                match self
                    .state
                    .task_manager
                    .abort_job(&job_id, fail_message)
                    .await
                {
                    Ok((running_tasks, _pending_tasks)) => {
                        if !running_tasks.is_empty() {
                            event_sender
                                .post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks))
                                .await?;
                        }
                    }
                    Err(e) => {
                        error!("Fail to invoke abort_job for job {} due to {:?}", job_id, e);
                    }
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!("Job {} Updated", job_id);
                if let Err(e) = self.state.task_manager.update_job(&job_id).await {
                    error!(
                        "Fail to invoke update_job for job {} due to {:?}",
                        job_id, e
                    );
                }
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                info!("Job {} Cancelled", job_id);
                match self.state.task_manager.cancel_job(&job_id).await {
                    Ok((running_tasks, _pending_tasks)) => {
                        event_sender
                            .post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks))
                            .await?;
                    }
                    Err(e) => {
                        error!(
                            "Fail to invoke cancel_job for job {} due to {:?}",
                            job_id, e
                        );
                    }
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                debug!(
                    "processing task status updates from {executor_id}: {:?}",
                    tasks_status
                );

                let num_status = tasks_status.len();
                self.state
                    .executor_manager
                    .unbind_tasks(vec![(executor_id.clone(), num_status as u32)])
                    .await?;
                match self
                    .state
                    .update_task_statuses(&executor_id, tasks_status)
                    .await
                {
                    Ok(stage_events) => {
                        event_sender
                            .post_event(QueryStageSchedulerEvent::ReviveOffers)
                            .await?;

                        for stage_event in stage_events {
                            event_sender.post_event(stage_event).await?;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to update {} task statuses for Executor {}: {:?}",
                            num_status, executor_id, e
                        );
                        // TODO error handling
                    }
                }
            }
            QueryStageSchedulerEvent::ReviveOffers => {
                self.state.revive_offers(event_sender).await?;
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                if let Err(e) = self
                    .state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await
                {
                    warn!("Fail to cancel running tasks due to {:?}", e);
                }
            }
        }
        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}

#[cfg(test)]
mod tests {
    use crate::config::SchedulerConfig;
    use crate::test_utils::{await_condition, SchedulerTest};
    use ballista_core::error::Result;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, LogicalPlan};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::time::Duration;
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn test_pending_job_metric() -> Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        let plan = test_plan(10);

        let mut test = SchedulerTest::new(SchedulerConfig::default(), 1, 1, None).await?;

        let job_id = "job-1";

        test.submit(job_id, &plan).await?;

        test.tick().await?;

        let running_jobs = test.running_job_number();
        let expected = 1usize;
        assert_eq!(
            expected, running_jobs,
            "Expected {} running jobs but found {}",
            expected, running_jobs
        );

        test.cancel(job_id).await?;

        let expected = 0usize;
        let success = await_condition(Duration::from_millis(10), 20, || {
            let running_jobs = test.running_job_number();

            futures::future::ready(Ok(running_jobs == expected))
        })
        .await
        .unwrap();
        assert!(
            success,
            "Expected {} running jobs but found {}",
            expected,
            test.running_job_number()
        );

        Ok(())
    }

    fn test_plan(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), partitions)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }
}
