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

use ballista_core::config::{BallistaConfig, BALLISTA_JOB_NAME};
use ballista_core::serde::protobuf::execute_query_params::{OptionalSessionId, Query};
use std::collections::HashMap;

use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpc;
use ballista_core::serde::protobuf::{
    execute_query_failure_result, execute_query_result, CancelJobParams, CancelJobResult,
    CleanJobDataParams, CleanJobDataResult, CreateSessionParams, CreateSessionResult,
    ExecuteQueryFailureResult, ExecuteQueryParams, ExecuteQueryResult, ExecuteQuerySuccessResult,
    ExecutorHeartbeat, GetJobStatusParams, GetJobStatusResult, HeartBeatParams, HeartBeatResult,
    RegisterExecutorParams, RegisterExecutorResult, RemoveSessionParams, RemoveSessionResult,
    UpdateSessionParams, UpdateSessionResult, UpdateTaskStatusParams, UpdateTaskStatusResult,
};
use ballista_core::serde::scheduler::ExecutorMetadata;

use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{debug, error, info, trace, warn};

use std::ops::Deref;

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::scheduler_server::SchedulerServer;

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerGrpc
    for SchedulerServer<T, U>
{
    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> Result<Response<RegisterExecutorResult>, Status> {
        let remote_addr = request.remote_addr();
        if let RegisterExecutorParams {
            metadata: Some(metadata),
        } = request.into_inner()
        {
            info!("Received register executor request for {:?}", metadata);
            let metadata = ExecutorMetadata {
                id: metadata.id,
                host: remote_addr.map_or("localhost".to_string(), |addr| addr.ip().to_string()),
                port: metadata.port as u16,
                grpc_port: metadata.grpc_port as u16,
                specification: metadata.specification.unwrap().into(),
            };

            self.do_register_executor(metadata).await.map_err(|e| {
                let msg = format!("Fail to do executor registration due to: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;

            Ok(Response::new(RegisterExecutorResult { success: true }))
        } else {
            warn!("Received invalid register executor request");
            Err(Status::invalid_argument("Missing metadata in request"))
        }
    }

    async fn heart_beat_from_executor(
        &self,
        request: Request<HeartBeatParams>,
    ) -> Result<Response<HeartBeatResult>, Status> {
        let remote_addr = request.remote_addr();
        let HeartBeatParams {
            executor_id,
            metrics,
            status,
            metadata,
        } = request.into_inner();
        debug!("Received heart beat request for {:?}", executor_id);

        // If not registered, do registration first before saving heart beat
        if let Err(e) = self
            .state
            .executor_manager
            .get_executor_metadata(&executor_id)
            .await
        {
            warn!("Fail to get executor metadata: {}", e);
            if let Some(metadata) = metadata {
                let metadata = ExecutorMetadata {
                    id: metadata.id,
                    host: remote_addr.map_or("localhost".to_string(), |addr| addr.ip().to_string()),
                    port: metadata.port as u16,
                    grpc_port: metadata.grpc_port as u16,
                    specification: metadata.specification.unwrap().into(),
                };

                self.do_register_executor(metadata).await.map_err(|e| {
                    let msg = format!("Fail to do executor registration due to: {e}");
                    error!("{}", msg);
                    Status::internal(msg)
                })?;
            } else {
                return Err(Status::invalid_argument(format!(
                    "The registration spec for executor {executor_id} is not included"
                )));
            }
        }

        let executor_heartbeat = ExecutorHeartbeat {
            executor_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            metrics,
            status,
        };

        self.state
            .executor_manager
            .save_executor_heartbeat(executor_heartbeat)
            .await
            .map_err(|e| {
                let msg = format!("Could not save executor heartbeat: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(HeartBeatResult {}))
    }

    async fn update_task_status(
        &self,
        request: Request<UpdateTaskStatusParams>,
    ) -> Result<Response<UpdateTaskStatusResult>, Status> {
        let UpdateTaskStatusParams {
            executor_id,
            task_status,
        } = request.into_inner();

        debug!(
            "Received task status update request for executor {:?}",
            executor_id
        );

        self.update_task_status(&executor_id, task_status)
            .await
            .map_err(|e| {
                let msg = format!(
                    "Fail to update tasks status from executor {:?} due to {:?}",
                    &executor_id, e
                );
                error!("{}", msg);
                Status::internal(msg)
            })?;

        Ok(Response::new(UpdateTaskStatusResult { success: true }))
    }

    async fn create_session(
        &self,
        request: Request<CreateSessionParams>,
    ) -> Result<Response<CreateSessionResult>, Status> {
        let session_params = request.into_inner();
        // parse config
        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &session_params.settings {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build().map_err(|e| {
            let msg = format!("Could not parse configs: {e}");
            error!("{}", msg);
            Status::internal(msg)
        })?;

        let ctx = self
            .state
            .session_manager
            .create_session(&config)
            .await
            .map_err(|e| Status::internal(format!("Failed to create SessionContext: {e:?}")))?;

        Ok(Response::new(CreateSessionResult {
            session_id: ctx.session_id(),
        }))
    }

    async fn update_session(
        &self,
        request: Request<UpdateSessionParams>,
    ) -> Result<Response<UpdateSessionResult>, Status> {
        let session_params = request.into_inner();
        // parse config
        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &session_params.settings {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build().map_err(|e| {
            let msg = format!("Could not parse configs: {e}");
            error!("{}", msg);
            Status::internal(msg)
        })?;

        self.state
            .session_manager
            .update_session(&session_params.session_id, &config)
            .await
            .map_err(|e| Status::internal(format!("Failed to create SessionContext: {e:?}")))?;

        Ok(Response::new(UpdateSessionResult { success: true }))
    }

    async fn remove_session(
        &self,
        request: Request<RemoveSessionParams>,
    ) -> Result<Response<RemoveSessionResult>, Status> {
        let session_params = request.into_inner();
        self.state
            .session_manager
            .remove_session(&session_params.session_id)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to remove SessionContext: {e:?} for session {}",
                    session_params.session_id
                ))
            })?;

        Ok(Response::new(RemoveSessionResult { success: true }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> Result<Response<ExecuteQueryResult>, Status> {
        let query_params = request.into_inner();
        if let ExecuteQueryParams {
            query: Some(query),
            optional_session_id,
            settings,
        } = query_params
        {
            let mut query_settings = HashMap::new();
            for kv_pair in settings {
                query_settings.insert(kv_pair.key, kv_pair.value);
            }

            let (session_id, session_ctx) = match optional_session_id {
                Some(OptionalSessionId::SessionId(session_id)) => {
                    match self.state.session_manager.get_session(&session_id).await {
                        Ok(ctx) => (session_id, ctx),
                        Err(e) => {
                            let msg = format!(
                                "Failed to load SessionContext for session ID {session_id}: {e}"
                            );
                            error!("{}", msg);
                            return Ok(Response::new(ExecuteQueryResult {
                                result: Some(execute_query_result::Result::Failure(
                                    ExecuteQueryFailureResult {
                                        failure: Some(
                                            execute_query_failure_result::Failure::SessionNotFound(
                                                msg,
                                            ),
                                        ),
                                    },
                                )),
                            }));
                        }
                    }
                }
                _ => {
                    // Create default config
                    let config = BallistaConfig::builder().build().map_err(|e| {
                        let msg = format!("Could not parse configs: {e}");
                        error!("{}", msg);
                        Status::internal(msg)
                    })?;
                    let ctx = self
                        .state
                        .session_manager
                        .create_session(&config)
                        .await
                        .map_err(|e| {
                            Status::internal(format!("Failed to create SessionContext: {e:?}"))
                        })?;

                    (ctx.session_id(), ctx)
                }
            };

            let plan = match query {
                Query::LogicalPlan(message) => {
                    match T::try_decode(message.as_slice()).and_then(|m| {
                        m.try_into_logical_plan(
                            session_ctx.deref(),
                            self.state.codec.logical_extension_codec(),
                        )
                    }) {
                        Ok(plan) => plan,
                        Err(e) => {
                            let msg = format!("Could not parse logical plan protobuf: {e}");
                            error!("{}", msg);
                            return Ok(Response::new(ExecuteQueryResult {
                                result: Some(execute_query_result::Result::Failure(
                                    ExecuteQueryFailureResult {
                                        failure: Some(execute_query_failure_result::Failure::PlanParsingFailure(msg)),
                                    },
                                )),
                            }));
                        }
                    }
                }
                Query::Sql(sql) => {
                    match session_ctx
                        .sql(&sql)
                        .await
                        .and_then(|df| df.into_optimized_plan())
                    {
                        Ok(plan) => plan,
                        Err(e) => {
                            let msg = format!("Error parsing SQL: {e}");
                            error!("{}", msg);
                            return Ok(Response::new(ExecuteQueryResult {
                                result: Some(execute_query_result::Result::Failure(
                                    ExecuteQueryFailureResult {
                                        failure: Some(execute_query_failure_result::Failure::PlanParsingFailure(msg)),
                                    },
                                )),
                            }));
                        }
                    }
                }
            };

            debug!("Received plan for execution: {:?}", plan);

            let job_id = self.state.task_manager.generate_job_id();
            let job_name = query_settings
                .get(BALLISTA_JOB_NAME)
                .cloned()
                .unwrap_or_else(|| "None".to_string());

            self.submit_job(&job_id, &job_name, session_ctx, &plan)
                .await
                .map_err(|e| {
                    let msg = format!("Failed to send JobQueued event for {job_id}: {e:?}");
                    error!("{}", msg);

                    Status::internal(msg)
                })?;

            Ok(Response::new(ExecuteQueryResult {
                result: Some(execute_query_result::Result::Success(
                    ExecuteQuerySuccessResult { job_id, session_id },
                )),
            }))
        } else {
            Err(Status::internal("Error parsing request"))
        }
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusParams>,
    ) -> Result<Response<GetJobStatusResult>, Status> {
        let job_id = request.into_inner().job_id;
        trace!("Received get_job_status request for job {}", job_id);
        match self.state.task_manager.get_job_status(&job_id).await {
            Ok(status) => Ok(Response::new(GetJobStatusResult { status })),
            Err(e) => {
                let msg = format!("Error getting status for job {job_id}: {e:?}");
                error!("{}", msg);
                Err(Status::internal(msg))
            }
        }
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobParams>,
    ) -> Result<Response<CancelJobResult>, Status> {
        let job_id = request.into_inner().job_id;
        info!("Received cancellation request for job {}", job_id);

        self.query_stage_event_loop
            .get_sender()
            .map_err(|e| {
                let msg = format!("Get query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?
            .post_event(QueryStageSchedulerEvent::JobCancel(job_id))
            .await
            .map_err(|e| {
                let msg = format!("Post to query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(CancelJobResult { cancelled: true }))
    }

    async fn clean_job_data(
        &self,
        request: Request<CleanJobDataParams>,
    ) -> Result<Response<CleanJobDataResult>, Status> {
        let job_id = request.into_inner().job_id;
        info!("Received clean data request for job {}", job_id);

        self.query_stage_event_loop
            .get_sender()
            .map_err(|e| {
                let msg = format!("Get query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?
            .post_event(QueryStageSchedulerEvent::JobDataClean(job_id))
            .await
            .map_err(|e| {
                let msg = format!("Post to query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(CleanJobDataResult {}))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion_proto::protobuf::LogicalPlanNode;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use tonic::Request;

    use crate::config::SchedulerConfig;
    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        executor_status, ExecutorRegistration, ExecutorStatus, HeartBeatParams,
        RegisterExecutorParams,
    };
    use ballista_core::serde::scheduler::ExecutorSpecification;
    use ballista_core::serde::BallistaCodec;

    use crate::test_utils::test_cluster_context;

    use super::{SchedulerGrpc, SchedulerServer};

    #[tokio::test]
    async fn test_register_executor_in_heartbeat_service() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default();
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster,
                BallistaCodec::default(),
                Arc::new(config),
            );
        scheduler.init().await?;

        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };

        let request: Request<HeartBeatParams> = Request::new(HeartBeatParams {
            executor_id: exec_meta.id.clone(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active("".to_string())),
            }),
            metadata: Some(exec_meta.clone()),
        });
        scheduler
            .heart_beat_from_executor(request)
            .await
            .expect("Received error response");

        let state = scheduler.state.clone();
        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "localhost".to_owned());

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_expired_executor() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default();
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster.clone(),
                BallistaCodec::default(),
                Arc::new(config),
            );
        scheduler.init().await?;

        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };

        let request: Request<RegisterExecutorParams> = Request::new(RegisterExecutorParams {
            metadata: Some(exec_meta.clone()),
        });
        let response = scheduler
            .register_executor(request)
            .await
            .expect("Received error response")
            .into_inner();

        // registration should success
        assert!(response.success);

        let state = scheduler.state.clone();
        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        // heartbeat from the executor
        let request: Request<HeartBeatParams> = Request::new(HeartBeatParams {
            executor_id: "abc".to_owned(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active("".to_string())),
            }),
            metadata: Some(exec_meta.clone()),
        });

        let _response = scheduler
            .heart_beat_from_executor(request)
            .await
            .expect("Received error response")
            .into_inner();

        let active_executors = state.executor_manager.get_alive_executors();
        assert_eq!(active_executors.len(), 1);

        let expired_executors = state.executor_manager.get_expired_executors();
        assert!(expired_executors.is_empty());

        // simulate the heartbeat timeout
        tokio::time::sleep(Duration::from_secs(
            scheduler.config.executor_timeout_seconds,
        ))
        .await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        // executor should be marked to dead
        assert!(state.executor_manager.is_dead_executor("abc"));

        let active_executors = state.executor_manager.get_alive_executors();
        assert!(active_executors.is_empty());
        Ok(())
    }
}
