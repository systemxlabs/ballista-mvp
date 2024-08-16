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

use ballista_core::{utils, BALLISTA_VERSION};
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use log::{debug, error, info, warn};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::{
    executor_grpc_server::{ExecutorGrpc, ExecutorGrpcServer},
    executor_status,
    scheduler_grpc_client::SchedulerGrpcClient,
    CancelTasksParams, CancelTasksResult, ExecutorStatus, HeartBeatParams, LaunchMultiTaskParams,
    LaunchMultiTaskResult, RemoveJobDataParams, RemoveJobDataResult, TaskStatus,
    UpdateTaskStatusParams,
};
use ballista_core::serde::scheduler::from_proto::get_task_definition_vec;
use ballista_core::serde::scheduler::PartitionId;
use ballista_core::serde::scheduler::TaskDefinition;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{create_grpc_client_connection, create_grpc_server};
use dashmap::DashMap;
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion_proto::{logical_plan::AsLogicalPlan, physical_plan::AsExecutionPlan};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::executor_process::ExecutorProcessConfig;
use crate::{as_task_status, TaskExecutionTimes};

type ServerHandle = JoinHandle<Result<(), BallistaError>>;
type SchedulerClients = Arc<DashMap<String, SchedulerGrpcClient<Channel>>>;

/// Wrap TaskDefinition with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskDefinition {
    scheduler_id: String,
    task: TaskDefinition,
}

/// Wrap TaskStatus with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskStatus {
    scheduler_id: String,
    task_status: TaskStatus,
}

pub async fn startup<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    scheduler: SchedulerGrpcClient<Channel>,
    config: Arc<ExecutorProcessConfig>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
) -> Result<ServerHandle, BallistaError> {
    let channel_buf_size = executor.concurrent_tasks * 50;
    let (tx_task, rx_task) = mpsc::channel::<CuratorTaskDefinition>(channel_buf_size);
    let (tx_task_status, rx_task_status) = mpsc::channel::<CuratorTaskStatus>(channel_buf_size);

    let executor_server = ExecutorServer::new(
        scheduler.clone(),
        executor.clone(),
        ExecutorEnv {
            tx_task,
            tx_task_status,
        },
        codec,
    );

    // 1. Start executor grpc service
    let server = {
        let executor_meta = executor.metadata.clone();
        let addr = format!("{}:{}", config.bind_host, executor_meta.grpc_port);
        let addr = addr.parse().unwrap();

        info!(
            "Ballista v{} Rust Executor Grpc Server listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let server = ExecutorGrpcServer::new(executor_server.clone())
            .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
            .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);
        tokio::spawn(async move {
            let grpc_server_future = create_grpc_server().add_service(server).serve(addr);
            grpc_server_future.await.map_err(|e| {
                error!("Tonic error, Could not start Executor Grpc Server.");
                BallistaError::TonicError(e)
            })
        })
    };

    let executor_server = Arc::new(executor_server);

    // 2. Start Heartbeater loop
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start(config.executor_heartbeat_interval_seconds);
    }

    // 3. Start TaskRunnerPool loop
    {
        let task_runner_pool = TaskRunnerPool::new(executor_server.clone());
        task_runner_pool.start(rx_task, rx_task_status);
    }

    Ok(server)
}

#[derive(Clone)]
pub struct ExecutorServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    _start_time: u128,
    executor: Arc<Executor>,
    executor_env: ExecutorEnv,
    codec: BallistaCodec<T, U>,
    scheduler_to_register: SchedulerGrpcClient<Channel>,
    schedulers: SchedulerClients,
}

#[derive(Clone)]
struct ExecutorEnv {
    /// Receive `TaskDefinition` from rpc then send to CPU bound tasks pool `dedicated_executor`.
    tx_task: mpsc::Sender<CuratorTaskDefinition>,
    /// Receive `TaskStatus` from CPU bound tasks pool `dedicated_executor` then use rpc send back to scheduler.
    tx_task_status: mpsc::Sender<CuratorTaskStatus>,
}

unsafe impl Sync for ExecutorEnv {}

/// Global flag indicating whether the executor is terminating. This should be
/// set to `true` when the executor receives a shutdown signal
pub static TERMINATING: AtomicBool = AtomicBool::new(false);

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorServer<T, U> {
    fn new(
        scheduler_to_register: SchedulerGrpcClient<Channel>,
        executor: Arc<Executor>,
        executor_env: ExecutorEnv,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            _start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            executor,
            executor_env,
            codec,
            scheduler_to_register,
            schedulers: Default::default(),
        }
    }

    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<SchedulerGrpcClient<Channel>, BallistaError> {
        let scheduler = self.schedulers.get(scheduler_id).map(|value| value.clone());
        // If channel does not exist, create a new one
        if let Some(scheduler) = scheduler {
            Ok(scheduler)
        } else {
            let scheduler_url = format!("http://{scheduler_id}");
            let connection = create_grpc_client_connection(scheduler_url).await?;
            let scheduler = SchedulerGrpcClient::new(connection)
                .max_encoding_message_size(16 * 1024 * 1024)
                .max_decoding_message_size(16 * 1024 * 1024);

            {
                self.schedulers
                    .insert(scheduler_id.to_owned(), scheduler.clone());
            }

            Ok(scheduler)
        }
    }

    /// 1. First Heartbeat to its registration scheduler, if successful then return; else go next.
    /// 2. Heartbeat to schedulers which has launching tasks to this executor until one succeeds
    async fn heartbeat(&self) {
        let status = if TERMINATING.load(Ordering::Acquire) {
            executor_status::Status::Terminating(String::default())
        } else {
            executor_status::Status::Active(String::default())
        };

        let heartbeat_params = HeartBeatParams {
            executor_id: self.executor.metadata.id.clone(),
            status: Some(ExecutorStatus {
                status: Some(status),
            }),
            metadata: Some(self.executor.metadata.clone()),
        };
        let mut scheduler = self.scheduler_to_register.clone();
        match scheduler
            .heart_beat_from_executor(heartbeat_params.clone())
            .await
        {
            Ok(_) => {
                return;
            }
            Err(e) => {
                warn!(
                    "Fail to update heartbeat to its registration scheduler due to {:?}",
                    e
                );
            }
        };

        for mut item in self.schedulers.iter_mut() {
            let scheduler_id = item.key().clone();
            let scheduler = item.value_mut();

            match scheduler
                .heart_beat_from_executor(heartbeat_params.clone())
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    warn!(
                        "Fail to update heartbeat to scheduler {} due to {:?}",
                        scheduler_id, e
                    );
                }
            }
        }
    }

    /// This method should not return Err. If task fails, a failure task status should be sent
    /// to the channel to notify the scheduler.
    async fn run_task(&self, task_identity: String, curator_task: CuratorTaskDefinition) {
        let start_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        info!("Start to run task {}", task_identity);
        let task = curator_task.task;

        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let partition_id = task.partition_id;
        let plan = task.plan;

        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id,
            partition_id,
        };

        // the query plan created by the scheduler always starts with a ShuffleWriterExec
        let shuffle_writer =
            if let Some(shuffle_writer) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
                // recreate the shuffle writer with the correct working directory
                Ok(ShuffleWriterExec::new(
                    job_id,
                    stage_id,
                    plan.children()[0].clone(),
                    self.executor.work_dir.to_string(),
                    shuffle_writer.shuffle_output_partitioning().cloned(),
                ))
            } else {
                Err(DataFusionError::Internal(
                    "Plan is not a ShuffleWriterExec".to_string(),
                ))
            }
            .unwrap();
        let shuffle_writer = Arc::new(shuffle_writer);

        let task_context = {
            let task_props = task.props;
            let mut config = ConfigOptions::new();
            for (k, v) in task_props.iter() {
                if let Err(e) = config.set(k, v) {
                    debug!("Fail to set session config for ({},{}): {:?}", k, v, e);
                }
            }
            let session_config = SessionConfig::from(config);

            let runtime = self.executor.get_runtime();

            Arc::new(TaskContext::new(
                Some(task_identity.clone()),
                task.session_id,
                session_config,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                runtime,
            ))
        };

        info!("Start to execute shuffle write for task {}", task_identity);

        let execution_result = self
            .executor
            .execute_query_stage(task_id, part.clone(), shuffle_writer.clone(), task_context)
            .await;
        info!("Done with task {}", task_identity);
        debug!("Statistics: {:?}", execution_result);

        let plan_metrics = utils::collect_plan_metrics(shuffle_writer.as_ref());
        let operator_metrics = match plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
        {
            Ok(metrics) => Some(metrics),
            Err(_) => None,
        };
        let executor_id = &self.executor.metadata.id;

        let end_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let task_execution_times = TaskExecutionTimes {
            launch_time: task.launch_time,
            start_exec_time,
            end_exec_time,
        };

        let task_status = as_task_status(
            execution_result,
            executor_id.clone(),
            task_id,
            part,
            operator_metrics,
            task_execution_times,
        );

        let scheduler_id = curator_task.scheduler_id;
        let task_status_sender = self.executor_env.tx_task_status.clone();
        task_status_sender
            .send(CuratorTaskStatus {
                scheduler_id,
                task_status,
            })
            .await
            .unwrap();
    }
}

/// Heartbeater will run forever.
struct Heartbeater<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Heartbeater<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(&self, executor_heartbeat_interval_seconds: u64) {
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting heartbeater to send heartbeat the scheduler periodically");
            loop {
                executor_server.heartbeat().await;
                tokio::time::sleep(Duration::from_secs(executor_heartbeat_interval_seconds)).await;
            }
        });
    }
}

/// There are two loop(future) running separately in tokio runtime.
/// First is for sending back task status to scheduler
/// Second is for receiving task from scheduler and run.
/// The two loops will run forever.
struct TaskRunnerPool<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskRunnerPool<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(
        &self,
        mut rx_task: mpsc::Receiver<CuratorTaskDefinition>,
        mut rx_task_status: mpsc::Receiver<CuratorTaskStatus>,
    ) {
        //1. loop for task status reporting
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting the task status reporter");
            loop {
                let mut curator_task_status_map: HashMap<String, Vec<TaskStatus>> = HashMap::new();
                // First try to fetch task status from the channel in *blocking* mode
                let maybe_task_status: Option<CuratorTaskStatus> = rx_task_status.recv().await;

                let mut fetched_task_num = 0usize;
                if let Some(task_status) = maybe_task_status {
                    let task_status_vec = curator_task_status_map
                        .entry(task_status.scheduler_id)
                        .or_default();
                    task_status_vec.push(task_status.task_status);
                    fetched_task_num += 1;
                } else {
                    info!("Channel is closed and will exit the task status report loop.");
                    return;
                }

                // Then try to fetch by non-blocking mode to fetch as much finished tasks as possible
                loop {
                    match rx_task_status.try_recv() {
                        Ok(task_status) => {
                            let task_status_vec = curator_task_status_map
                                .entry(task_status.scheduler_id)
                                .or_default();
                            task_status_vec.push(task_status.task_status);
                            fetched_task_num += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            info!("Fetched {} tasks status to report", fetched_task_num);
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            info!("Channel is closed and will exit the task status report loop");
                            return;
                        }
                    }
                }

                for (scheduler_id, tasks_status) in curator_task_status_map.into_iter() {
                    match executor_server.get_scheduler_client(&scheduler_id).await {
                        Ok(mut scheduler) => {
                            if let Err(e) = scheduler
                                .update_task_status(UpdateTaskStatusParams {
                                    executor_id: executor_server.executor.metadata.id.clone(),
                                    task_status: tasks_status.clone(),
                                })
                                .await
                            {
                                error!("Fail to update tasks {:?} due to {:?}", tasks_status, e);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Fail to connect to scheduler {} due to {:?}",
                                scheduler_id, e
                            );
                        }
                    }
                }
            }
        });

        //2. loop for task fetching and running
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting the task runner pool");

            // Use a dedicated executor for CPU bound tasks so that the main tokio
            // executor can still answer requests even when under load
            let dedicated_executor =
                DedicatedExecutor::new("task_runner", executor_server.executor.concurrent_tasks);

            loop {
                let maybe_task: Option<CuratorTaskDefinition> = rx_task.recv().await;
                if let Some(curator_task) = maybe_task {
                    let task_identity = format!(
                        "TID {} {}/{}/{}",
                        &curator_task.task.task_id,
                        &curator_task.task.job_id,
                        &curator_task.task.stage_id,
                        &curator_task.task.partition_id,
                    );
                    info!("Received task {:?}", &task_identity);

                    let server = executor_server.clone();
                    dedicated_executor.spawn(async move {
                        server.run_task(task_identity.clone(), curator_task).await;
                    });
                } else {
                    info!("Channel is closed and will exit the task receive loop");
                    return;
                }
            }
        });
    }
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorGrpc
    for ExecutorServer<T, U>
{
    /// by this interface, it can reduce the deserialization cost for multiple tasks
    /// belong to the same job stage running on the same one executor
    async fn launch_multi_task(
        &self,
        request: Request<LaunchMultiTaskParams>,
    ) -> Result<Response<LaunchMultiTaskResult>, Status> {
        let LaunchMultiTaskParams {
            multi_tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for multi_task in multi_tasks {
            let multi_task: Vec<TaskDefinition> = get_task_definition_vec(
                multi_task,
                self.executor.get_runtime(),
                self.codec.clone(),
            )
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
            for task in multi_task {
                task_sender
                    .send(CuratorTaskDefinition {
                        scheduler_id: scheduler_id.clone(),
                        task,
                    })
                    .await
                    .unwrap();
            }
        }
        Ok(Response::new(LaunchMultiTaskResult { success: true }))
    }

    async fn cancel_tasks(
        &self,
        request: Request<CancelTasksParams>,
    ) -> Result<Response<CancelTasksResult>, Status> {
        let task_infos = request.into_inner().task_infos;
        info!("Cancelling tasks for {:?}", task_infos);

        let mut cancelled = true;

        for task in task_infos {
            if let Err(e) = self
                .executor
                .cancel_task(
                    task.task_id as usize,
                    task.job_id,
                    task.stage_id as usize,
                    task.partition_id as usize,
                )
                .await
            {
                error!("Error cancelling task: {:?}", e);
                cancelled = false;
            }
        }

        Ok(Response::new(CancelTasksResult { cancelled }))
    }

    async fn remove_job_data(
        &self,
        request: Request<RemoveJobDataParams>,
    ) -> Result<Response<RemoveJobDataResult>, Status> {
        let job_id = request.into_inner().job_id;

        let work_dir = PathBuf::from(&self.executor.work_dir);
        let mut path = work_dir.clone();
        path.push(&job_id);

        // Verify it's an existing directory
        if !path.is_dir() {
            return if !path.exists() {
                Ok(Response::new(RemoveJobDataResult {}))
            } else {
                Err(Status::invalid_argument(format!(
                    "Path {path:?} is not for a directory!!!"
                )))
            };
        }

        if !is_subdirectory(path.as_path(), work_dir.as_path()) {
            return Err(Status::invalid_argument(format!(
                "Path {path:?} is not a subdirectory of {work_dir:?}!!!"
            )));
        }

        info!("Remove data for job {:?}", job_id);

        std::fs::remove_dir_all(&path)?;

        Ok(Response::new(RemoveJobDataResult {}))
    }
}

// Check whether the path is the subdirectory of the base directory
fn is_subdirectory(path: &Path, base_path: &Path) -> bool {
    if let (Ok(path), Ok(base_path)) = (path.canonicalize(), base_path.canonicalize()) {
        if let Some(parent_path) = path.parent() {
            parent_path.starts_with(base_path)
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::executor_server::is_subdirectory;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_is_subdirectory() {
        let base_dir = TempDir::new().unwrap().into_path();

        // Normal correct one
        {
            let job_path = prepare_testing_job_directory(&base_dir, "job_a");
            assert!(is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Empty job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));

            let job_path = prepare_testing_job_directory(&base_dir, ".");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Malicious job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "..");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
        }
    }

    fn prepare_testing_job_directory(base_dir: &Path, job_id: &str) -> PathBuf {
        let mut path = base_dir.to_path_buf();
        path.push(job_id);
        if !path.exists() {
            fs::create_dir(&path).unwrap();
        }
        path
    }
}
