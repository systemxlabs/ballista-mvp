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

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::Stream;
use log::{debug, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{
    job_status, AvailableTaskSlots, ExecutorHeartbeat, JobStatus,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata, PartitionId};
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::default_session_builder;

use crate::cluster::kv::KeyValueState;
use crate::cluster::storage::etcd::EtcdClient;
use crate::cluster::storage::sled::SledClient;
use crate::cluster::storage::KeyValueStore;
use crate::config::{ClusterStorageConfig, SchedulerConfig, TaskDistributionPolicy};
use crate::scheduler_server::SessionBuilder;
use crate::state::execution_graph::{create_task_info, ExecutionGraph, TaskDescription};
use crate::state::task_manager::JobInfoCache;

pub mod kv;
pub mod storage;

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
pub mod test_util;

#[derive(Clone)]
pub struct BallistaCluster {
    cluster_state: Arc<dyn ClusterState>,
    job_state: Arc<dyn JobState>,
}

impl BallistaCluster {
    pub fn new(cluster_state: Arc<dyn ClusterState>, job_state: Arc<dyn JobState>) -> Self {
        Self {
            cluster_state,
            job_state,
        }
    }

    pub fn new_kv<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        store: S,
        scheduler: impl Into<String>,
        session_builder: SessionBuilder,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        let kv_state = Arc::new(KeyValueState::new(scheduler, store, codec, session_builder));
        Self {
            cluster_state: kv_state.clone(),
            job_state: kv_state,
        }
    }

    pub async fn new_from_config(config: &SchedulerConfig) -> Result<Self> {
        let scheduler = config.scheduler_name();

        match &config.cluster_storage {
            ClusterStorageConfig::Etcd(urls) => {
                let etcd = etcd_client::Client::connect(urls.as_slice(), None)
                    .await
                    .map_err(|err| {
                        BallistaError::Internal(format!("Could not connect to etcd: {err:?}"))
                    })?;

                Ok(Self::new_kv(
                    EtcdClient::new(config.namespace.clone(), etcd),
                    scheduler,
                    default_session_builder,
                    BallistaCodec::default(),
                ))
            }

            ClusterStorageConfig::Sled(dir) => {
                if let Some(dir) = dir.as_ref() {
                    info!("Initializing Sled database in directory {}", dir);
                    let sled = SledClient::try_new(dir)?;

                    Ok(Self::new_kv(
                        sled,
                        scheduler,
                        default_session_builder,
                        BallistaCodec::default(),
                    ))
                } else {
                    info!("Initializing Sled database in temp directory");
                    let sled = SledClient::try_new_temporary()?;

                    Ok(Self::new_kv(
                        sled,
                        scheduler,
                        default_session_builder,
                        BallistaCodec::default(),
                    ))
                }
            }
        }
    }

    pub fn cluster_state(&self) -> Arc<dyn ClusterState> {
        self.cluster_state.clone()
    }

    pub fn job_state(&self) -> Arc<dyn JobState> {
        self.job_state.clone()
    }
}

/// Stream of `ExecutorHeartbeat`. This stream should contain all `ExecutorHeartbeats` received
/// by any schedulers with a shared `ClusterState`
pub type ExecutorHeartbeatStream = Pin<Box<dyn Stream<Item = ExecutorHeartbeat> + Send>>;

/// A task bound with an executor to execute.
/// BoundTask.0 is the executor id; While BoundTask.1 is the task description.
pub type BoundTask = (String, TaskDescription);

/// ExecutorSlot.0 is the executor id; While ExecutorSlot.1 is for slot number.
pub type ExecutorSlot = (String, u32);

/// A trait that contains the necessary method to maintain a globally consistent view of cluster resources
#[tonic::async_trait]
pub trait ClusterState: Send + Sync + 'static {
    /// Initialize when it's necessary, especially for state with backend storage
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    /// Bind the ready to running tasks from [`active_jobs`] with available executors.
    ///
    /// If `executors` is provided, only bind slots from the specified executor IDs
    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<String, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<BoundTask>>;

    /// Unbind executor and task when a task finishes or fails. It will increase the executor
    /// available task slots.
    ///
    /// This operations should be atomic. Either all reservations are cancelled or none are
    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()>;

    /// Register a new executor in the cluster.
    async fn register_executor(&self, metadata: ExecutorMetadata, spec: ExecutorData)
        -> Result<()>;

    /// Save the executor metadata. This will overwrite existing metadata for the executor ID
    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()>;

    /// Get executor metadata for the provided executor ID. Returns an error if the executor does not exist
    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata>;

    /// Save the executor heartbeat
    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()>;

    /// Remove the executor from the cluster
    async fn remove_executor(&self, executor_id: &str) -> Result<()>;

    /// Return a map of the last seen heartbeat for all active executors
    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat>;

    /// Get executor heartbeat for the provided executor ID. Return None if the executor does not exist
    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat>;
}

/// A trait that contains the necessary methods for persisting state related to executing jobs
#[tonic::async_trait]
pub trait JobState: Send + Sync {
    /// Accept job into  a scheduler's job queue. This should be called when a job is
    /// received by the scheduler but before it is planned and may or may not be saved
    /// in global state
    fn accept_job(&self, job_id: &str, queued_at: u64) -> Result<()>;

    /// Submit a new job to the `JobState`. It is assumed that the submitter owns the job.
    /// In local state the job should be save as `JobStatus::Active` and in shared state
    /// it should be saved as `JobStatus::Running` with `scheduler` set to the current scheduler
    async fn submit_job(&self, job_id: String, graph: &ExecutionGraph) -> Result<()>;

    /// Return a `Vec` of all active job IDs in the `JobState`
    async fn get_jobs(&self) -> Result<HashSet<String>>;

    /// Fetch the job status
    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>>;

    /// Get the `ExecutionGraph` for job. The job may or may not belong to the caller
    /// and should return the `ExecutionGraph` for the given job (if it exists) at the
    /// time this method is called with no guarantees that the graph has not been
    /// subsequently updated by another scheduler.
    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>>;

    /// Persist the current state of an owned job to global state. This should fail
    /// if the job is not owned by the caller.
    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()>;

    /// Mark a job which has not been submitted as failed. This should be called if a job fails
    /// during planning (and does not yet have an `ExecutionGraph`)
    async fn fail_unscheduled_job(&self, job_id: &str, reason: String) -> Result<()>;

    /// Delete a job from the global state
    async fn remove_job(&self, job_id: &str) -> Result<()>;

    /// Get the `SessionContext` associated with `session_id`. Returns an error if the
    /// session does not exist
    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>>;

    /// Create a new saved session
    async fn create_session(&self, config: &HashMap<String, String>)
        -> Result<Arc<SessionContext>>;
}

pub(crate) async fn bind_task_bias(
    mut slots: Vec<&mut AvailableTaskSlots>,
    active_jobs: Arc<HashMap<String, JobInfoCache>>,
) -> Vec<BoundTask> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    let total_slots = slots.iter().fold(0, |acc, s| acc + s.slots);
    if total_slots == 0 {
        warn!("Not enough available executor slots for task running!!!");
        return schedulable_tasks;
    }

    // Sort the slots by descending order
    slots.sort_by(|a, b| Ord::cmp(&b.slots, &a.slots));

    let mut idx_slot = 0usize;
    let mut slot = &mut slots[idx_slot];
    for (job_id, job_info) in active_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!(
                "Job {} is not in running status and will be skipped",
                job_id
            );
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        while let Some((running_stage, task_id_gen)) = graph.fetch_running_stage() {
            // We are sure that it will at least bind one task by going through the following logic.
            // It will not go into a dead loop.
            let runnable_tasks = running_stage
                .task_infos
                .iter_mut()
                .enumerate()
                .filter(|(_partition, info)| info.is_none())
                .take(total_slots as usize)
                .collect::<Vec<_>>();
            for (partition_id, task_info) in runnable_tasks {
                // Assign [`slot`] with a slot available slot number larger than 0
                while slot.slots == 0 {
                    idx_slot += 1;
                    if idx_slot >= slots.len() {
                        return schedulable_tasks;
                    }
                    slot = &mut slots[idx_slot];
                }
                let executor_id = slot.executor_id.clone();
                let task_id = *task_id_gen;
                *task_id_gen += 1;
                *task_info = Some(create_task_info(executor_id.clone(), task_id));

                let partition = PartitionId {
                    job_id: job_id.clone(),
                    stage_id: running_stage.stage_id,
                    partition_id,
                };
                let task_desc = TaskDescription {
                    session_id: session_id.clone(),
                    partition,
                    task_id,
                    plan: running_stage.plan.clone(),
                };
                schedulable_tasks.push((executor_id, task_desc));

                slot.slots -= 1;
            }
        }
    }

    schedulable_tasks
}

pub(crate) async fn bind_task_round_robin(
    mut slots: Vec<&mut AvailableTaskSlots>,
    active_jobs: Arc<HashMap<String, JobInfoCache>>,
) -> Vec<BoundTask> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    let mut total_slots = slots.iter().fold(0, |acc, s| acc + s.slots);
    if total_slots == 0 {
        warn!("Not enough available executor slots for task running!!!");
        return schedulable_tasks;
    }
    info!("Total slot number is {}", total_slots);

    // Sort the slots by descending order
    slots.sort_by(|a, b| Ord::cmp(&b.slots, &a.slots));

    let mut idx_slot = 0usize;
    for (job_id, job_info) in active_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!(
                "Job {} is not in running status and will be skipped",
                job_id
            );
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        while let Some((running_stage, task_id_gen)) = graph.fetch_running_stage() {
            // We are sure that it will at least bind one task by going through the following logic.
            // It will not go into a dead loop.
            let runnable_tasks = running_stage
                .task_infos
                .iter_mut()
                .enumerate()
                .filter(|(_partition, info)| info.is_none())
                .take(total_slots as usize)
                .collect::<Vec<_>>();
            for (partition_id, task_info) in runnable_tasks {
                // Move to the index which has available slots
                if idx_slot >= slots.len() {
                    idx_slot = 0;
                }
                if slots[idx_slot].slots == 0 {
                    idx_slot = 0;
                }
                // Since the slots is a vector with descending order, and the total available slots is larger than 0,
                // we are sure the available slot number at idx_slot is larger than 1
                let slot = &mut slots[idx_slot];
                let executor_id = slot.executor_id.clone();
                let task_id = *task_id_gen;
                *task_id_gen += 1;
                *task_info = Some(create_task_info(executor_id.clone(), task_id));

                let partition = PartitionId {
                    job_id: job_id.clone(),
                    stage_id: running_stage.stage_id,
                    partition_id,
                };
                let task_desc = TaskDescription {
                    session_id: session_id.clone(),
                    partition,
                    task_id,
                    plan: running_stage.plan.clone(),
                };
                schedulable_tasks.push((executor_id, task_desc));

                idx_slot += 1;
                slot.slots -= 1;
                total_slots -= 1;
                if total_slots == 0 {
                    return schedulable_tasks;
                }
            }
        }
    }

    schedulable_tasks
}
