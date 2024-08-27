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
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{accept, ExecutionPlan, ExecutionPlanVisitor};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{debug, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    self, execution_graph_stage::StageType, JobStatus, RunningJob, SuccessfulJob, TaskStatus,
};
use ballista_core::serde::protobuf::{job_status, FailedJob, ShuffleWritePartition};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PartitionStats,
};
use ballista_core::serde::BallistaCodec;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::display::print_stage_metrics;
use crate::planner::DistributedPlanner;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_stage::RunningStage;
pub(crate) use crate::state::execution_stage::{
    ExecutionStage, ResolvedStage, SuccessfulStage, TaskInfo, UnresolvedStage,
};
use crate::state::task_manager::UpdatedStages;

/// Represents the DAG for a distributed query plan.
///
/// A distributed query plan consists of a set of stages which must be executed sequentially.
///
/// Each stage consists of a set of partitions which can be executed in parallel, where each partition
/// represents a `Task`, which is the basic unit of scheduling in Ballista.
///
/// As an example, consider a SQL query which performs a simple aggregation:
///
/// `SELECT id, SUM(gmv) FROM some_table GROUP BY id`
///
/// This will produce a DataFusion execution plan that looks something like
///
///
///   CoalesceBatchesExec: target_batch_size=4096
///     RepartitionExec: partitioning=Hash([Column { name: "id", index: 0 }], 4)
///       AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(some_table.gmv)]
///         TableScan: some_table
///
/// The Ballista `DistributedPlanner` will turn this into a distributed plan by creating a shuffle
/// boundary (called a "Stage") whenever the underlying plan needs to perform a repartition.
/// In this case we end up with a distributed plan with two stages:
///
///
/// ExecutionGraph[job_id=job, session_id=session, available_tasks=1, complete=false]
/// =========UnResolvedStage[id=2, children=1]=========
/// Inputs{1: StageOutput { partition_locations: {}, complete: false }}
/// ShuffleWriterExec: None
///   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     CoalesceBatchesExec: target_batch_size=4096
///       UnresolvedShuffleExec
/// =========ResolvedStage[id=1, partitions=1]=========
/// ShuffleWriterExec: Some(Hash([Column { name: "id", index: 0 }], 4))
///   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     TableScan: some_table
///
///
/// The DAG structure of this `ExecutionGraph` is encoded in the stages. Each stage's `input` field
/// will indicate which stages it depends on, and each stage's `output_links` will indicate which
/// stage it needs to publish its output to.
///
/// If a stage has `output_links` is empty then it is the final stage in this query, and it should
/// publish its outputs to the `ExecutionGraph`s `output_locations` representing the final query results.
#[derive(Clone)]
pub struct ExecutionGraph {
    /// Curator scheduler name. Can be `None` is `ExecutionGraph` is not currently curated by any scheduler
    scheduler_id: Option<String>,
    /// ID for this job
    job_id: String,
    /// Session ID for this job
    session_id: String,
    /// Status of this job
    status: JobStatus,
    /// Timestamp of when this job was submitted
    queued_at: u64,
    /// Job start time
    start_time: u64,
    /// Job end time
    end_time: u64,
    /// Map from Stage ID -> ExecutionStage
    stages: HashMap<usize, ExecutionStage>,
    /// Total number fo output partitions
    output_partitions: usize,
    /// Locations of this `ExecutionGraph` final output locations
    output_locations: Vec<PartitionLocation>,
    /// Task ID generator, generate unique TID in the execution graph
    task_id_gen: usize,
}

#[derive(Clone, Debug)]
pub struct RunningTaskInfo {
    pub task_id: usize,
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
    pub executor_id: String,
}

impl ExecutionGraph {
    pub fn try_new(
        scheduler_id: &str,
        job_id: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
    ) -> Result<Self> {
        let mut planner = DistributedPlanner::new();

        let output_partitions = plan.output_partitioning().partition_count();

        let shuffle_stages = planner.plan_query_stages(job_id, plan)?;
        debug!(
            "Planned shuffle stages: \n{}",
            shuffle_stages
                .iter()
                .map(|stage| DisplayableExecutionPlan::new(stage.as_ref())
                    .indent(true)
                    .to_string())
                .collect::<Vec<_>>()
                .join("\n")
        );

        let builder = ExecutionStageBuilder::new();
        let stages = builder.build(shuffle_stages)?;

        let started_at = timestamp_millis();

        Ok(Self {
            scheduler_id: Some(scheduler_id.to_string()),
            job_id: job_id.to_string(),
            session_id: session_id.to_string(),
            status: JobStatus {
                job_id: job_id.to_string(),
                status: Some(Status::Running(RunningJob {
                    queued_at,
                    started_at,
                    scheduler: scheduler_id.to_string(),
                })),
            },
            queued_at,
            start_time: started_at,
            end_time: 0,
            stages,
            output_partitions,
            output_locations: vec![],
            task_id_gen: 0,
        })
    }

    pub fn job_id(&self) -> &str {
        self.job_id.as_str()
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub fn status(&self) -> &JobStatus {
        &self.status
    }

    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    pub fn end_time(&self) -> u64 {
        self.end_time
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    pub fn next_task_id(&mut self) -> usize {
        let new_tid = self.task_id_gen;
        self.task_id_gen += 1;
        new_tid
    }

    pub(crate) fn stages(&self) -> &HashMap<usize, ExecutionStage> {
        &self.stages
    }

    /// An ExecutionGraph is successful if all its stages are successful
    pub fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    /// Revive the execution graph by converting the resolved stages to running stages
    /// If any stages are converted, return true; else false.
    pub fn revive(&mut self) -> bool {
        let running_stages = self
            .stages
            .values()
            .filter_map(|stage| {
                if let ExecutionStage::Resolved(resolved_stage) = stage {
                    Some(resolved_stage.to_running())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if running_stages.is_empty() {
            false
        } else {
            for running_stage in running_stages {
                self.stages.insert(
                    running_stage.stage_id,
                    ExecutionStage::Running(running_stage),
                );
            }
            true
        }
    }

    /// Update task statuses and task metrics in the graph.
    /// This will also push shuffle partitions to their respective shuffle read stages.
    pub fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            let stage_id = task_status.stage_id as usize;
            let stage_task_statuses = job_task_statuses.entry(stage_id).or_default();
            stage_task_statuses.push(task_status);
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        let mut resolved_stages = HashSet::new();
        let mut successful_stages = HashSet::new();
        let mut failed_stages = HashMap::new();

        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        let partition_id = task_status.clone().partition_id as usize;
                        let task_identity = format!(
                            "TID {} {}/{}/{}",
                            task_status.task_id, job_id, stage_id, partition_id
                        );
                        let operator_metrics = task_status.metrics.clone();

                        if !running_stage.update_task_info(partition_id, task_status.clone()) {
                            continue;
                        }

                        if let Some(task_status::Status::Failed(failed_task)) = task_status.status {
                            failed_stages.insert(stage_id, failed_task.error);
                        } else if let Some(task_status::Status::Successful(successful_task)) =
                            task_status.status
                        {
                            // update task metrics for successfu task
                            running_stage.update_task_metrics(partition_id, operator_metrics)?;

                            locations.append(&mut partition_to_location(
                                &job_id,
                                partition_id,
                                stage_id,
                                executor,
                                successful_task.partitions,
                            ));
                        } else {
                            warn!(
                                "The task {}'s status is invalid for updating",
                                task_identity
                            );
                        }
                    }

                    let is_final_successful = running_stage.is_successful();
                    if is_final_successful {
                        successful_stages.insert(stage_id);
                        // if this stage is final successful, we want to combine the stage metrics to plan's metric set and print out the plan
                        if let Some(stage_metrics) = running_stage.stage_metrics.as_ref() {
                            print_stage_metrics(
                                &job_id,
                                stage_id,
                                running_stage.plan.as_ref(),
                                stage_metrics,
                            );
                        }
                    }

                    let output_links = running_stage.output_links.clone();
                    resolved_stages.extend(
                        &mut self
                            .update_stage_output_links(
                                stage_id,
                                is_final_successful,
                                locations,
                                output_links,
                            )?
                            .into_iter(),
                    );
                } else if let ExecutionStage::UnResolved(_unsolved_stage) = stage {
                    for task_status in stage_task_statuses.into_iter() {
                        // handle delayed failed tasks if the stage's next attempt is still in UnResolved status.
                        if let Some(task_status::Status::Failed(failed_task)) = task_status.status {
                            failed_stages.insert(stage_id, failed_task.error);
                        }
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in running when updating the status of tasks {:?}",
                        job_id,
                        stage_id,
                        stage_task_statuses
                            .into_iter()
                            .map(|task_status| task_status.partition_id)
                            .collect::<Vec<_>>(),
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        self.processing_stages_update(UpdatedStages {
            resolved_stages,
            successful_stages,
            failed_stages,
        })
    }

    /// Processing stage status update after task status changing
    fn processing_stages_update(
        &mut self,
        updated_stages: UpdatedStages,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        let mut has_resolved = false;
        let mut job_err_msg = "".to_owned();

        for stage_id in updated_stages.resolved_stages {
            self.resolve_stage(stage_id)?;
            has_resolved = true;
        }

        for stage_id in updated_stages.successful_stages {
            self.succeed_stage(stage_id);
        }

        // Fail the stage and also abort the job
        for (stage_id, err_msg) in &updated_stages.failed_stages {
            job_err_msg = format!("Job failed due to stage {stage_id} failed: {err_msg}\n");
        }

        let mut events = vec![];

        if !updated_stages.failed_stages.is_empty() {
            info!("Job {} is failed", job_id);
            self.fail_job(job_err_msg.clone());
            events.push(QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message: job_err_msg,
                queued_at: self.queued_at,
                failed_at: timestamp_millis(),
            });
        } else if self.is_successful() {
            // If this ExecutionGraph is successful, finish it
            info!("Job {} is success, finalizing output partitions", job_id);
            self.succeed_job()?;
            events.push(QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at: self.queued_at,
                completed_at: timestamp_millis(),
            });
        } else if has_resolved {
            events.push(QueryStageSchedulerEvent::JobUpdated(job_id))
        }
        Ok(events)
    }

    /// Return a Vec of resolvable stage ids
    fn update_stage_output_links(
        &mut self,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
        output_links: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut resolved_stages = vec![];
        let job_id = &self.job_id;
        if output_links.is_empty() {
            // If `output_links` is empty, then this is a final stage
            self.output_locations.extend(locations);
        } else {
            for link in output_links.iter() {
                // If this is an intermediate stage, we need to push its `PartitionLocation`s to the parent stage
                if let Some(linked_stage) = self.stages.get_mut(link) {
                    if let ExecutionStage::UnResolved(linked_unresolved_stage) = linked_stage {
                        linked_unresolved_stage
                            .add_input_partitions(stage_id, locations.clone())?;

                        // If all tasks for this stage are complete, mark the input complete in the parent stage
                        if is_completed {
                            linked_unresolved_stage.complete_input(stage_id);
                        }

                        // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the parent stage plan
                        if linked_unresolved_stage.resolvable() {
                            resolved_stages.push(linked_unresolved_stage.stage_id);
                        }
                    } else {
                        return Err(BallistaError::Internal(format!(
                            "Error updating job {job_id}: The stage {link} as the output link of stage {stage_id}  should be unresolved"
                        )));
                    }
                } else {
                    return Err(BallistaError::Internal(format!(
                        "Error updating job {job_id}: Invalid output link {stage_id} for stage {link}"
                    )));
                }
            }
        }
        Ok(resolved_stages)
    }

    /// Return all currently running tasks along with the executor ID on which they are assigned
    pub fn running_tasks(&self) -> Vec<RunningTaskInfo> {
        self.stages
            .iter()
            .flat_map(|(_, stage)| {
                if let ExecutionStage::Running(stage) = stage {
                    stage
                        .running_tasks()
                        .into_iter()
                        .map(
                            |(task_id, stage_id, partition_id, executor_id)| RunningTaskInfo {
                                task_id,
                                job_id: self.job_id.clone(),
                                stage_id,
                                partition_id,
                                executor_id,
                            },
                        )
                        .collect::<Vec<RunningTaskInfo>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<RunningTaskInfo>>()
    }

    /// Total number of tasks in this plan that are ready for scheduling
    pub fn available_tasks(&self) -> usize {
        self.stages
            .values()
            .map(|stage| {
                if let ExecutionStage::Running(stage) = stage {
                    stage.available_tasks()
                } else {
                    0
                }
            })
            .sum()
    }

    pub(crate) fn fetch_running_stage(&mut self) -> Option<(&mut RunningStage, &mut usize)> {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            warn!("Call fetch_runnable_stage on failed Job");
            return None;
        }

        let running_stage_id = self.get_running_stage_id();
        if let Some(running_stage_id) = running_stage_id {
            if let Some(ExecutionStage::Running(running_stage)) =
                self.stages.get_mut(&running_stage_id)
            {
                Some((running_stage, &mut self.task_id_gen))
            } else {
                warn!("Fail to find running stage with id {running_stage_id}");
                None
            }
        } else {
            None
        }
    }

    fn get_running_stage_id(&mut self) -> Option<usize> {
        let mut running_stage_id = self.stages.iter().find_map(|(stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                if stage.available_tasks() > 0 {
                    Some(*stage_id)
                } else {
                    None
                }
            } else {
                None
            }
        });

        // If no available tasks found in the running stage,
        // try to find a resolved stage and convert it to the running stage
        if running_stage_id.is_none() {
            if self.revive() {
                running_stage_id = self.get_running_stage_id();
            } else {
                running_stage_id = None;
            }
        }

        running_stage_id
    }

    pub fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }

    /// Convert unresolved stage to be resolved
    pub fn resolve_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::UnResolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Resolved(stage.to_resolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a unresolved stage {}/{} to resolve",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert running stage to be successful
    pub fn succeed_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Successful(stage.to_successful()));
            true
        } else {
            warn!(
                "Fail to find a running stage {}/{} to make it success",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// fail job with error message
    pub fn fail_job(&mut self, error: String) {
        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            status: Some(Status::Failed(FailedJob {
                error,
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
    }

    /// Mark the job success
    pub fn succeed_job(&mut self) -> Result<()> {
        if !self.is_successful() {
            return Err(BallistaError::Internal(format!(
                "Attempt to finalize an incomplete job {}",
                self.job_id()
            )));
        }

        let partition_location = self
            .output_locations()
            .into_iter()
            .map(|l| l.try_into())
            .collect::<Result<Vec<_>>>()?;

        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            status: Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,

                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };

        Ok(())
    }

    pub(crate) async fn decode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        proto: protobuf::ExecutionGraph,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<ExecutionGraph> {
        let mut stages: HashMap<usize, ExecutionStage> = HashMap::new();
        for graph_stage in proto.stages {
            let stage_type = graph_stage.stage_type.expect("Unexpected empty stage");

            let execution_stage = match stage_type {
                StageType::UnresolvedStage(stage) => {
                    let stage: UnresolvedStage =
                        UnresolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::UnResolved(stage))
                }
                StageType::ResolvedStage(stage) => {
                    let stage: ResolvedStage = ResolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Resolved(stage))
                }
                StageType::SuccessfulStage(stage) => {
                    let stage: SuccessfulStage =
                        SuccessfulStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Successful(stage))
                }
            };

            stages.insert(execution_stage.0, execution_stage.1);
        }

        let output_locations: Vec<PartitionLocation> = proto
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ExecutionGraph {
            scheduler_id: (!proto.scheduler_id.is_empty()).then_some(proto.scheduler_id),
            job_id: proto.job_id,
            session_id: proto.session_id,
            status: proto.status.ok_or_else(|| {
                BallistaError::Internal("Invalid Execution Graph: missing job status".to_owned())
            })?,
            queued_at: proto.queued_at,
            start_time: proto.start_time,
            end_time: proto.end_time,
            stages,
            output_partitions: proto.output_partitions as usize,
            output_locations,
            task_id_gen: proto.task_id_gen as usize,
        })
    }

    /// Running stages will not be persisted so that will not be encoded.
    /// Running stages will be convert back to the resolved stages to be encoded and persisted
    pub(crate) fn encode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        graph: ExecutionGraph,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ExecutionGraph> {
        let job_id = graph.job_id().to_owned();

        let stages = graph
            .stages
            .into_values()
            .map(|stage| {
                let stage_type = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        StageType::UnresolvedStage(UnresolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Resolved(stage) => {
                        StageType::ResolvedStage(ResolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Running(stage) => {
                        StageType::ResolvedStage(ResolvedStage::encode(stage.to_resolved(), codec)?)
                    }
                    ExecutionStage::Successful(stage) => StageType::SuccessfulStage(
                        SuccessfulStage::encode(job_id.clone(), stage, codec)?,
                    ),
                };
                Ok(protobuf::ExecutionGraphStage {
                    stage_type: Some(stage_type),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_locations: Vec<protobuf::PartitionLocation> = graph
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(protobuf::ExecutionGraph {
            job_id: graph.job_id,
            session_id: graph.session_id,
            status: Some(graph.status),
            queued_at: graph.queued_at,
            start_time: graph.start_time,
            end_time: graph.end_time,
            stages,
            output_partitions: graph.output_partitions as u64,
            output_locations,
            scheduler_id: graph.scheduler_id.unwrap_or_default(),
            task_id_gen: graph.task_id_gen as u32,
        })
    }
}

impl Debug for ExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .values()
            .map(|stage| format!("{stage:?}"))
            .collect::<Vec<String>>()
            .join("");
        write!(
            f,
            "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, is_successful={}]\n{}",
            self.job_id,
            self.session_id,
            self.available_tasks(),
            self.is_successful(),
            stages
        )
    }
}

pub(crate) fn create_task_info(executor_id: String, task_id: usize) -> TaskInfo {
    TaskInfo {
        task_id,
        scheduled_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        // Those times will be updated when the task finish
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        finish_time: 0,
        task_status: task_status::Status::Running(RunningTask { executor_id }),
    }
}

/// Utility for building a set of `ExecutionStage`s from
/// a list of `ShuffleWriterExec`.
///
/// This will infer the dependency structure for the stages
/// so that we can construct a DAG from the stages.
struct ExecutionStageBuilder {
    /// Stage ID which is currently being visited
    current_stage_id: usize,
    /// Map from stage ID -> List of child stage IDs
    stage_dependencies: HashMap<usize, Vec<usize>>,
    /// Map from Stage ID -> output link
    output_links: HashMap<usize, Vec<usize>>,
}

impl ExecutionStageBuilder {
    pub fn new() -> Self {
        Self {
            current_stage_id: 0,
            stage_dependencies: HashMap::new(),
            output_links: HashMap::new(),
        }
    }

    pub fn build(
        mut self,
        stages: Vec<Arc<ShuffleWriterExec>>,
    ) -> Result<HashMap<usize, ExecutionStage>> {
        let mut execution_stages: HashMap<usize, ExecutionStage> = HashMap::new();
        // First, build the dependency graph
        for stage in &stages {
            accept(stage.as_ref(), &mut self)?;
        }

        // Now, create the execution stages
        for stage in stages {
            let stage_id = stage.stage_id();
            let output_links = self.output_links.remove(&stage_id).unwrap_or_default();

            let child_stages = self
                .stage_dependencies
                .remove(&stage_id)
                .unwrap_or_default();

            let stage = if child_stages.is_empty() {
                ExecutionStage::Resolved(ResolvedStage::new(
                    stage_id,
                    stage,
                    output_links,
                    HashMap::new(),
                ))
            } else {
                ExecutionStage::UnResolved(UnresolvedStage::new(
                    stage_id,
                    stage,
                    output_links,
                    child_stages,
                ))
            };
            execution_stages.insert(stage_id, stage);
        }

        Ok(execution_stages)
    }
}

impl ExecutionPlanVisitor for ExecutionStageBuilder {
    type Error = BallistaError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> std::result::Result<bool, Self::Error> {
        if let Some(shuffle_write) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
            self.current_stage_id = shuffle_write.stage_id();
        } else if let Some(unresolved_shuffle) =
            plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            if let Some(output_links) = self.output_links.get_mut(&unresolved_shuffle.stage_id) {
                if !output_links.contains(&self.current_stage_id) {
                    output_links.push(self.current_stage_id);
                }
            } else {
                self.output_links
                    .insert(unresolved_shuffle.stage_id, vec![self.current_stage_id]);
            }

            if let Some(deps) = self.stage_dependencies.get_mut(&self.current_stage_id) {
                if !deps.contains(&unresolved_shuffle.stage_id) {
                    deps.push(unresolved_shuffle.stage_id);
                }
            } else {
                self.stage_dependencies
                    .insert(self.current_stage_id, vec![unresolved_shuffle.stage_id]);
            }
        }
        Ok(true)
    }
}

/// Represents the basic unit of work for the Ballista executor. Will execute
/// one partition of one stage on one task slot.
#[derive(Clone)]
pub struct TaskDescription {
    pub session_id: String,
    pub partition: PartitionId,
    pub task_id: usize,
    pub plan: Arc<dyn ExecutionPlan>,
}

impl Debug for TaskDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);
        write!(
            f,
            "TaskDescription[session_id: {}, job: {}, stage: {}, partition: {}, task_id: {}]\n{}",
            self.session_id,
            self.partition.job_id,
            self.partition.stage_id,
            self.partition.partition_id,
            self.task_id,
            plan
        )
    }
}

impl TaskDescription {
    pub fn get_output_partition_number(&self) -> usize {
        let shuffle_writer = self
            .plan
            .as_any()
            .downcast_ref::<ShuffleWriterExec>()
            .unwrap();
        shuffle_writer
            .shuffle_output_partitioning()
            .map(|partitioning| partitioning.partition_count())
            .unwrap_or_else(|| 1)
    }
}

fn partition_to_location(
    job_id: &str,
    map_partition_id: usize,
    stage_id: usize,
    executor: &ExecutorMetadata,
    shuffles: Vec<ShuffleWritePartition>,
) -> Vec<PartitionLocation> {
    shuffles
        .into_iter()
        .map(|shuffle| PartitionLocation {
            map_partition_id,
            partition_id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id,
                partition_id: shuffle.partition_id as usize,
            },
            executor_meta: executor.clone(),
            partition_stats: PartitionStats::new(
                Some(shuffle.num_rows),
                Some(shuffle.num_batches),
                Some(shuffle.num_bytes),
            ),
            path: shuffle.path,
        })
        .collect()
}
