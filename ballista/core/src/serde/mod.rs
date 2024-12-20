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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use arrow_flight::sql::ProstMessageExt;
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::common::proto_error;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::{
    convert_required,
    logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec},
    physical_plan::PhysicalExtensionCodec,
};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::fmt::Debug;
use std::sync::Arc;
use std::{convert::TryInto, io::Cursor};

use crate::execution_plans::{ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec};
use crate::serde::protobuf::ballista_physical_plan_node::PhysicalPlanType;
use crate::serde::scheduler::PartitionLocation;
pub use generated::ballista as protobuf;

pub mod generated;
pub mod scheduler;

impl ProstMessageExt for protobuf::Action {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.Action"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: protobuf::Action::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{e:?}")))
        .and_then(|node| node.try_into())
}

#[derive(Clone, Debug)]
pub struct BallistaCodec {
    logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
    physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for BallistaCodec {
    fn default() -> Self {
        Self {
            logical_extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            physical_extension_codec: Arc::new(BallistaPhysicalExtensionCodec {}),
        }
    }
}

impl BallistaCodec {
    pub fn new(
        logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
        physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        Self {
            logical_extension_codec,
            physical_extension_codec,
        }
    }

    pub fn logical_extension_codec(&self) -> &dyn LogicalExtensionCodec {
        self.logical_extension_codec.as_ref()
    }

    pub fn physical_extension_codec(&self) -> &dyn PhysicalExtensionCodec {
        self.physical_extension_codec.as_ref()
    }
}

#[derive(Debug)]
pub struct BallistaPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for BallistaPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ballista_plan: protobuf::BallistaPhysicalPlanNode =
            protobuf::BallistaPhysicalPlanNode::decode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Could not deserialize BallistaPhysicalPlanNode: {e}"
                ))
            })?;

        let ballista_plan =
            ballista_plan.physical_plan_type.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "Could not deserialize BallistaPhysicalPlanNode because it's physical_plan_type is none".to_string()
                )
            })?;

        match ballista_plan {
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input = inputs[0].clone();

                let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                    registry,
                    input.schema().as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::new(
                    shuffle_writer.job_id.clone(),
                    shuffle_writer.stage_id as usize,
                    input,
                    "".to_string(), // this is intentional but hacky - the executor will fill this in
                    shuffle_output_partitioning,
                )))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let stage_id = shuffle_reader.stage_id as usize;
                let schema: SchemaRef = Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<Vec<PartitionLocation>> = shuffle_reader
                    .partition
                    .iter()
                    .map(|p| {
                        p.location
                            .iter()
                            .map(|l| {
                                l.clone().try_into().map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Fail to get partition location due to {e:?}"
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let partitioning = parse_protobuf_partitioning(
                    shuffle_reader.partitioning.as_ref(),
                    registry,
                    schema.as_ref(),
                )?
                .expect("partitioning is required");
                let shuffle_reader =
                    ShuffleReaderExec::new(stage_id, partitioning, partition_location, schema);
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::UnresolvedShuffle(unresolved_shuffle) => {
                let schema: SchemaRef = Arc::new(convert_required!(unresolved_shuffle.schema)?);
                let partitioning = parse_protobuf_partitioning(
                    unresolved_shuffle.partitioning.as_ref(),
                    registry,
                    schema.as_ref(),
                )?
                .expect("partitioning is required");
                Ok(Arc::new(UnresolvedShuffleExec {
                    stage_id: unresolved_shuffle.stage_id as usize,
                    schema,
                    partitioning,
                }))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            // note that we use shuffle_output_partitioning() rather than output_partitioning()
            // to get the true output partitioning
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Some(Partitioning::Hash(exprs, partition_count)) => {
                    Some(datafusion_proto::protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                None => None,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "physical_plan::to_proto() invalid partitioning for ShuffleWriterExec: {other:?}"
                    )));
                }
            };

            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(
                    protobuf::ShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: None,
                        output_partitioning,
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle writer execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<ShuffleReaderExec>() {
            let stage_id = exec.stage_id as u32;
            let mut partition = vec![];
            for location in &exec.partition {
                partition.push(protobuf::ShuffleReaderPartition {
                    location: location
                        .iter()
                        .map(|l| {
                            l.clone().try_into().map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Fail to get partition location due to {e:?}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
            let partitioning = serialize_partitioning(&exec.partitioning)?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        stage_id,
                        partition,
                        schema: Some(exec.schema.as_ref().try_into()?),
                        partitioning: Some(partitioning),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle reader execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            let partitioning = serialize_partitioning(&exec.partitioning)?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::UnresolvedShuffle(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema.as_ref().try_into()?),
                        partitioning: Some(partitioning),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode unresolved shuffle execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }
    }
}

pub fn serialize_partitioning(
    partitioning: &Partitioning,
) -> Result<protobuf::Partitioning, DataFusionError> {
    let serialized_partitioning = match partitioning {
        Partitioning::RoundRobinBatch(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::RoundRobin(
                *partition_count as u64,
            )),
        },
        Partitioning::Hash(exprs, partition_count) => {
            let serialized_exprs = exprs
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<PhysicalExprNode>, DataFusionError>>()?;
            protobuf::Partitioning {
                partition_method: Some(protobuf::partitioning::PartitionMethod::Hash(
                    datafusion_proto::protobuf::PhysicalHashRepartition {
                        hash_expr: serialized_exprs,
                        partition_count: *partition_count as u64,
                    },
                )),
            }
        }
        Partitioning::UnknownPartitioning(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::Unknown(
                *partition_count as u64,
            )),
        },
    };
    Ok(serialized_partitioning)
}

pub fn parse_protobuf_partitioning(
    partitioning: Option<&protobuf::Partitioning>,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Option<Partitioning>, DataFusionError> {
    match partitioning {
        Some(protobuf::Partitioning { partition_method }) => match partition_method {
            Some(protobuf::partitioning::PartitionMethod::RoundRobin(partition_count)) => Ok(Some(
                Partitioning::RoundRobinBatch(*partition_count as usize),
            )),
            Some(protobuf::partitioning::PartitionMethod::Hash(hash_repartition)) => {
                parse_protobuf_hash_partitioning(Some(hash_repartition), registry, input_schema)
            }
            Some(protobuf::partitioning::PartitionMethod::Unknown(partition_count)) => Ok(Some(
                Partitioning::UnknownPartitioning(*partition_count as usize),
            )),
            None => Ok(None),
        },
        None => Ok(None),
    }
}
