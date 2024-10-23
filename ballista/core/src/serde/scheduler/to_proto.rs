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

use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use std::convert::TryInto;

use crate::error::BallistaError;

use crate::serde::protobuf;

use crate::serde::scheduler::{
    Action, ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation, PartitionStats,
};
use protobuf::{action::ActionType, operator_metric, NamedCount, NamedGauge, NamedTime};

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::FetchPartition {
                job_id,
                stage_id,
                partition_id,
                path,
                host,
                port,
            } => Ok(protobuf::Action {
                action_type: Some(ActionType::FetchPartition(protobuf::FetchPartition {
                    job_id,
                    stage_id: stage_id as u32,
                    partition_id: partition_id as u32,
                    path,
                    host,
                    port: port as u32,
                })),
                settings: vec![],
            }),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionId> for PartitionId {
    fn into(self) -> protobuf::PartitionId {
        protobuf::PartitionId {
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
        }
    }
}

impl TryInto<protobuf::PartitionLocation> for PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PartitionLocation, Self::Error> {
        Ok(protobuf::PartitionLocation {
            partition_id: Some(self.partition_id.into()),
            executor_meta: Some(self.executor_meta.into()),
            partition_stats: Some(self.partition_stats.into()),
            path: self.path,
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionStats> for PartitionStats {
    fn into(self) -> protobuf::PartitionStats {
        let none_value = -1_i64;
        protobuf::PartitionStats {
            num_rows: self.num_rows.map(|n| n as i64).unwrap_or(none_value),
            num_batches: self.num_batches.map(|n| n as i64).unwrap_or(none_value),
            num_bytes: self.num_bytes.map(|n| n as i64).unwrap_or(none_value),
            column_stats: vec![],
        }
    }
}

impl TryInto<protobuf::OperatorMetric> for &MetricValue {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::OperatorMetric, Self::Error> {
        match self {
            MetricValue::OutputRows(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::OutputRows(count.value() as u64)),
            }),
            MetricValue::ElapsedCompute(time) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::ElapseTime(time.value() as u64)),
            }),
            MetricValue::SpillCount(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::SpillCount(count.value() as u64)),
            }),
            MetricValue::SpilledBytes(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::SpilledBytes(count.value() as u64)),
            }),
            MetricValue::CurrentMemoryUsage(gauge) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::CurrentMemoryUsage(
                    gauge.value() as u64
                )),
            }),
            MetricValue::Count { name, count } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Count(NamedCount {
                    name: name.to_string(),
                    value: count.value() as u64,
                })),
            }),
            MetricValue::Gauge { name, gauge } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Gauge(NamedGauge {
                    name: name.to_string(),
                    value: gauge.value() as u64,
                })),
            }),
            MetricValue::Time { name, time } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Time(NamedTime {
                    name: name.to_string(),
                    value: time.value() as u64,
                })),
            }),
            MetricValue::StartTimestamp(timestamp) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::StartTimestamp(
                    timestamp
                        .value()
                        .and_then(|m| m.timestamp_nanos_opt())
                        .unwrap_or(0),
                )),
            }),
            MetricValue::EndTimestamp(timestamp) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::EndTimestamp(
                    timestamp
                        .value()
                        .and_then(|m| m.timestamp_nanos_opt())
                        .unwrap_or(0),
                )),
            }),
        }
    }
}

impl TryInto<protobuf::OperatorMetricsSet> for MetricsSet {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::OperatorMetricsSet, Self::Error> {
        let metrics = self
            .iter()
            .map(|m| m.value().try_into())
            .collect::<Result<Vec<_>, BallistaError>>()?;
        Ok(protobuf::OperatorMetricsSet { metrics })
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorMetadata> for ExecutorMetadata {
    fn into(self) -> protobuf::ExecutorMetadata {
        protobuf::ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u32,
            grpc_port: self.grpc_port as u32,
            specification: Some(self.specification.into()),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorSpecification> for ExecutorSpecification {
    fn into(self) -> protobuf::ExecutorSpecification {
        protobuf::ExecutorSpecification {
            resources: vec![protobuf::executor_resource::Resource::TaskSlots(
                self.task_slots,
            )]
            .into_iter()
            .map(|r| protobuf::ExecutorResource { resource: Some(r) })
            .collect(),
        }
    }
}
