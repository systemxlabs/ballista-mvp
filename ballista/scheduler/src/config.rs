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
//

//! Ballista scheduler specific configuration

/// Configurations for the ballista scheduler of scheduling jobs and tasks
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Namespace of this scheduler. Schedulers using the same cluster storage and namespace
    /// will share global cluster state.
    pub namespace: String,
    /// The external hostname of the scheduler
    pub external_host: String,
    /// The bind port for the scheduler's gRPC service
    pub bind_port: u16,
    /// The event loop buffer size. for a system of high throughput, a larger value like 1000000 is recommended
    pub event_loop_buffer_size: u32,
    /// Policy of distributing tasks to available executor slots. For a cluster with single scheduler, round-robin is recommended
    pub task_distribution: TaskDistributionPolicy,
    /// The delayed interval for cleaning up finished job data, mainly the shuffle data, 0 means the cleaning up is disabled
    pub finished_job_data_clean_up_interval_seconds: u64,
    /// The delayed interval for cleaning up finished job state stored in the backend, 0 means the cleaning up is disabled.
    pub finished_job_state_clean_up_interval_seconds: u64,
    /// Configuration for ballista cluster storage
    pub cluster_storage: ClusterStorageConfig,
    /// Time in seconds to allow executor for graceful shutdown. Once an executor signals it has entered Terminating status
    /// the scheduler should only consider the executor dead after this time interval has elapsed
    pub executor_termination_grace_period: u64,
    /// The maximum size of a decoded message at the grpc server side.
    pub grpc_server_max_decoding_message_size: u32,
    /// The maximum size of an encoded message at the grpc server side.
    pub grpc_server_max_encoding_message_size: u32,
    /// The executor timeout in seconds. It should be longer than executor's heartbeat intervals.
    pub executor_timeout_seconds: u64,
    /// The interval to check expired or dead executors
    pub expire_dead_executor_interval_seconds: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            namespace: String::default(),
            external_host: "localhost".to_string(),
            bind_port: 50050,
            event_loop_buffer_size: 10000,
            task_distribution: TaskDistributionPolicy::Bias,
            finished_job_data_clean_up_interval_seconds: 300,
            finished_job_state_clean_up_interval_seconds: 3600,
            cluster_storage: ClusterStorageConfig::Sled(None),
            executor_termination_grace_period: 0,
            grpc_server_max_decoding_message_size: 16777216,
            grpc_server_max_encoding_message_size: 16777216,
            executor_timeout_seconds: 180,
            expire_dead_executor_interval_seconds: 15,
        }
    }
}

impl SchedulerConfig {
    pub fn scheduler_name(&self) -> String {
        format!("{}:{}", self.external_host, self.bind_port)
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.external_host = hostname.into();
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.bind_port = port;
        self
    }

    pub fn with_event_loop_buffer_size(mut self, buffer_size: u32) -> Self {
        self.event_loop_buffer_size = buffer_size;
        self
    }

    pub fn with_finished_job_data_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_data_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_finished_job_state_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_state_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_task_distribution(mut self, policy: TaskDistributionPolicy) -> Self {
        self.task_distribution = policy;
        self
    }

    pub fn with_cluster_storage(mut self, config: ClusterStorageConfig) -> Self {
        self.cluster_storage = config;
        self
    }

    pub fn with_remove_executor_wait_secs(mut self, value: u64) -> Self {
        self.executor_termination_grace_period = value;
        self
    }

    pub fn with_grpc_server_max_decoding_message_size(mut self, value: u32) -> Self {
        self.grpc_server_max_decoding_message_size = value;
        self
    }

    pub fn with_grpc_server_max_encoding_message_size(mut self, value: u32) -> Self {
        self.grpc_server_max_encoding_message_size = value;
        self
    }
}

#[derive(Clone, Debug)]
pub enum ClusterStorageConfig {
    Etcd(Vec<String>),
    Sled(Option<String>),
}

#[derive(Debug, Clone, Copy)]
pub enum TaskDistributionPolicy {
    /// Eagerly assign tasks to executor slots. This will assign as many task slots per executor
    /// as are currently available
    Bias,
    /// Distribute tasks evenly across executors. This will try and iterate through available executors
    /// and assign one task to each executor until all tasks are assigned.
    RoundRobin,
}
