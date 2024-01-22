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

//! Ballista Rust scheduler binary.

use std::sync::Arc;
use std::{env, io};

use anyhow::Result;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{ClusterStorageConfig, SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let bind_port = env::var("BIND_PORT")
        .unwrap_or("50050".to_string())
        .parse::<u16>()
        .unwrap();

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or("INFO,datafusion=INFO".to_string()));
    // Console layer
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_writer(io::stdout)
        .with_env_filter(log_filter)
        .init();

    let addr = format!("{}:{}", "0.0.0.0", bind_port);
    let addr = addr.parse()?;

    let cluster_backend = env::var("CLUSTER_STORAGE").unwrap_or("sled".to_string());
    let cluster_storage_config = match cluster_backend.as_str() {
        "memory" => ClusterStorageConfig::Memory,
        "etcd" => ClusterStorageConfig::Etcd(
            "localhost:2379"
                .to_string()
                .split_whitespace()
                .map(|s| s.to_string())
                .collect(),
        ),
        "sled" => ClusterStorageConfig::Sled(None),
        _ => unimplemented!(),
    };

    let config = SchedulerConfig {
        namespace: "ballista".to_string(),
        external_host: "localhost".to_string(),
        bind_port,
        event_loop_buffer_size: 10000,
        task_distribution: TaskDistributionPolicy::Bias,
        finished_job_data_clean_up_interval_seconds: 300,
        finished_job_state_clean_up_interval_seconds: 3600,
        advertise_flight_sql_endpoint: None,
        cluster_storage: cluster_storage_config,
        job_resubmit_interval_ms: None,        // not resubmit
        executor_termination_grace_period: 30, // seconds
        scheduler_event_expected_processing_duration: 0, // disable
        grpc_server_max_decoding_message_size: 16777216, // 16MB
        grpc_server_max_encoding_message_size: 16777216, // 16MB
        executor_timeout_seconds: 180,
        expire_dead_executor_interval_seconds: 15,
    };

    let cluster = BallistaCluster::new_from_config(&config).await?;

    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}
