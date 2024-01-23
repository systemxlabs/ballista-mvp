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

//! Ballista Rust executor binary.

use anyhow::Result;
use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ExecutorProcessConfig {
        special_mod_log_level: "INFO,datafusion=INFO".to_string(),
        bind_host: "0.0.0.0".to_string(),
        port: std::env::var("BIND_PORT")
            .unwrap_or("50051".to_string())
            .parse::<u16>()
            .unwrap(),
        grpc_port: std::env::var("BIND_GRPC_PORT")
            .unwrap_or("50052".to_string())
            .parse::<u16>()
            .unwrap(),
        scheduler_host: "localhost".to_string(),
        scheduler_port: 50050,
        concurrent_tasks: 0, // defaults to all available cores
        work_dir: None,
        grpc_server_max_decoding_message_size: 16777216, // 16MB
        grpc_server_max_encoding_message_size: 16777216, // 16MB
        executor_heartbeat_interval_seconds: 60,
    };

    start_executor_process(Arc::new(config)).await
}
