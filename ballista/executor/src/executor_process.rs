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

//! Ballista Executor Process

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{env, io};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use tempfile::TempDir;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::executor_status::Status;
use ballista_core::serde::protobuf::{
    scheduler_grpc_client::SchedulerGrpcClient, ExecutorRegistration, ExecutorResource,
    ExecutorSpecification, ExecutorStatus, HeartBeatParams,
};
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{create_grpc_client_connection, create_grpc_server};
use ballista_core::BALLISTA_VERSION;

use crate::executor::{Executor, TasksDrainedFuture};
use crate::executor_server;
use crate::executor_server::TERMINATING;
use crate::flight_service::BallistaFlightService;
use crate::shutdown::Shutdown;
use crate::shutdown::ShutdownNotifier;

pub struct ExecutorProcessConfig {
    pub bind_host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub scheduler_host: String,
    pub scheduler_port: u16,
    pub concurrent_tasks: usize,
    pub work_dir: Option<String>,
    pub special_mod_log_level: String,
    /// The maximum size of a decoded message at the grpc server side.
    pub grpc_server_max_decoding_message_size: u32,
    /// The maximum size of an encoded message at the grpc server side.
    pub grpc_server_max_encoding_message_size: u32,
    pub executor_heartbeat_interval_seconds: u64,
}

pub async fn start_executor_process(opt: Arc<ExecutorProcessConfig>) -> Result<()> {
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(opt.special_mod_log_level.clone()));
    // Console layer
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_writer(io::stdout)
        .with_env_filter(log_filter)
        .init();

    let addr = format!("{}:{}", opt.bind_host, opt.port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {addr}"))?;

    let scheduler_host = opt.scheduler_host.clone();
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{scheduler_host}:{scheduler_port}");

    let work_dir = opt.work_dir.clone().unwrap_or(
        TempDir::new()?
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap(),
    );

    let concurrent_tasks = if opt.concurrent_tasks == 0 {
        // use all available cores if no concurrency level is specified
        num_cpus::get()
    } else {
        opt.concurrent_tasks
    };

    info!("Running with config:");
    info!("work_dir: {}", work_dir);
    info!("concurrent_tasks: {}", concurrent_tasks);

    // assign this executor an unique ID
    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        port: opt.port as u32,
        grpc_port: opt.grpc_port as u32,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
            }],
        }),
    };

    let config = RuntimeConfig::new().with_temp_file_path(work_dir.clone());
    let runtime = {
        Arc::new(RuntimeEnv::new(config).map_err(|_| {
            BallistaError::Internal("Failed to init Executor RuntimeEnv".to_owned())
        })?)
    };

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime,
        concurrent_tasks,
    ));

    let connection = create_grpc_client_connection(scheduler_url)
        .await
        .context("Could not connect to scheduler")?;

    let mut scheduler = SchedulerGrpcClient::new(connection)
        .max_encoding_message_size(16 * 1024 * 1024)
        .max_decoding_message_size(16 * 1024 * 1024);

    let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::default();

    // Graceful shutdown notification
    let shutdown_noti = ShutdownNotifier::new();

    let mut service_handlers: FuturesUnordered<JoinHandle<Result<(), BallistaError>>> =
        FuturesUnordered::new();

    // Channels used to receive stop requests from Executor grpc service.
    let (stop_send, mut stop_recv) = mpsc::channel::<bool>(10);

    service_handlers.push(
        //If there is executor registration error during startup, return the error and stop early.
        executor_server::startup(
            scheduler.clone(),
            opt.clone(),
            executor.clone(),
            default_codec,
            stop_send,
            &shutdown_noti,
        )
        .await?,
    );
    service_handlers.push(tokio::spawn(flight_server_run(
        addr,
        shutdown_noti.subscribe_for_shutdown(),
    )));

    let tasks_drained = TasksDrainedFuture(executor);

    // Concurrently run the service checking and listen for the `shutdown` signal and wait for the stop request coming.
    // The check_services runs until an error is encountered, so under normal circumstances, this `select!` statement runs
    // until the `shutdown` signal is received or a stop request is coming.
    let (notify_scheduler, _stop_reason) = tokio::select! {
        service_val = check_services(&mut service_handlers) => {
            let msg = format!("executor services stopped with reason {service_val:?}");
            info!("{:?}", msg);
            (true, msg)
        },
        _ = signal::ctrl_c() => {
            let msg = "executor received ctrl-c event.".to_string();
             info!("{:?}", msg);
            (true, msg)
        },
        _ = stop_recv.recv() => {
            (false, "".to_string())
        },
    };

    // Set status to fenced
    info!("setting executor to TERMINATING status");
    TERMINATING.store(true, Ordering::Release);

    if notify_scheduler {
        // Send a heartbeat to update status of executor to `Fenced`. This should signal to the
        // scheduler to no longer schedule tasks on this executor
        if let Err(error) = scheduler
            .heart_beat_from_executor(HeartBeatParams {
                executor_id: executor_id.clone(),
                status: Some(ExecutorStatus {
                    status: Some(Status::Terminating(String::default())),
                }),
                metadata: Some(ExecutorRegistration {
                    id: executor_id.clone(),
                    port: opt.port as u32,
                    grpc_port: opt.grpc_port as u32,
                    specification: Some(ExecutorSpecification {
                        resources: vec![ExecutorResource {
                            resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
                        }],
                    }),
                }),
            })
            .await
        {
            error!("error sending heartbeat with fenced status: {:?}", error);
        }

        // Wait for tasks to drain
        tasks_drained.await;
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let ShutdownNotifier {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = shutdown_noti;

    // When `notify_shutdown` is dropped, all components which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all related components to finish the shutdown processing.
    let _ = shutdown_complete_rx.recv().await;
    info!("Executor stopped.");
    Ok(())
}

// Arrow flight service
async fn flight_server_run(
    addr: SocketAddr,
    mut grpc_shutdown: Shutdown,
) -> Result<(), BallistaError> {
    let service = BallistaFlightService::new();
    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor Flight Server listening on {:?}",
        BALLISTA_VERSION, addr
    );

    let shutdown_signal = grpc_shutdown.recv();
    let server_future = create_grpc_server()
        .add_service(server)
        .serve_with_shutdown(addr, shutdown_signal);

    server_future.await.map_err(|e| {
        error!("Tonic error, Could not start Executor Flight Server.");
        BallistaError::TonicError(e)
    })
}

// Check the status of long running services
async fn check_services(
    service_handlers: &mut FuturesUnordered<JoinHandle<Result<(), BallistaError>>>,
) -> Result<(), BallistaError> {
    loop {
        match service_handlers.next().await {
            Some(result) => match result {
                // React to "inner_result", i.e. propagate as BallistaError
                Ok(inner_result) => match inner_result {
                    Ok(()) => (),
                    Err(e) => return Err(e),
                },
                // React to JoinError
                Err(e) => return Err(BallistaError::TokioError(e)),
            },
            None => {
                info!("service handlers are all done with their work!");
                return Ok(());
            }
        }
    }
}
