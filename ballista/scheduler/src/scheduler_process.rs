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

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::future::{self, Either, TryFutureExt};
use hyper::{server::conn::AddrStream, service::make_service_fn, Server};
use log::info;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::server::Connected;
use tower::Service;

use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::create_grpc_server;
use ballista_core::BALLISTA_VERSION;

use crate::api::{get_routes, EitherBody, Error};
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::flight_sql::FlightSqlServiceImpl;
use crate::scheduler_server::SchedulerServer;
use crate::state::task_manager::DefaultTaskLauncher;

pub async fn start_server(
    cluster: BallistaCluster,
    addr: SocketAddr,
    config: Arc<SchedulerConfig>,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    // Should only call SchedulerServer::new() once in the process
    info!("Starting Scheduler grpc server with push task scheduling policy",);

    let mut scheduler_server: SchedulerServer = SchedulerServer::new(
        config.scheduler_name(),
        cluster,
        BallistaCodec::default(),
        config.clone(),
        Arc::new(DefaultTaskLauncher::new(config.scheduler_name())),
    );

    scheduler_server.init().await?;

    Server::bind(&addr)
        .serve(make_service_fn(move |request: &AddrStream| {
            let config = &scheduler_server.state.config;
            let scheduler_grpc_server = SchedulerGrpcServer::new(scheduler_server.clone())
                .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
                .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);

            let tonic_builder = create_grpc_server().add_service(scheduler_grpc_server);

            let tonic_builder = tonic_builder.add_service(FlightServiceServer::new(
                FlightSqlServiceImpl::new(scheduler_server.clone()),
            ));

            let mut tonic = tonic_builder.into_service();

            let mut warp = warp::service(get_routes(scheduler_server.clone()));

            let connect_info = request.connect_info();
            future::ok::<_, Infallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    // Set the connect info from hyper to tonic
                    let (mut parts, body) = req.into_parts();
                    parts.extensions.insert(connect_info.clone());
                    let req = http::Request::from_parts(parts, body);

                    if req.uri().path().starts_with("/api") {
                        return Either::Left(
                            warp.call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(Error::from),
                        );
                    }

                    Either::Right(
                        tonic
                            .call(req)
                            .map_ok(|res| res.map(EitherBody::Right))
                            .map_err(Error::from),
                    )
                },
            ))
        }))
        .await
        .context("Could not start grpc server")
}
