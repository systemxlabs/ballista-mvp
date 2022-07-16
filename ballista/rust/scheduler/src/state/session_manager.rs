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

use crate::scheduler_server::SessionBuilder;
use crate::state::backend::{Keyspace, StateBackendClient};
use crate::state::{decode_protobuf, encode_protobuf};
use ballista_core::config::BallistaConfig;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::{self, KeyValuePair};
use datafusion::prelude::{SessionConfig, SessionContext};

use std::sync::Arc;

#[derive(Clone)]
pub struct SessionManager {
    state: Arc<dyn StateBackendClient>,
    session_builder: SessionBuilder,
}

impl SessionManager {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        session_builder: SessionBuilder,
    ) -> Self {
        Self {
            state,
            session_builder,
        }
    }

    pub async fn update_session(
        &self,
        session_id: &str,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        let mut settings: Vec<KeyValuePair> = vec![];

        for (key, value) in config.settings() {
            settings.push(KeyValuePair {
                key: key.clone(),
                value: value.clone(),
            })
        }

        let value = encode_protobuf(&protobuf::SessionSettings { configs: settings })?;
        self.state
            .put(Keyspace::Sessions, session_id.to_owned(), value)
            .await?;

        Ok(create_datafusion_context(config, self.session_builder))
    }

    pub async fn create_session(
        &self,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        let mut settings: Vec<KeyValuePair> = vec![];

        for (key, value) in config.settings() {
            settings.push(KeyValuePair {
                key: key.clone(),
                value: value.clone(),
            })
        }

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        let ctx = create_datafusion_context(&config, self.session_builder);

        let value = encode_protobuf(&protobuf::SessionSettings { configs: settings })?;
        self.state
            .put(Keyspace::Sessions, ctx.session_id(), value)
            .await?;

        Ok(ctx)
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let value = self.state.get(Keyspace::Sessions, session_id).await?;

        let settings: protobuf::SessionSettings = decode_protobuf(&value)?;

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings.configs {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        Ok(create_datafusion_context(&config, self.session_builder))
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    config: &BallistaConfig,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_batch_size(config.default_batch_size())
        .with_repartition_joins(config.repartition_joins())
        .with_repartition_aggregations(config.repartition_aggregations())
        .with_repartition_windows(config.repartition_windows())
        .with_parquet_pruning(config.parquet_pruning());
    let session_state = session_builder(config);
    Arc::new(SessionContext::with_state(session_state))
}

/// Update the existing DataFusion session context with Ballista Configuration
pub fn update_datafusion_context(
    session_ctx: Arc<SessionContext>,
    config: &BallistaConfig,
) -> Arc<SessionContext> {
    {
        let mut mut_state = session_ctx.state.write();
        // TODO Currently we have to start from default session config due to the interface not support update
        mut_state.config = SessionConfig::default()
            .with_target_partitions(config.default_shuffle_partitions())
            .with_batch_size(config.default_batch_size())
            .with_repartition_joins(config.repartition_joins())
            .with_repartition_aggregations(config.repartition_aggregations())
            .with_repartition_windows(config.repartition_windows())
            .with_parquet_pruning(config.parquet_pruning());
    }
    session_ctx
}
