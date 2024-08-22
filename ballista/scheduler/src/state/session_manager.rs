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
use ballista_core::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::HashMap;

use crate::cluster::JobState;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionManager {
    state: Arc<dyn JobState>,
}

impl SessionManager {
    pub fn new(state: Arc<dyn JobState>) -> Self {
        Self { state }
    }

    pub async fn create_session(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Arc<SessionContext>> {
        self.state.create_session(config).await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.state.get_session(session_id).await
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    ballista_config: &HashMap<String, String>,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let config = SessionConfig::from_string_hash_map(ballista_config.clone()).unwrap();
    let config = config.set_bool("datafusion.optimizer.enable_round_robin_repartition", false);
    let session_state = session_builder(config);
    Arc::new(SessionContext::new_with_state(session_state))
}
