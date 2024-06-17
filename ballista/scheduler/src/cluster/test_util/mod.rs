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

use crate::cluster::JobState;
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::ExecutionGraph;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::JobStatus;
use std::sync::Arc;

pub struct JobStateTest<S: JobState> {
    state: Arc<S>,
}

impl<S: JobState> JobStateTest<S> {
    pub async fn new(state: S) -> Result<Self> {
        Ok(Self {
            state: Arc::new(state),
        })
    }

    pub fn queue_job(self, job_id: &str) -> Result<Self> {
        self.state.accept_job(job_id, timestamp_millis())?;
        Ok(self)
    }

    pub async fn fail_planning(self, job_id: &str) -> Result<Self> {
        self.state
            .fail_unscheduled_job(job_id, "failed planning".to_string())
            .await?;
        Ok(self)
    }

    pub async fn assert_queued(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Queued job {} not found", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Queued(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected queued status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn submit_job(self, graph: &ExecutionGraph) -> Result<Self> {
        self.state
            .submit_job(graph.job_id().to_string(), graph)
            .await?;
        Ok(self)
    }

    pub async fn assert_job_running(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Running(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected running status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn update_job(self, graph: &ExecutionGraph) -> Result<Self> {
        self.state.save_job(graph.job_id(), graph).await?;
        Ok(self)
    }

    pub async fn assert_job_failed(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Failed(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected failed status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn assert_job_successful(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);
        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Successful(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected success status but found {:?}",
            status
        );

        Ok(self)
    }
}

pub async fn test_job_planning_failure<S: JobState>(state: S, graph: ExecutionGraph) -> Result<()> {
    let test = JobStateTest::new(state).await?;

    let job_id = graph.job_id().to_string();

    test.queue_job(&job_id)?
        .fail_planning(&job_id)
        .await?
        .assert_job_failed(&job_id)
        .await?;

    Ok(())
}
