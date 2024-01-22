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

//! Distributed execution context.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::DataFilePaths;
use log::info;
use parking_lot::Mutex;
use sqlparser::ast::Statement;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{CreateSessionParams, KeyValuePair};
use ballista_core::utils::{
    create_df_ctx_with_ballista_query_planner, create_grpc_client_connection,
};
use datafusion_proto::protobuf::LogicalPlanNode;

use datafusion::catalog::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    CreateExternalTable, DdlStatement, LogicalPlan, TableScan,
};
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, ParquetReadOptions,
    SessionConfig, SessionContext,
};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};

struct BallistaContextState {
    /// Ballista configuration
    config: BallistaConfig,
    /// Scheduler host
    scheduler_host: String,
    /// Scheduler port
    scheduler_port: u16,
    /// Tables that have been registered with this context
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl BallistaContextState {
    pub fn new(
        scheduler_host: String,
        scheduler_port: u16,
        config: &BallistaConfig,
    ) -> Self {
        Self {
            config: config.clone(),
            scheduler_host,
            scheduler_port,
            tables: HashMap::new(),
        }
    }

    pub fn config(&self) -> &BallistaConfig {
        &self.config
    }
}

pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
    context: Arc<SessionContext>,
}

impl BallistaContext {
    /// Create a context for executing queries against a remote Ballista scheduler instance
    pub async fn remote(
        host: &str,
        port: u16,
        config: &BallistaConfig,
    ) -> ballista_core::error::Result<Self> {
        let state = BallistaContextState::new(host.to_owned(), port, config);

        let scheduler_url =
            format!("http://{}:{}", &state.scheduler_host, state.scheduler_port);
        info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );
        let connection = create_grpc_client_connection(scheduler_url.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        let mut scheduler = SchedulerGrpcClient::new(connection)
            .max_encoding_message_size(16 * 1024 * 1024)
            .max_decoding_message_size(16 * 1024 * 1024);

        let remote_session_id = scheduler
            .create_session(CreateSessionParams {
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner()
            .session_id;

        info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        let ctx = {
            create_df_ctx_with_ballista_query_planner::<LogicalPlanNode>(
                scheduler_url,
                remote_session_id,
                state.config(),
            )
        };

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            context: Arc::new(ctx),
        })
    }

    /// Create a DataFrame representing an Avro table scan
    pub async fn read_avro<P: DataFilePaths>(
        &self,
        paths: P,
        options: AvroReadOptions<'_>,
    ) -> Result<DataFrame> {
        let df = self.context.read_avro(paths, options).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a Parquet table scan
    pub async fn read_parquet<P: DataFilePaths>(
        &self,
        paths: P,
        options: ParquetReadOptions<'_>,
    ) -> Result<DataFrame> {
        let df = self.context.read_parquet(paths, options).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a CSV table scan
    pub async fn read_csv<P: DataFilePaths>(
        &self,
        paths: P,
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        let df = self.context.read_csv(paths, options).await?;
        Ok(df)
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        let mut state = self.state.lock();
        state.tables.insert(name.to_owned(), table);
        Ok(())
    }

    pub async fn register_csv(
        &self,
        name: &str,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let plan = self
            .read_csv(path, options)
            .await
            .map_err(|e| {
                DataFusionError::Context(format!("Can't read CSV: {path}"), Box::new(e))
            })?
            .into_optimized_plan()?;
        match plan {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source_as_provider(&source)?)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    pub async fn register_parquet(
        &self,
        name: &str,
        path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        match self
            .read_parquet(path, options)
            .await?
            .into_optimized_plan()?
        {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source_as_provider(&source)?)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    pub async fn register_avro(
        &self,
        name: &str,
        path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        match self.read_avro(path, options).await?.into_optimized_plan()? {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source_as_provider(&source)?)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    /// is a 'show *' sql
    pub async fn is_show_statement(&self, sql: &str) -> Result<bool> {
        let mut is_show_variable: bool = false;
        let statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        if let DFStatement::Statement(st) = &statements[0] {
            match **st {
                Statement::ShowVariable { .. } => {
                    is_show_variable = true;
                }
                Statement::ShowColumns { .. } => {
                    is_show_variable = true;
                }
                Statement::ShowTables { .. } => {
                    is_show_variable = true;
                }
                _ => {
                    is_show_variable = false;
                }
            }
        };

        Ok(is_show_variable)
    }

    /// Create a DataFrame from a SQL statement.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let mut ctx = self.context.clone();

        let is_show = self.is_show_statement(sql).await?;
        // the show tables、 show columns sql can not run at scheduler because the tables is store at client
        if is_show {
            let state = self.state.lock();
            ctx = Arc::new(SessionContext::new_with_config(
                SessionConfig::new().with_information_schema(
                    state.config.default_with_information_schema(),
                ),
            ));
        }

        // register tables with DataFusion context
        {
            let state = self.state.lock();
            for (name, prov) in &state.tables {
                // ctx is shared between queries, check table exists or not before register
                let table_ref = TableReference::Bare {
                    table: Cow::Borrowed(name),
                };
                if !ctx.table_exist(table_ref)? {
                    ctx.register_table(
                        TableReference::Bare {
                            table: Cow::Borrowed(name),
                        },
                        Arc::clone(prov),
                    )?;
                }
            }
        }

        let plan = ctx.state().create_logical_plan(sql).await?;

        match plan {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
                CreateExternalTable {
                    ref schema,
                    ref name,
                    ref location,
                    ref file_type,
                    ref has_header,
                    ref delimiter,
                    ref table_partition_cols,
                    ref if_not_exists,
                    ..
                },
            )) => {
                let table_exists = ctx.table_exist(name)?;
                let schema: SchemaRef = Arc::new(schema.as_ref().to_owned().into());
                let table_partition_cols = table_partition_cols
                    .iter()
                    .map(|col| {
                        schema
                            .field_with_name(col)
                            .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                            .map_err(DataFusionError::ArrowError)
                    })
                    .collect::<Result<Vec<_>>>()?;

                match (if_not_exists, table_exists) {
                    (_, false) => match file_type.to_lowercase().as_str() {
                        "csv" => {
                            let mut options = CsvReadOptions::new()
                                .has_header(*has_header)
                                .delimiter(*delimiter as u8)
                                .table_partition_cols(table_partition_cols.to_vec());
                            if !schema.fields().is_empty() {
                                options = options.schema(&schema);
                            }
                            self.register_csv(name.table(), location, options).await?;
                            Ok(DataFrame::new(ctx.state(), plan))
                        }
                        "parquet" => {
                            self.register_parquet(
                                name.table(),
                                location,
                                ParquetReadOptions::default()
                                    .table_partition_cols(table_partition_cols),
                            )
                            .await?;
                            Ok(DataFrame::new(ctx.state(), plan))
                        }
                        "avro" => {
                            self.register_avro(
                                name.table(),
                                location,
                                AvroReadOptions::default()
                                    .table_partition_cols(table_partition_cols),
                            )
                            .await?;
                            Ok(DataFrame::new(ctx.state(), plan))
                        }
                        _ => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported file type {file_type:?}."
                        ))),
                    },
                    (true, true) => Ok(DataFrame::new(ctx.state(), plan)),
                    (false, true) => Err(DataFusionError::Execution(format!(
                        "Table '{name:?}' already exists"
                    ))),
                }
            }
            _ => ctx.execute_logical_plan(plan).await,
        }
    }
}
