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

use std::collections::{HashMap, HashSet};
use std::{sync::Arc, task::Poll};

use ballista_core::error::{ballista_error, BallistaError, Result};

use crate::cluster::storage::KeyValueStore;
use async_trait::async_trait;
use futures::{FutureExt, Stream};
use log::warn;
use tokio::sync::Mutex;

use crate::cluster::storage::{Keyspace, Lock, Operation, Watch, WatchEvent};

/// A [`StateBackendClient`] implementation that uses file-based storage to save cluster state.
#[derive(Clone)]
pub struct SledClient {
    db: sled::Db,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl SledClient {
    /// Creates a SledClient that saves data to the specified file.
    pub fn try_new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Ok(Self {
            db: sled::open(path).map_err(sled_to_ballista_error)?,
            locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Creates a SledClient that saves data to a temp file.
    pub fn try_new_temporary() -> Result<Self> {
        Ok(Self {
            db: sled::Config::new()
                .temporary(true)
                .open()
                .map_err(sled_to_ballista_error)?,
            locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

fn sled_to_ballista_error(e: sled::Error) -> BallistaError {
    match e {
        sled::Error::Io(io) => BallistaError::IoError(io),
        _ => BallistaError::General(format!("{e}")),
    }
}

#[async_trait]
impl KeyValueStore for SledClient {
    async fn get(&self, keyspace: Keyspace, key: &str) -> Result<Vec<u8>> {
        let key = format!("/{keyspace:?}/{key}");
        Ok(self
            .db
            .get(key)
            .map_err(|e| ballista_error(&format!("sled error {e:?}")))?
            .map(|v| v.to_vec())
            .unwrap_or_default())
    }

    async fn scan(
        &self,
        keyspace: Keyspace,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("/{keyspace:?}/");
        if let Some(limit) = limit {
            Ok(self
                .db
                .scan_prefix(prefix)
                .take(limit)
                .map(|v| {
                    v.map(|(key, value)| {
                        (
                            std::str::from_utf8(&key).unwrap().to_owned(),
                            value.to_vec(),
                        )
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| ballista_error(&format!("sled error {e:?}")))?)
        } else {
            Ok(self
                .db
                .scan_prefix(prefix)
                .map(|v| {
                    v.map(|(key, value)| {
                        (
                            std::str::from_utf8(&key).unwrap().to_owned(),
                            value.to_vec(),
                        )
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| ballista_error(&format!("sled error {e:?}")))?)
        }
    }

    async fn scan_keys(&self, keyspace: Keyspace) -> Result<HashSet<String>> {
        let prefix = format!("/{keyspace:?}/");
        Ok(self
            .db
            .scan_prefix(prefix.clone())
            .map(|v| {
                v.map(|(key, _value)| {
                    std::str::from_utf8(&key)
                        .unwrap()
                        .strip_prefix(&prefix)
                        .unwrap()
                        .to_owned()
                })
            })
            .collect::<std::result::Result<HashSet<_>, _>>()
            .map_err(|e| ballista_error(&format!("sled error {e:?}")))?)
    }

    async fn put(&self, keyspace: Keyspace, key: String, value: Vec<u8>) -> Result<()> {
        let key = format!("/{keyspace:?}/{key}");
        self.db
            .insert(key, value)
            .map_err(|e| {
                warn!("sled insert failed: {}", e);
                ballista_error("sled insert failed")
            })
            .map(|_| ())
    }

    async fn apply_txn(&self, ops: Vec<(Operation, Keyspace, String)>) -> Result<()> {
        let mut batch = sled::Batch::default();

        for (op, keyspace, key_str) in ops {
            let key = format!("/{:?}/{}", &keyspace, key_str);
            match op {
                Operation::Put(value) => batch.insert(key.as_str(), value),
                Operation::Delete => batch.remove(key.as_str()),
            }
        }

        self.db.apply_batch(batch).map_err(|e| {
            warn!("sled transaction insert failed: {}", e);
            ballista_error("sled operations failed")
        })
    }

    async fn lock(&self, keyspace: Keyspace, key: &str) -> Result<Box<dyn Lock>> {
        let mut mlock = self.locks.lock().await;
        let lock_key = format!("/{keyspace:?}/{key}");
        if let Some(lock) = mlock.get(&lock_key) {
            Ok(Box::new(lock.clone().lock_owned().await))
        } else {
            let new_lock = Arc::new(Mutex::new(()));
            mlock.insert(lock_key, new_lock.clone());
            Ok(Box::new(new_lock.lock_owned().await))
        }
    }

    async fn watch(&self, keyspace: Keyspace, prefix: String) -> Result<Box<dyn Watch>> {
        let prefix = format!("/{keyspace:?}/{prefix}");

        Ok(Box::new(SledWatch {
            subscriber: self.db.watch_prefix(prefix),
        }))
    }
}

struct SledWatch {
    subscriber: sled::Subscriber,
}

#[tonic::async_trait]
impl Watch for SledWatch {
    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Stream for SledWatch {
    type Item = WatchEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.get_mut().subscriber.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(sled::Event::Insert { key, value })) => {
                let key = std::str::from_utf8(&key).unwrap().to_owned();
                Poll::Ready(Some(WatchEvent::Put(key, value.to_vec())))
            }
            Poll::Ready(Some(sled::Event::Remove { key })) => {
                let key = std::str::from_utf8(&key).unwrap().to_owned();
                Poll::Ready(Some(WatchEvent::Delete(key)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.subscriber.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyValueStore, SledClient, Watch, WatchEvent};

    use crate::cluster::storage::Keyspace;

    use futures::StreamExt;
    use std::result::Result;

    fn create_instance() -> Result<SledClient, Box<dyn std::error::Error>> {
        Ok(SledClient::try_new_temporary()?)
    }

    #[tokio::test]
    async fn put_read() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        client
            .put(Keyspace::Slots, key.to_owned(), value.to_vec())
            .await?;
        assert_eq!(client.get(Keyspace::Slots, key).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn read_empty() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let empty: &[u8] = &[];
        assert_eq!(client.get(Keyspace::Slots, key).await?, empty);
        Ok(())
    }

    #[tokio::test]
    async fn read_watch() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        let mut watch: Box<dyn Watch<Item = WatchEvent>> =
            client.watch(Keyspace::Slots, key.to_owned()).await?;
        client
            .put(Keyspace::Slots, key.to_owned(), value.to_vec())
            .await?;
        assert_eq!(
            watch.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value.to_owned()
            ))
        );
        let value2 = "value2".as_bytes();
        client
            .put(Keyspace::Slots, key.to_owned(), value2.to_vec())
            .await?;
        assert_eq!(
            watch.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value2.to_owned()
            ))
        );
        watch.cancel().await?;
        Ok(())
    }
}
