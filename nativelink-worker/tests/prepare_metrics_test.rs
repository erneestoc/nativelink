// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serial_test::serial;

#[serial]
mod tests {
    use core::future::Future;
    use core::time::Duration;
    use std::sync::{Arc, Mutex};

    use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
    use nativelink_config::stores::{
        FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
    };
    use nativelink_error::ResultExt;
    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Action, Command, Directory, DirectoryNode, ExecuteRequest, FileNode,
    };
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
    use nativelink_store::ac_utils::serialize_and_upload_message;
    use nativelink_store::fast_slow_store::FastSlowStore;
    use nativelink_store::filesystem_store::FilesystemStore;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::action_messages::OperationId;
    use nativelink_util::common::{fs, make_temp_path};
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
    use nativelink_util::store_trait::{Store, StoreLike};
    use nativelink_worker::running_actions_manager::{
        Callbacks, ExecutionConfiguration, RunningAction, RunningActionsManager,
        RunningActionsManagerArgs, RunningActionsManagerImpl,
    };
    use opentelemetry::global;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
    use pretty_assertions::assert_eq;

    const WORKER_ID: &str = "prepare_metrics_worker";
    const NUM_ROOT_FILES: usize = 25;
    const NUM_SUBDIR_FILES: usize = 2;

    /// One captured data point, flattened for assertions.
    #[derive(Debug, Clone)]
    struct Sample {
        name: String,
        attrs: Vec<(String, String)>,
        count: u64,
        sum: f64,
    }

    /// Minimal in-test metric exporter: flattens histogram and sum data
    /// points into `Sample`s. Avoids the `opentelemetry_sdk` `testing`
    /// feature so no dependency repin is needed.
    #[derive(Debug, Clone, Default)]
    struct CapturingExporter {
        samples: Arc<Mutex<Vec<Sample>>>,
    }

    impl CapturingExporter {
        fn collect(&self) -> Vec<Sample> {
            self.samples.lock().unwrap().clone()
        }
    }

    impl PushMetricExporter for CapturingExporter {
        fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
            fn attrs_of<'a>(
                iter: impl Iterator<Item = &'a opentelemetry::KeyValue>,
            ) -> Vec<(String, String)> {
                iter.map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect()
            }
            let mut out = Vec::new();
            for scope in metrics.scope_metrics() {
                for metric in scope.metrics() {
                    let name = metric.name().to_string();
                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                            for dp in hist.data_points() {
                                out.push(Sample {
                                    name: name.clone(),
                                    attrs: attrs_of(dp.attributes()),
                                    count: dp.count(),
                                    sum: dp.sum(),
                                });
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for dp in sum.data_points() {
                                out.push(Sample {
                                    name: name.clone(),
                                    attrs: attrs_of(dp.attributes()),
                                    count: dp.value(),
                                    sum: 0.0,
                                });
                            }
                        }
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                            for dp in sum.data_points() {
                                out.push(Sample {
                                    name: name.clone(),
                                    attrs: attrs_of(dp.attributes()),
                                    count: 0,
                                    sum: dp.value() as f64,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            self.samples.lock().unwrap().extend(out);
            core::future::ready(Ok(()))
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    fn stage_samples<'a>(samples: &'a [Sample], stage: &str) -> Vec<&'a Sample> {
        samples
            .iter()
            .filter(|s| {
                s.name == "worker.prepare.stage.duration"
                    && s.attrs
                        .iter()
                        .any(|(k, v)| k == "worker.prepare.stage" && v == stage)
            })
            .collect()
    }

    fn fs_op_count(samples: &[Sample], op: &str, target: &str) -> u64 {
        samples
            .iter()
            .filter(|s| {
                s.name == "worker.prepare.fs.ops"
                    && s.attrs.iter().any(|(k, v)| k == "worker.fs.op" && v == op)
                    && s.attrs
                        .iter()
                        .any(|(k, v)| k == "worker.fs.target" && v == target)
            })
            .map(|s| s.count)
            .sum()
    }

    #[nativelink_test]
    async fn prepare_metrics_capture_all_segments() -> Result<(), Box<dyn core::error::Error>> {
        // Install the capturing provider BEFORE anything touches
        // PREPARE_METRICS: the LazyLock instruments bind to the global
        // provider present at first use.
        let exporter = CapturingExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());

        // Store setup (mirrors running_actions_manager_test::setup_stores).
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: None,
            ..Default::default()
        };
        let slow_config = MemorySpec::default();
        let fast_store: Arc<FilesystemStore> = FilesystemStore::new(&fast_config).await?;
        let slow_store = MemoryStore::new(&slow_config);
        let ac_store = MemoryStore::new(&slow_config);
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(slow_config),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                bypass_dedup_threshold_bytes: 0,
            },
            Store::new(fast_store.clone()),
            Store::new(slow_store.clone()),
        );

        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config: &UploadActionResultConfig {
                    upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(600),
                max_cleanup_wait: Duration::from_secs(10),
                max_cleanup_backoff: Duration::from_millis(100),
                timeout_handled_externally: false,
                directory_cache: None,
                #[cfg(target_os = "linux")]
                use_namespaces: false,
            },
            Callbacks {
                now_fn: std::time::SystemTime::now,
                sleep_fn: |duration| Box::pin(tokio::time::sleep(duration)),
            },
        )?);

        // Build a 27-file input root: 25 small files at the root plus a
        // subdirectory with 2 more (exercises mkdir + a second directory
        // proto fetch). This mirrors the production report shape (27 files,
        // a few KB each).
        let mut hasher_digests = Vec::new();
        for i in 0..(NUM_ROOT_FILES + NUM_SUBDIR_FILES) {
            let content = format!("prepare metrics file body {i:04} {}", "x".repeat(512));
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(content.as_bytes());
            let digest = hasher.finalize_digest();
            cas_store
                .update_oneshot(digest, content.into())
                .await
                .err_tip(|| "uploading input file blob")?;
            hasher_digests.push(digest);
        }
        let file_node = |name: String, digest| FileNode {
            name,
            digest: Some(digest),
            is_executable: false,
            node_properties: None,
        };
        let subdir = Directory {
            files: (0..NUM_SUBDIR_FILES)
                .map(|i| {
                    file_node(
                        format!("sub_file_{i}.txt"),
                        hasher_digests[NUM_ROOT_FILES + i].into(),
                    )
                })
                .collect(),
            ..Default::default()
        };
        let subdir_digest = serialize_and_upload_message(
            &subdir,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root = Directory {
            files: (0..NUM_ROOT_FILES)
                .map(|i| file_node(format!("file_{i:02}.txt"), hasher_digests[i].into()))
                .collect(),
            directories: vec![DirectoryNode {
                name: "subdir".to_string(),
                digest: Some(subdir_digest.into()),
            }],
            ..Default::default()
        };
        let input_root_digest = serialize_and_upload_message(
            &input_root,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let command = Command {
            arguments: vec!["true".to_string()],
            output_files: vec!["out/result.txt".to_string()],
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: None,
                    worker_id: WORKER_ID.to_string(),
                },
            )
            .await?;

        // Prepare (the phase under test), then clean up (records teardown).
        let running_action = running_action.prepare_action().await?;
        running_action.cleanup().await?;

        provider.force_flush()?;
        let samples = exporter.collect();

        // Per-action stage histogram: each prepare stage recorded once.
        let mut breakdown = Vec::new();
        for stage in [
            "queue_delay",
            "command_fetch",
            "input_materialize",
            "output_paths",
            "teardown",
        ] {
            let stage_data = stage_samples(&samples, stage);
            let count: u64 = stage_data.iter().map(|s| s.count).sum();
            let sum_ms: f64 = stage_data.iter().map(|s| s.sum).sum();
            assert_eq!(count, 1, "expected exactly one {stage} sample");
            breakdown.push((stage, sum_ms));
        }
        // No execution happened, so no pre_spawn_delay may be recorded.
        assert_eq!(
            stage_samples(&samples, "pre_spawn_delay").len(),
            0,
            "pre_spawn_delay must not be recorded without execute()"
        );

        // Filesystem op counters: 27 hardlinked files, at least one mkdir
        // (the subdirectory), no copies/symlinks.
        assert_eq!(
            fs_op_count(&samples, "hardlink", "file"),
            (NUM_ROOT_FILES + NUM_SUBDIR_FILES) as u64,
            "all input files must materialize via hardlink"
        );
        assert!(
            fs_op_count(&samples, "mkdir", "dir") >= 1,
            "subdirectory creation must be counted"
        );
        assert_eq!(fs_op_count(&samples, "copy", "file"), 0);
        assert_eq!(fs_op_count(&samples, "symlink", "file"), 0);

        // Proto fetches: the root and the subdirectory Directory protos.
        let dir_proto_fetches: u64 = samples
            .iter()
            .filter(|s| {
                s.name == "worker.prepare.proto_fetch.duration"
                    && s.attrs
                        .iter()
                        .any(|(k, v)| k == "worker.proto.kind" && v == "directory")
            })
            .map(|s| s.count)
            .sum();
        assert_eq!(dir_proto_fetches, 2, "root + subdir Directory fetches");

        // The in-flight gauge must return to zero.
        let active: f64 = samples
            .iter()
            .filter(|s| s.name == "worker.prepare.materializations.active")
            .map(|s| s.sum)
            .sum();
        assert!(
            active.abs() < f64::EPSILON,
            "materializations.active must net to zero, got {active}"
        );

        // Local read on the production-reported shape (27 files, few KB).
        eprintln!("=== prepare breakdown (27 small files, local) ===");
        for (stage, sum_ms) in &breakdown {
            eprintln!("{stage:>18}: {sum_ms:8.3} ms");
        }
        let hardlink_ms: f64 = samples
            .iter()
            .filter(|s| {
                s.name == "worker.prepare.fs_op.duration"
                    && s.attrs
                        .iter()
                        .any(|(k, v)| k == "worker.fs.op" && v == "hardlink")
            })
            .map(|s| s.sum)
            .sum();
        eprintln!("  hardlink total: {hardlink_ms:8.3} ms across 27 files");

        Ok(())
    }
}
