// Scratch benchmark (not for upstream): reproduces the production outlier
// shape (4,400 tiny-file tree) through the REAL stack: DirectoryCache ->
// FastSlowStore(FilesystemStore fast) -> GrpcStore -> real ByteStream/CAS
// servers over loopback TCP. Measures cold construct time alone and under
// contention (5 concurrent 800-file trees), and prints a `tar -x` physical
// floor reference on the same volume.
//
// Run: cargo test --release -p nativelink-worker --test batch_io_outlier_bench_test -- --nocapture

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, GrpcEndpoint, GrpcSpec, MemorySpec, Retry, StoreDirection,
    StoreSpec, StoreType,
};
use nativelink_error::Error;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode, SymlinkNode,
};
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::background_spawn;
use nativelink_util::common::{DigestInfo, make_temp_path};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::directory_cache::{DirectoryCache, DirectoryCacheConfig};
use prost::Message;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

const ZERO_DIGEST_SHA256: [u8; 32] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9,
    0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52,
    0xb8, 0x55,
];

fn file_digest(tree_id: u32, idx: u32, shared: bool, content: &[u8]) -> DigestInfo {
    let mut hash = [0u8; 32];
    hash[0] = 1;
    hash[1..5].copy_from_slice(&(if shared { 0 } else { tree_id + 1 }).to_le_bytes());
    hash[5..9].copy_from_slice(&idx.to_le_bytes());
    hash[9] = u8::from(shared);
    DigestInfo::new(hash, content.len() as u64)
}

/// ~450 B/file to match the production outlier shape (2.0 MB / 4,410 files).
fn file_content(tree_id: u32, idx: u32, shared: bool) -> Vec<u8> {
    let seed = if shared {
        format!("shared-{idx}-")
    } else {
        format!("tree{tree_id}-file{idx}-")
    };
    let mut content = seed.into_bytes();
    while content.len() < 448 {
        let extend: Vec<u8> = content.clone();
        content.extend_from_slice(&extend);
    }
    content.truncate(448);
    content
}

/// Builds one tree seeded ONLY into the slow (memory-behind-gRPC) store.
/// Layout per leaf dir: `files_per_leaf` regular files (every 5th shared
/// across trees, every 7th executable, every 11th empty) plus one symlink.
async fn seed_stress_tree(
    slow_store: &Arc<MemoryStore>,
    tree_id: u32,
    mid_dirs: u32,
    leaf_dirs: u32,
    files_per_leaf: u32,
) -> Result<(DigestInfo, u32, ExpectedTree), Error> {
    let mut expected = ExpectedTree::default();
    let mut dir_seq = 0u32;
    let mut file_count = 0u32;
    let mut mid_nodes = Vec::new();
    for m in 0..mid_dirs {
        let mut leaf_nodes = Vec::new();
        for l in 0..leaf_dirs {
            let mut file_nodes = Vec::new();
            let mut symlink_nodes = Vec::new();
            for f in 0..files_per_leaf {
                let idx = (m * leaf_dirs + l) * files_per_leaf + f;
                let shared = f % 5 == 0;
                let is_executable = f % 7 == 0;
                let empty = f % 11 == 3;
                let name = format!("file_{f}");
                let rel = PathBuf::from(format!("mid_{m}/leaf_{l}/{name}"));
                file_count += 1;
                if empty {
                    expected.files.insert(rel, (Vec::new(), false));
                    file_nodes.push(FileNode {
                        name,
                        digest: Some(DigestInfo::new(ZERO_DIGEST_SHA256, 0).into()),
                        is_executable: false,
                        node_properties: None,
                    });
                    continue;
                }
                let content = file_content(tree_id, idx, shared);
                let digest = file_digest(tree_id, idx, shared, &content);
                slow_store
                    .update_oneshot(digest, Bytes::from(content.clone()))
                    .await?;
                expected.files.insert(rel, (content, is_executable));
                file_nodes.push(FileNode {
                    name,
                    digest: Some(digest.into()),
                    is_executable,
                    node_properties: None,
                });
            }
            if cfg!(unix) {
                symlink_nodes.push(SymlinkNode {
                    name: "link_to_first".to_string(),
                    target: "file_0".to_string(),
                    node_properties: None,
                });
                expected.symlinks.insert(
                    PathBuf::from(format!("mid_{m}/leaf_{l}/link_to_first")),
                    "file_0".to_string(),
                );
            }
            let leaf = ProtoDirectory {
                files: file_nodes,
                directories: vec![],
                symlinks: symlink_nodes,
                node_properties: None,
            }
            .encode_to_vec();
            let mut hash = [0u8; 32];
            hash[0] = 2;
            hash[1..5].copy_from_slice(&(tree_id + 1).to_le_bytes());
            hash[5..9].copy_from_slice(&dir_seq.to_le_bytes());
            dir_seq += 1;
            let digest = DigestInfo::new(hash, leaf.len() as u64);
            slow_store.update_oneshot(digest, Bytes::from(leaf)).await?;
            leaf_nodes.push(DirectoryNode {
                name: format!("leaf_{l}"),
                digest: Some(digest.into()),
            });
        }
        let mid = ProtoDirectory {
            files: vec![],
            directories: leaf_nodes,
            symlinks: vec![],
            node_properties: None,
        }
        .encode_to_vec();
        let mut hash = [0u8; 32];
        hash[0] = 3;
        hash[1..5].copy_from_slice(&(tree_id + 1).to_le_bytes());
        hash[5..9].copy_from_slice(&dir_seq.to_le_bytes());
        dir_seq += 1;
        let digest = DigestInfo::new(hash, mid.len() as u64);
        slow_store.update_oneshot(digest, Bytes::from(mid)).await?;
        mid_nodes.push(DirectoryNode {
            name: format!("mid_{m}"),
            digest: Some(digest.into()),
        });
    }
    let root = ProtoDirectory {
        files: vec![],
        directories: mid_nodes,
        symlinks: vec![],
        node_properties: None,
    }
    .encode_to_vec();
    let mut hash = [0u8; 32];
    hash[0] = 4;
    hash[1..5].copy_from_slice(&(tree_id + 1).to_le_bytes());
    let digest = DigestInfo::new(hash, root.len() as u64);
    slow_store.update_oneshot(digest, Bytes::from(root)).await?;
    Ok((digest, file_count, expected))
}

/// Expected on-disk state: relative path -> (content, `is_executable`) or
/// symlink target.
#[derive(Debug, Default)]
struct ExpectedTree {
    files: HashMap<PathBuf, (Vec<u8>, bool)>,
    symlinks: HashMap<PathBuf, String>,
}

async fn verify_tree(dest: &Path, expected: &ExpectedTree) -> Result<(), String> {
    for (rel, (content, is_executable)) in &expected.files {
        let path = dest.join(rel);
        let actual = tokio::fs::read(&path)
            .await
            .map_err(|e| format!("missing file {}: {e}", path.display()))?;
        if &actual != content {
            return Err(format!(
                "content mismatch at {} ({} vs {} bytes)",
                path.display(),
                actual.len(),
                content.len()
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = tokio::fs::metadata(&path)
                .await
                .map_err(|e| format!("stat {}: {e}", path.display()))?
                .permissions()
                .mode();
            if *is_executable && mode & 0o111 == 0 {
                return Err(format!("expected +x on {}", path.display()));
            }
            // Empty files are written directly (not CAS-backed) and keep
            // default permissions; all CAS-backed non-executables must be
            // read-only whether they came via hardlink or copy.
            if !*is_executable && !content.is_empty() && mode & 0o222 != 0 {
                return Err(format!(
                    "expected read-only mode on {} (got {mode:o})",
                    path.display()
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = is_executable;
    }
    for (rel, target) in &expected.symlinks {
        let path = dest.join(rel);
        let actual = tokio::fs::read_link(&path)
            .await
            .map_err(|e| format!("missing symlink {}: {e}", path.display()))?;
        if actual.to_str() != Some(target.as_str()) {
            return Err(format!("symlink target mismatch at {}", path.display()));
        }
    }
    Ok(())
}

async fn start_real_cas_server(memory_store: Arc<MemoryStore>) -> u16 {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("main_cas", Store::new(memory_store));

    let bs_server = ByteStreamServer::new(
        &[WithInstanceName {
            instance_name: String::new(),
            config: ByteStreamConfig {
                cas_store: "main_cas".to_string(),
                persist_stream_on_disconnect_timeout_s: 0,
                max_bytes_per_stream: 64 * 1024,
            },
        }],
        &store_manager,
        &RemoteCacheCompressionInstances::default(),
    )
    .expect("bytestream server");
    let cas_server = CasServer::new(
        &[WithInstanceName {
            instance_name: String::new(),
            config: CasStoreConfig {
                cas_store: "main_cas".to_string(),
                experimental_chunking: None,
            },
        }],
        &store_manager,
        &RemoteCacheCompressionInstances::default(),
    )
    .expect("cas server");

    let listener = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = listener.local_addr().unwrap().port();
    background_spawn!("bench_server", async move {
        Server::builder()
            .add_service(bs_server.into_service())
            .add_service(cas_server.into_service())
            .serve_with_incoming(listener)
            .await
            .unwrap();
    });
    port
}

fn grpc_spec(port: u16) -> GrpcSpec {
    GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![GrpcEndpoint {
            address: format!("http://localhost:{port}"),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 0,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
        }],
        store_type: StoreType::Cas,
        retry: Retry::default(),
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 120,
        use_legacy_resource_names: false,
        headers: HashMap::new(),
        forward_headers: vec![],
    }
}

async fn make_cache(grpc_store: Arc<GrpcStore>, tag: &str) -> Result<Arc<DirectoryCache>, Error> {
    let fast_spec = FilesystemSpec {
        content_path: make_temp_path(&format!("cas_content_{tag}")),
        temp_path: make_temp_path(&format!("cas_temp_{tag}")),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store: Arc<FilesystemStore> = FilesystemStore::new(&fast_spec).await?;
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_spec),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast_store),
        Store::new(grpc_store),
    );
    Ok(Arc::new(
        DirectoryCache::new(
            DirectoryCacheConfig {
                cache_root: make_temp_path(&format!("directory_cache_{tag}")).into(),
                ..Default::default()
            },
            cas_store,
        )
        .await?,
    ))
}

async fn tar_floor_ms(tree_path: &Path, tag: &str) -> (u128, u128) {
    let tar_path = PathBuf::from(make_temp_path(&format!("floor_{tag}.tar")));
    let extract_path = PathBuf::from(make_temp_path(&format!("floor_extract_{tag}")));
    if let Some(parent) = tar_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::create_dir_all(&extract_path).await.unwrap();
    let start = Instant::now();
    let status = tokio::process::Command::new("tar")
        .arg("-cf")
        .arg(&tar_path)
        .arg("-C")
        .arg(tree_path)
        .arg(".")
        .status()
        .await
        .expect("tar -c");
    assert!(status.success());
    let create_ms = start.elapsed().as_millis();
    let start = Instant::now();
    let status = tokio::process::Command::new("tar")
        .arg("-xf")
        .arg(&tar_path)
        .arg("-C")
        .arg(&extract_path)
        .status()
        .await
        .expect("tar -x");
    assert!(status.success());
    (create_ms, start.elapsed().as_millis())
}

// Plain tokio::test on purpose: #[nativelink_test] installs a capturing
// TRACE subscriber whose multi-GB output dominates wall time and poisons
// the measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_outlier_tree_real_grpc() -> Result<(), Error> {
    // Big tree = production outlier shape: 20x10x22 = 4400 files @~450B.
    // Mediums = concurrent cold neighbors: 4x5x40 = 800 files each.
    let memory_store = MemoryStore::new(&MemorySpec::default());
    let (big_root, big_files, big_expected) = seed_stress_tree(&memory_store, 0, 20, 10, 22).await?;
    let mut mediums = Vec::new();
    for t in 1..=5u32 {
        mediums.push(seed_stress_tree(&memory_store, t, 4, 5, 40).await?);
    }
    let port = start_real_cas_server(memory_store).await;

    // Scenario A: big tree alone, cold.
    let grpc_a = GrpcStore::new(&grpc_spec(port)).await?;
    let cache_a = make_cache(grpc_a, "a").await?;
    let dest_a = PathBuf::from(make_temp_path("wd_a")).join("big");
    let start = Instant::now();
    assert!(!cache_a.get_or_create(big_root, &dest_a).await?);
    let alone_ms = start.elapsed().as_millis();
    verify_tree(&dest_a, &big_expected)
        .await
        .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "{e}"))?;

    // Physical floor: tar -x of the same tree on the same volume.
    let (tar_c_ms, tar_x_ms) = tar_floor_ms(&dest_a, "a").await;

    // Scenario B: big tree + 5 concurrent medium trees, all cold, fresh cache.
    let grpc_b = GrpcStore::new(&grpc_spec(port)).await?;
    let cache_b = make_cache(grpc_b, "b").await?;
    let wd_b = PathBuf::from(make_temp_path("wd_b"));
    tokio::fs::create_dir_all(&wd_b).await?;
    let mut handles = Vec::new();
    for (i, (root, _, _)) in mediums.iter().enumerate() {
        let cache = cache_b.clone();
        let root = *root;
        let dest = wd_b.join(format!("medium_{i}"));
        handles.push(tokio::spawn(
            async move { cache.get_or_create(root, &dest).await },
        ));
    }
    let dest_big = wd_b.join("big");
    let start = Instant::now();
    assert!(!cache_b.get_or_create(big_root, &dest_big).await?);
    let contended_ms = start.elapsed().as_millis();
    for handle in handles {
        handle.await.expect("task panicked")?;
    }
    verify_tree(&dest_big, &big_expected)
        .await
        .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "{e}"))?;
    for (i, (_, _, medium_expected)) in mediums.iter().enumerate() {
        verify_tree(&wd_b.join(format!("medium_{i}")), medium_expected)
            .await
            .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "{e}"))?;
    }

    eprintln!("\nBENCH_RESULT");
    eprintln!("=== OUTLIER TREE, REAL gRPC ({big_files} files @~450B) ===");
    eprintln!("big tree ALONE:     {alone_ms}ms");
    eprintln!("big tree CONTENDED: {contended_ms}ms (5x800-file neighbors)");
    eprintln!("tar floor:          create {tar_c_ms}ms, extract {tar_x_ms}ms");
    eprintln!("BENCH_END");
    Ok(())
}
