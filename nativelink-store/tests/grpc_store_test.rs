use core::pin::Pin;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex;
use futures::stream::unfold;
use futures::{Stream, StreamExt};
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::{
    Capabilities, CapabilitiesServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, CacheCapabilities, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetCapabilitiesRequest, GetTreeRequest, GetTreeResponse, ServerCapabilities,
    batch_read_blobs_response, compressor, digest_function,
};
use nativelink_proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::background_spawn;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreLike, UploadSizeInfo};
use nativelink_util::telemetry::ClientHeaders;
use opentelemetry::Context;
use regex::Regex;
use tokio::time::timeout;
use tonic::metadata::KeyAndValueRef;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const RAW_INPUT: &str = "123";

fn test_spec<T: Into<String>>(endpoint: T, use_legacy_resource_names: bool) -> GrpcSpec {
    GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![GrpcEndpoint {
            address: endpoint.into(),
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
        rpc_timeout_s: 1,
        use_legacy_resource_names,
        headers: HashMap::new(),
        forward_headers: vec![],
        max_batch_size_bytes: 0,
    }
}

#[nativelink_test]
async fn fast_find_missing_blobs() -> Result<(), Error> {
    let spec = test_spec("http://foobar", false);
    let store = GrpcStore::new(&spec).await?;
    let request = Request::new(FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![],
        digest_function: digest_function::Value::Sha256.into(),
    });
    let res = timeout(Duration::from_secs(1), async move {
        store.find_missing_blobs(request).await
    })
    .await??;
    let inner_res = res.into_inner();
    assert_eq!(inner_res.missing_blob_digests.len(), 0);
    Ok(())
}

#[derive(Debug, Clone)]
struct ReadRequestHolder {
    request: ReadRequest,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct FakeStreamServer {
    write_requests: Arc<Mutex<Vec<WriteRequest>>>,
    read_requests: Arc<Mutex<Vec<ReadRequestHolder>>>,
}

impl FakeStreamServer {
    fn new() -> Self {
        Self {
            write_requests: Arc::new(Mutex::new(vec![])),
            read_requests: Arc::new(Mutex::new(vec![])),
        }
    }
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + 'static>>;

struct ReaderState {
    responded: bool,
}

#[tonic::async_trait]
impl ByteStream for FakeStreamServer {
    type ReadStream = ReadStream;

    async fn read(
        &self,
        grpc_request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let mut request_metadata: HashMap<String, String> = HashMap::new();
        for kv in grpc_request.metadata().iter() {
            match kv {
                KeyAndValueRef::Ascii(metadata_key, metadata_value) => {
                    request_metadata.insert(
                        metadata_key.to_string(),
                        metadata_value.to_str().unwrap().to_string(),
                    );
                }
                KeyAndValueRef::Binary(metadata_key, metadata_value) => {
                    request_metadata
                        .insert(metadata_key.to_string(), format!("{metadata_value:#?}"));
                }
            }
        }
        let read_request = grpc_request.into_inner();
        self.read_requests.lock().await.push(ReadRequestHolder {
            request: read_request,
            metadata: request_metadata,
        });

        let folded = unfold(ReaderState { responded: false }, async move |state| {
            if state.responded {
                return None;
            }
            let response = ReadResponse {
                data: RAW_INPUT.as_bytes().into(),
            };
            Some((Ok(response), ReaderState { responded: true }))
        });
        Ok(Response::new(Box::pin(folded)))
    }

    async fn write(
        &self,
        grpc_request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let write_request = match grpc_request.into_inner().next().await {
            None => {
                return Err(Status::unknown("Client closed stream"));
            }
            Some(Err(err)) => return Err(err),
            Some(Ok(write_request)) => write_request,
        };
        info!(?write_request, "write request");
        let committed_size = write_request.data.len() as i64;
        self.write_requests.lock().await.push(write_request);
        Ok(Response::new(WriteResponse { committed_size }))
    }

    #[allow(clippy::unimplemented)]
    async fn query_write_status(
        &self,
        _grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        unimplemented!();
    }
}

async fn make_fake_bytestream_server() -> (FakeStreamServer, u16) {
    let fake_stream_server = FakeStreamServer::new();
    let server = ByteStreamServer::new(fake_stream_server.clone());
    let listener = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = listener.local_addr().unwrap().port();

    background_spawn!("server", async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming(listener)
            .await
            .unwrap();
    });

    (fake_stream_server, port)
}

async fn write_update_works_core(
    use_legacy_resource_names: bool,
    upload_pattern: Regex,
) -> Result<(), Error> {
    let (server, port) = make_fake_bytestream_server().await;
    let spec = test_spec(
        format!("http://localhost:{port}"),
        use_legacy_resource_names,
    );
    let store = GrpcStore::new(&spec).await?;
    let digest = DigestInfo::try_new(VALID_HASH, RAW_INPUT.len()).unwrap();

    let (mut tx, rx) = make_buf_channel_pair();
    let send_fut = async move {
        tx.send(RAW_INPUT.into()).await?;
        tx.send_eof()
    };
    let (res1, res2) = futures::join!(
        send_fut,
        store.update(
            digest,
            rx,
            UploadSizeInfo::ExactSize(RAW_INPUT.len().try_into().unwrap())
        )
    );
    res1.merge(res2)?;

    let write_requests = server.write_requests.lock().await;
    assert_eq!(write_requests.len(), 1);
    let write_request = write_requests.first().unwrap();
    assert!(
        upload_pattern.is_match(&write_request.resource_name),
        "resource name: {}",
        write_request.resource_name
    );
    assert_eq!(write_request.data, RAW_INPUT.as_bytes());
    Ok(())
}

#[nativelink_test]
async fn write_update_works() -> Result<(), Error> {
    let upload_pattern = Regex::new("/uploads/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/blobs/sha256/0123456789abcdef000000000000000000010000000000000123456789abcdef/3").unwrap();
    write_update_works_core(false, upload_pattern).await
}

#[nativelink_test]
async fn write_update_works_with_legacy_resource_names() -> Result<(), Error> {
    let upload_pattern = Regex::new("/uploads/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/blobs/0123456789abcdef000000000000000000010000000000000123456789abcdef/3").unwrap();
    write_update_works_core(true, upload_pattern).await
}

async fn read_works_core<F>(
    use_legacy_resource_names: bool,
    upload_pattern: &str,
    edit_spec: F,
) -> Result<ReadRequestHolder, Error>
where
    F: FnOnce(GrpcSpec) -> GrpcSpec,
{
    let (server, port) = make_fake_bytestream_server().await;
    let spec = edit_spec(test_spec(
        format!("http://localhost:{port}"),
        use_legacy_resource_names,
    ));
    let store = GrpcStore::new(&spec).await?;
    let digest = DigestInfo::try_new(VALID_HASH, RAW_INPUT.len()).unwrap();

    let (tx, mut rx) = make_buf_channel_pair();
    store.get_part(digest, tx, 0, None).await.unwrap();
    let bytes = rx.recv().await?;
    assert_eq!(bytes, RAW_INPUT.as_bytes());

    let read_requests = server.read_requests.lock().await;
    assert_eq!(read_requests.len(), 1);
    let read_request = read_requests.first().unwrap();
    assert_eq!(upload_pattern, &read_request.request.resource_name);

    Ok(read_request.clone())
}

#[nativelink_test]
async fn read_works() -> Result<(), Error> {
    let upload_pattern =
        "/blobs/sha256/0123456789abcdef000000000000000000010000000000000123456789abcdef/3";
    read_works_core(false, upload_pattern, core::convert::identity)
        .await
        .unwrap();
    Ok(())
}

#[nativelink_test]
async fn read_works_with_legacy_resource_names() -> Result<(), Error> {
    let upload_pattern =
        "/blobs/0123456789abcdef000000000000000000010000000000000123456789abcdef/3";
    read_works_core(true, upload_pattern, core::convert::identity)
        .await
        .unwrap();
    Ok(())
}

#[nativelink_test]
async fn read_works_with_headers() -> Result<(), Error> {
    fn set_spec(mut spec: GrpcSpec) -> GrpcSpec {
        spec.headers.insert("foo".into(), "bar".into());
        // Testing with mixed case, as it gets lowercased internally
        spec.forward_headers.push("SomeTHING".into());
        spec
    }

    let upload_pattern =
        "/blobs/sha256/0123456789abcdef000000000000000000010000000000000123456789abcdef/3";

    let client_headers = {
        let mut headers: HashMap<String, String> = HashMap::new();
        // We're inserting a lowercase one here as the telemetry insertion uses a lowercase one
        headers.insert("something".to_string(), "From outside".to_string());
        ClientHeaders(Arc::new(headers))
    };

    let cx_guard = Context::map_current(|cx| cx.with_value(client_headers)).attach();

    let read_request = read_works_core(false, upload_pattern, set_spec)
        .await
        .unwrap();
    assert_eq!(read_request.metadata.get("foo"), Some(&"bar".to_string()));
    assert_eq!(
        read_request.metadata.get("something"),
        Some(&"From outside".to_string()),
        "{:#?}",
        read_request.metadata
    );
    drop(cx_guard);

    Ok(())
}

// ---- BatchReadBlobs (get_many) test harness ----
//
// A fake server implementing both ContentAddressableStorage and Capabilities,
// just enough to exercise GrpcStore::get_many and its capabilities discovery.

#[derive(Debug, Clone)]
struct FakeCasServer {
    /// Blob contents the server will return, keyed by digest hash hex.
    blobs: Arc<Mutex<HashMap<String, bytes::Bytes>>>,
    /// Every `BatchReadBlobs` request received, for assertions on batching.
    batch_requests: Arc<Mutex<Vec<BatchReadBlobsRequest>>>,
    /// Value advertised in `GetCapabilities`' `max_batch_total_size_bytes`.
    advertised_max_batch_size: i64,
}

impl FakeCasServer {
    fn new(advertised_max_batch_size: i64) -> Self {
        Self {
            blobs: Arc::new(Mutex::new(HashMap::new())),
            batch_requests: Arc::new(Mutex::new(vec![])),
            advertised_max_batch_size,
        }
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for FakeCasServer {
    #[allow(clippy::unimplemented)]
    async fn find_missing_blobs(
        &self,
        _request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        unimplemented!("find_missing_blobs not used by get_many tests")
    }

    #[allow(clippy::unimplemented)]
    async fn batch_update_blobs(
        &self,
        _request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        unimplemented!("batch_update_blobs not used by get_many tests")
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let request = request.into_inner();
        self.batch_requests.lock().await.push(request.clone());
        let blobs = self.blobs.lock().await;
        let responses = request
            .digests
            .into_iter()
            .map(|digest| {
                let hash = digest.hash.clone();
                blobs.get(&hash).map_or_else(
                    || batch_read_blobs_response::Response {
                        digest: Some(digest.clone()),
                        data: bytes::Bytes::new(),
                        compressor: compressor::Value::Identity.into(),
                        status: Some(GrpcStatus {
                            code: tonic::Code::NotFound.into(),
                            message: "missing".to_string(),
                            details: vec![],
                        }),
                    },
                    |data| batch_read_blobs_response::Response {
                        digest: Some(digest.clone()),
                        data: data.clone(),
                        compressor: compressor::Value::Identity.into(),
                        status: Some(GrpcStatus::default()),
                    },
                )
            })
            .collect();
        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    type GetTreeStream =
        Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send + 'static>>;

    #[allow(clippy::unimplemented)]
    async fn get_tree(
        &self,
        _request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        unimplemented!("get_tree not used by get_many tests")
    }
}

#[tonic::async_trait]
impl Capabilities for FakeCasServer {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        Ok(Response::new(ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                max_batch_total_size_bytes: self.advertised_max_batch_size,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

async fn make_fake_cas_server(advertised_max_batch_size: i64) -> (FakeCasServer, u16) {
    let fake = FakeCasServer::new(advertised_max_batch_size);
    let listener = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = listener.local_addr().unwrap().port();
    let cas_service = ContentAddressableStorageServer::new(fake.clone());
    let capabilities_service = CapabilitiesServer::new(fake.clone());
    background_spawn!("fake_cas_server", async move {
        Server::builder()
            .add_service(cas_service)
            .add_service(capabilities_service)
            .serve_with_incoming(listener)
            .await
            .unwrap();
    });
    (fake, port)
}

/// Hex hash with the low byte set to `n`, distinct per blob.
fn hash_for(n: u8) -> String {
    format!("{n:02x}{}", &VALID_HASH[2..])
}

// get_many fetches several blobs that all fit in one batch and returns each
// one's bytes positionally; an absent blob comes back as Ok(None).
#[nativelink_test]
async fn get_many_batches_and_returns_blobs() -> Result<(), Error> {
    // Large advertised limit -> everything in one BatchReadBlobs request.
    let (server, port) = make_fake_cas_server(4 * 1024 * 1024).await;
    {
        let mut blobs = server.blobs.lock().await;
        blobs.insert(hash_for(1), bytes::Bytes::from_static(b"alpha"));
        blobs.insert(hash_for(3), bytes::Bytes::from_static(b"charlie"));
    }
    let spec = test_spec(format!("http://localhost:{port}"), false);
    let store = GrpcStore::new(&spec).await?;

    let d1 = DigestInfo::try_new(&hash_for(1), 5)?;
    let d2 = DigestInfo::try_new(&hash_for(2), 9)?; // never inserted
    let d3 = DigestInfo::try_new(&hash_for(3), 7)?;
    let keys = [
        nativelink_util::store_trait::StoreKey::from(d1),
        nativelink_util::store_trait::StoreKey::from(d2),
        nativelink_util::store_trait::StoreKey::from(d3),
    ];
    let results = store.get_many(&keys).await?;
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].as_ref().expect("ok").as_deref(),
        Some(&b"alpha"[..])
    );
    assert_eq!(results[1].as_ref().expect("ok").as_deref(), None);
    assert_eq!(
        results[2].as_ref().expect("ok").as_deref(),
        Some(&b"charlie"[..])
    );

    // All three digests fit under the 4MB limit -> exactly one batch RPC.
    assert_eq!(server.batch_requests.lock().await.len(), 1);
    Ok(())
}

// When the upstream advertises a small max_batch_total_size_bytes, get_many
// must split the digests across multiple BatchReadBlobs requests.
#[nativelink_test]
async fn get_many_splits_by_advertised_batch_size() -> Result<(), Error> {
    // Advertise a 10-byte limit; each blob is 8 bytes, so only one blob
    // fits per batch -> three separate BatchReadBlobs requests.
    let (server, port) = make_fake_cas_server(10).await;
    {
        let mut blobs = server.blobs.lock().await;
        for n in 1..=3u8 {
            blobs.insert(hash_for(n), bytes::Bytes::from_static(b"12345678"));
        }
    }
    let spec = test_spec(format!("http://localhost:{port}"), false);
    let store = GrpcStore::new(&spec).await?;

    let keys: Vec<_> = (1..=3u8)
        .map(|n| {
            nativelink_util::store_trait::StoreKey::from(
                DigestInfo::try_new(&hash_for(n), 8).unwrap(),
            )
        })
        .collect();
    let results = store.get_many(&keys).await?;
    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(r.as_ref().expect("ok").as_deref(), Some(&b"12345678"[..]));
    }
    // 8-byte blobs against a 10-byte limit -> one blob per batch.
    assert_eq!(server.batch_requests.lock().await.len(), 3);
    Ok(())
}
