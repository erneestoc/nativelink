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

use std::collections::HashMap;

use nativelink_config::cas_server::{CapabilitiesConfig, WithInstanceName};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::GetCapabilitiesRequest;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
use nativelink_service::capabilities_server::CapabilitiesServer;
use pretty_assertions::assert_eq;
use tonic::Request;

/// The configured `max_batch_total_size_bytes` for an instance must be the
/// value advertised back via `GetCapabilities`.
#[nativelink_test]
async fn advertises_configured_max_batch_total_size() -> Result<(), Error> {
    const CONFIGURED: u64 = 4 * 1024 * 1024;

    let configs = vec![WithInstanceName {
        instance_name: "main".to_string(),
        config: CapabilitiesConfig {
            remote_execution: None,
            max_batch_total_size_bytes: CONFIGURED,
        },
    }];
    let server = CapabilitiesServer::new(&configs, &HashMap::new()).await?;

    let resp = server
        .get_capabilities(Request::new(GetCapabilitiesRequest {
            instance_name: "main".to_string(),
        }))
        .await
        .expect("get_capabilities should succeed")
        .into_inner();

    let cache_caps = resp
        .cache_capabilities
        .expect("cache_capabilities should be present");
    assert_eq!(
        cache_caps.max_batch_total_size_bytes,
        i64::try_from(CONFIGURED).unwrap(),
    );
    Ok(())
}

/// An instance not present in the capabilities config still gets a sane,
/// positive `max_batch_total_size_bytes` (the 4MB fallback) rather than 0.
#[nativelink_test]
async fn unknown_instance_gets_default_max_batch_total_size() -> Result<(), Error> {
    let server = CapabilitiesServer::new(&[], &HashMap::new()).await?;

    let resp = server
        .get_capabilities(Request::new(GetCapabilitiesRequest {
            instance_name: "does-not-exist".to_string(),
        }))
        .await
        .expect("get_capabilities should succeed")
        .into_inner();

    let cache_caps = resp
        .cache_capabilities
        .expect("cache_capabilities should be present");
    assert_eq!(cache_caps.max_batch_total_size_bytes, 4 * 1024 * 1024);
    Ok(())
}
