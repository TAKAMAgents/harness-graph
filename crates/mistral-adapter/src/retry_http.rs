//! HTTP transport that preserves provider `Retry-After` without retaining bodies.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use rig::{
    http_client::{
        Error, HttpClientExt, LazyBody, MultipartForm, Request, ReqwestClient, Response,
        StreamingResponse,
    },
    wasm_compat::WasmCompatSend,
};
use tokio::{sync::Mutex, time::Instant};

const MAX_PROVIDER_RETRY_AFTER: Duration = Duration::from_secs(90);

/// Closed provider retry instruction retained outside source-bearing errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderRetryInstruction {
    /// No valid provider delay accompanied the transient response.
    Absent,
    /// The provider supplied a bounded delay that must be honored.
    Wait(Duration),
    /// The provider requested a delay outside this adapter's safety bound.
    ExceedsBound,
}

/// Shared rate-limit gate for concurrent requests from one Mistral adapter.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderRetryGate {
    state: Arc<Mutex<ProviderRetryState>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProviderRetryState {
    not_before: Option<Instant>,
    exceeds_bound: bool,
}

impl ProviderRetryGate {
    async fn observe(&self, instruction: ProviderRetryInstruction) {
        let mut state = self.state.lock().await;
        match instruction {
            ProviderRetryInstruction::Absent => {}
            ProviderRetryInstruction::Wait(delay) => {
                let candidate = Instant::now() + delay;
                state.not_before = Some(
                    state
                        .not_before
                        .map_or(candidate, |current| current.max(candidate)),
                );
            }
            ProviderRetryInstruction::ExceedsBound => state.exceeds_bound = true,
        }
    }

    /// Resolve the shared provider delay after one transient response.
    pub(crate) async fn instruction(&self) -> ProviderRetryInstruction {
        let mut state = self.state.lock().await;
        if state.exceeds_bound {
            state.exceeds_bound = false;
            return ProviderRetryInstruction::ExceedsBound;
        }
        let Some(not_before) = state.not_before else {
            return ProviderRetryInstruction::Absent;
        };
        let delay = not_before.saturating_duration_since(Instant::now());
        if delay.is_zero() {
            state.not_before = None;
            ProviderRetryInstruction::Absent
        } else {
            ProviderRetryInstruction::Wait(delay)
        }
    }
}

/// Reqwest backend that preserves response headers needed by retry policy.
#[derive(Clone)]
pub(crate) struct RetryAwareHttpClient {
    inner: ReqwestClient,
    retry_gate: ProviderRetryGate,
}

impl RetryAwareHttpClient {
    /// Construct a transport and its shared provider retry gate.
    pub(crate) fn new(inner: ReqwestClient, retry_gate: ProviderRetryGate) -> Self {
        Self { inner, retry_gate }
    }
}

impl Default for RetryAwareHttpClient {
    fn default() -> Self {
        Self::new(ReqwestClient::default(), ProviderRetryGate::default())
    }
}

impl std::fmt::Debug for RetryAwareHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetryAwareHttpClient")
            .field("response_bodies", &"not retained on HTTP errors")
            .finish_non_exhaustive()
    }
}

impl HttpClientExt for RetryAwareHttpClient {
    fn send<T, U>(
        &self,
        request: Request<T>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        let (parts, body) = request.into_parts();
        let request = self
            .inner
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body.into());
        let retry_gate = self.retry_gate.clone();

        async move {
            let response = request.send().await.map_err(instance_error)?;
            if !response.status().is_success() {
                let status = response.status();
                if status.as_u16() == 429 || status.is_server_error() {
                    let instruction = response
                        .headers()
                        .get("retry-after")
                        .map_or(ProviderRetryInstruction::Absent, parse_retry_after);
                    retry_gate.observe(instruction).await;
                }
                // Provider bodies can contain reflected request data. Preserve
                // the typed HTTP status and deliberately discard the body.
                return Err(Error::InvalidStatusCode(status));
            }

            let mut builder = Response::builder().status(response.status());
            if let Some(headers) = builder.headers_mut() {
                *headers = response.headers().clone();
            }
            let body: LazyBody<U> = Box::pin(async move {
                let bytes = response.bytes().await.map_err(instance_error)?;
                Ok(U::from(bytes))
            });
            builder.body(body).map_err(Error::Protocol)
        }
    }

    fn send_multipart<U>(
        &self,
        request: Request<MultipartForm>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
    where
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        self.inner.send_multipart(request)
    }

    fn send_streaming<T>(
        &self,
        request: Request<T>,
    ) -> impl Future<Output = Result<StreamingResponse, Error>> + WasmCompatSend
    where
        T: Into<Bytes> + WasmCompatSend,
    {
        self.inner.send_streaming(request)
    }
}

fn parse_retry_after(value: &rig::http_client::HeaderValue) -> ProviderRetryInstruction {
    let Ok(value) = value.to_str() else {
        return ProviderRetryInstruction::Absent;
    };
    let delay = value.parse::<u64>().map_or_else(
        |_| {
            httpdate::parse_http_date(value).map_or(Duration::ZERO, |deadline| {
                deadline
                    .duration_since(std::time::SystemTime::now())
                    .unwrap_or(Duration::ZERO)
            })
        },
        Duration::from_secs,
    );
    if delay > MAX_PROVIDER_RETRY_AFTER {
        ProviderRetryInstruction::ExceedsBound
    } else {
        ProviderRetryInstruction::Wait(delay)
    }
}

#[cfg(not(target_family = "wasm"))]
fn instance_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Instance(Box::new(error))
}

#[cfg(target_family = "wasm")]
fn instance_error(error: impl std::error::Error + 'static) -> Error {
    Error::Instance(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::{ProviderRetryInstruction, parse_retry_after};

    #[test]
    fn retry_after_supports_delta_seconds_http_dates_and_rejects_unbounded_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let one_second = rig::http_client::HeaderValue::from_static("1");
        assert_eq!(
            parse_retry_after(&one_second),
            ProviderRetryInstruction::Wait(std::time::Duration::from_secs(1))
        );

        let unbounded = rig::http_client::HeaderValue::from_static("91");
        assert_eq!(
            parse_retry_after(&unbounded),
            ProviderRetryInstruction::ExceedsBound
        );
        let future = std::time::SystemTime::now() + std::time::Duration::from_secs(30);
        let http_date = rig::http_client::HeaderValue::from_str(&httpdate::fmt_http_date(future))?;
        assert!(matches!(
            parse_retry_after(&http_date),
            ProviderRetryInstruction::Wait(delay)
                if (29..=30).contains(&delay.as_secs())
        ));
        Ok(())
    }
}
