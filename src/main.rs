use std::{collections::VecDeque, error::Error, pin::Pin, sync::Arc, task::Poll};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use camino::Utf8PathBuf as PathBuf;
use futures::{Future, Stream, StreamExt};
use par_stream::{ParStreamExt, TryParStreamExt};
use reqwest::{Method, Request, Response, Url};
use semver::Version;
use tower::{
    buffer::Buffer,
    util::{BoxCloneService, BoxService},
    Service, ServiceExt,
};
use tracing::metadata::LevelFilter;

#[derive(Clone)]
struct CratesIoRetry(usize);

impl CratesIoRetry {
    fn subtract(&self) -> std::future::Ready<Self> {
        std::future::ready(Self(self.0 - 1))
    }
}

impl<E: std::fmt::Debug> tower::retry::Policy<Request, Response, E> for CratesIoRetry {
    type Future = std::future::Ready<Self>;

    fn retry(&self, _: &Request, result: Result<&Response, &E>) -> Option<Self::Future> {
        match result {
            Ok(response) => {
                match response.status().as_u16() {
                    // retry on timeout or throttle
                    408 | 429 => Some(self.subtract()),
                    // don't retry otherwise
                    _ => None,
                }
            }
            Err(err) => {
                tracing::warn!(?err);
                if self.0 > 0 {
                    tracing::warn!("Retrying, {} attempts remain.", self.0);
                    Some(std::future::ready(CratesIoRetry(self.0 - 1)))
                } else {
                    None
                }
            }
        }
    }

    fn clone_request(&self, req: &Request) -> Option<Request> {
        req.try_clone()
    }
}

#[derive(Clone)]
struct CratesIoClient {
    service: Buffer<BoxService<Request, Response, Arc<dyn Error + Send + Sync>>, Request>,
}

impl CratesIoClient {
    fn new() -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Accept",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        let client = reqwest::ClientBuilder::new()
            .user_agent("crates-io-compression-bench/0.0.0 (https://github.com/lovesegfault/crates-io-compression-bench)")
            .default_headers(headers)
            .build()
            .context("build reqwest api client")?;

        let service = tower::ServiceBuilder::new()
            .buffer(10)
            .rate_limit(1, std::time::Duration::from_secs(1))
            .retry(CratesIoRetry(3))
            .timeout(std::time::Duration::from_secs(5))
            .service(client)
            .map_err(Arc::<dyn Error + Send + Sync>::from)
            .boxed();

        let service = Buffer::new(service, 100);

        Ok(Self { service })
    }

    async fn get(&mut self, url: Url) -> Result<Response> {
        self.service
            .ready()
            .await
            .map_err(|e| anyhow!(e).context("wait for service readiness"))?
            .call(Request::new(Method::GET, url))
            .await
            .map_err(|e| anyhow!(e).context("crates-io client get"))
    }

    fn top_crates_stream(&self) -> TopCratesStream {
        TopCratesStream::new(self)
    }

    async fn download_crate(
        &mut self,
        name: &str,
        version: &Version,
    ) -> Result<impl Stream<Item = reqwest::Result<Bytes>>> {
        let url = format!("https://crates.io/api/v1/crates/{name}/{version}/download");
        let url = Url::parse(&url).context("parse crate download url")?;

        Ok(self
            .get(url)
            .await
            .context("get crate download")?
            .bytes_stream())
    }
}

#[derive(Debug, serde::Deserialize)]
struct CrateEntry {
    name: String,
    newest_version: Version,
}

impl CrateEntry {
    fn download_url(&self) -> String {
        format!(
            "https://crates.io/api/v1/crates/{}/{}/download",
            self.name, self.newest_version
        )
    }
}

#[derive(Debug, serde::Deserialize)]
struct CratesListMeta {
    next_page: String,
    total: u64,
}

#[derive(Debug, serde::Deserialize)]
struct CratesListPage {
    crates: Vec<CrateEntry>,
    meta: CratesListMeta,
}

struct TopCratesStream {
    buffer: VecDeque<CrateEntry>,
    next_page: Url,
    client: Option<CratesIoClient>,
    fut: Option<
        Pin<
            Box<
                dyn Future<Output = Result<(CratesIoClient, Vec<CrateEntry>, CratesListMeta)>>
                    + Send,
            >,
        >,
    >,
}

impl TopCratesStream {
    fn new(client: &CratesIoClient) -> Self {
        Self {
            client: Some(client.clone()),
            buffer: VecDeque::new(),
            next_page: Self::page_url("?sort=recent-downloads&page=1")
                .expect("constructed initial page url is valid"),
            fut: None,
        }
    }

    fn page_url(part: &str) -> Result<Url> {
        Url::parse(&format!("https://crates.io/api/v1/crates{part}",))
            .context("parse next page url")
    }
}

impl Stream for TopCratesStream {
    type Item = Result<CrateEntry>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // If we have crates left from the previous page
        if let Some(entry) = self.buffer.pop_front() {
            return Poll::Ready(Some(Ok(entry)));
        }
        // Otherwise, get the next page
        let mut next = self.fut.take().unwrap_or_else(|| {
            let mut client = self
                .client
                .take()
                .expect("client is always Some when fut is None");
            let next_page = self.next_page.clone();
            Box::pin(async move {
                let page: CratesListPage = client
                    .get(next_page)
                    .await
                    .context("get next top crates page")?
                    .json()
                    .await
                    .context("parse crates page json")?;

                Result::<(CratesIoClient, Vec<CrateEntry>, CratesListMeta)>::Ok((
                    client,
                    page.crates,
                    page.meta,
                ))
            })
        });

        match next.as_mut().poll(cx) {
            Poll::Ready(Ok((client, entries, meta))) => {
                self.client = Some(client);
                self.buffer.extend(entries.into_iter());
                match Self::page_url(&meta.next_page) {
                    Ok(url) => {
                        self.next_page = url;
                        Poll::Ready(self.buffer.pop_front().map(Ok))
                    }
                    Err(e) => Poll::Ready(Some(
                        Err(e).context("invalid next page url in crates-io meta"),
                    )),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => {
                self.fut = Some(next);
                Poll::Pending
            }
        }
    }
}

// struct CompressionResults(DashMap<CrateEntry, DashMap<CompressionAlgo, CompressionData>>)
// struct CompressionData { original: u64, compressed: u64 }

#[tokio::main]
async fn main() -> Result<()> {
    let env = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(env)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let crates_io = CratesIoClient::new()?;
    crates_io
        .top_crates_stream()
        .take(20)
        .map(move |krate| krate.map(|inner| (crates_io.clone(), inner)))
        .try_par_then_unordered(None, move |(mut client, krate)| async move {
            let bytes = client.download_crate(&krate.name, &krate.newest_version).await?;
            Result::<(), anyhow::Error>::Ok(())
        })
        .par_for_each(None, |_| async move {})
        .await;

    Ok(())
}
