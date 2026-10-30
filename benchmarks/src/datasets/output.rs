use std::path::Path;
use std::sync::{Arc, LazyLock};

use aws_credential_types::provider::ProvideCredentials;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::object_store::aws::{AmazonS3Builder, AwsCredential};
use datafusion::object_store::buffered::BufWriter;
use datafusion::object_store::local::LocalFileSystem;
use datafusion::object_store::path::Path as ObjectPath;
use datafusion::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, StaticCredentialProvider,
};
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::datasets::common::workspace_root;

const SUCCESS: &str = "_SUCCESS";

/// The repository's testdata directory, the default location under which datasets are generated.
pub static TESTDATA_ROOT: LazyLock<String> = LazyLock::new(|| {
    workspace_root()
        .join("testdata")
        .to_string_lossy()
        .into_owned()
});

/// A dataset destination. Both local files and S3 objects use the same write path.
pub struct DatasetOutput {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    location: Url,
}

impl DatasetOutput {
    pub async fn new(location: &str) -> Result<Self> {
        let location = if location.contains("://") {
            Url::parse(location).map_err(|e| exec_datafusion_err!("Invalid output URL: {e}"))?
        } else {
            Url::from_directory_path(std::path::absolute(location)?)
                .map_err(|()| exec_datafusion_err!("Invalid output path"))?
        };
        if location.query().is_some() || location.fragment().is_some() {
            return exec_err!("Output URLs must not contain a query or fragment");
        }
        let (store, prefix): (Arc<dyn ObjectStore>, _) = match location.scheme() {
            "file" => {
                let path = location
                    .to_file_path()
                    .map_err(|()| exec_datafusion_err!("Invalid file URL"))?;
                (
                    Arc::new(LocalFileSystem::new()),
                    ObjectPath::from_absolute_path(path)?,
                )
            }
            "s3" => {
                let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let credentials = config.credentials_provider().ok_or_else(|| {
                    exec_datafusion_err!("AWS credential provider is unavailable")
                })?;
                let region = config.region().ok_or_else(|| {
                    exec_datafusion_err!("Set AWS_REGION or configure a region in your AWS profile")
                })?;
                let credentials = credentials.provide_credentials().await.map_err(|error| {
                    exec_datafusion_err!("AWS credential resolution failed: {error}")
                })?;
                let store = AmazonS3Builder::from_env()
                    .with_url(location.as_str())
                    .with_region(region.as_ref())
                    .with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential {
                        key_id: credentials.access_key_id().to_owned(),
                        secret_key: credentials.secret_access_key().to_owned(),
                        token: credentials.session_token().map(str::to_owned),
                    })))
                    .build()?;
                (Arc::new(store), ObjectPath::from_url_path(location.path())?)
            }
            scheme => {
                return exec_err!("Unsupported output scheme: {scheme}");
            }
        };
        Ok(Self {
            store,
            prefix,
            location,
        })
    }

    fn key(&self, relative: &str) -> ObjectPath {
        relative
            .split('/')
            .fold(self.prefix.clone(), |path, part| path.join(part))
    }

    pub fn location(&self) -> String {
        self.location.as_str().trim_end_matches('/').to_owned()
    }

    pub fn register(&self, ctx: &datafusion::prelude::SessionContext) {
        ctx.register_object_store(&self.location, Arc::clone(&self.store));
    }

    pub fn writer(&self, relative: &str) -> BufWriter {
        BufWriter::new(Arc::clone(&self.store), self.key(relative))
    }

    async fn exists(&self, relative: &str) -> Result<bool> {
        match self.store.head(&self.key(relative)).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        Ok(self
            .store
            .list(Some(&self.prefix))
            .try_next()
            .await?
            .is_none())
    }

    /// Whether a previous generation into this output completed, in which case it is skipped.
    /// Fails on an output that holds anything else, such as an interrupted generation.
    pub async fn is_complete(&self) -> Result<bool> {
        if self.exists(SUCCESS).await? {
            eprintln!("{} is already generated", self.location);
            return Ok(true);
        }
        if !self.is_empty().await? {
            return exec_err!(
                "{} is not empty but holds no complete dataset; remove it and generate again",
                self.location
            );
        }
        Ok(false)
    }

    /// Marks the output as a complete dataset, once every table has been written.
    pub async fn mark_complete(&self) -> Result<()> {
        self.write(SUCCESS, Vec::new()).await
    }

    pub async fn write(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        self.store.put(&self.key(relative), bytes.into()).await?;
        Ok(())
    }

    pub async fn copy_file(&self, relative: &str, path: &Path) -> Result<()> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut writer = self.writer(relative);
        tokio::io::copy(&mut file, &mut writer).await?;
        writer.shutdown().await?;
        Ok(())
    }
}
