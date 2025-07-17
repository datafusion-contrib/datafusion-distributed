use std::{ env, sync::{Arc}};

use anyhow::{anyhow, Context};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::{
    api::{Api, ListParams, ResourceExt},
    Client,
};
use tonic::{async_trait};

use crate::{
    logging::trace, result::Result, transport::{self, WorkerTransport}, transport_traits::{GrpcTransport, InMemTransport}, vocab::Host
};
use crate::test_worker::TestWorker;

#[async_trait]
pub trait WorkerDiscovery: Send + Sync {
    async fn workers(
        &self,
    ) -> Result<Vec<(Host, Arc<dyn WorkerTransport>)>>;
}

pub struct TestDiscovery { workers: Vec<(Host, Arc<dyn WorkerTransport>)> }

impl TestDiscovery {
    /// Spin up `n` duplex-backed Flight servers and return their transports.
    pub async fn new(n: usize) -> Result<Self> {
        let mut workers = Vec::with_capacity(n);

        for i in 0..n {
            // 1. Build the pair (client transport & background server task).
            let (transport, _server_task) = InMemTransport::pair(TestWorker::default()).await?;

            let transport: Arc<dyn WorkerTransport> = transport; // TODO: I dont like this upcast

            // 2. Give the worker a human-friendly name (handy for debug logs).
            let host = Host {
                name: format!("test-{i}"),
                addr: format!("inmem://{i}"), 
            };

            workers.push((host.clone(), transport.clone())); // TODO: I dont like this clone
            transport::register(&host, transport);
        }

        Ok(Self { workers })
    }
}

#[async_trait]
impl WorkerDiscovery for TestDiscovery {
    async fn workers(&self) -> Result<Vec<(Host, Arc<dyn WorkerTransport>)>> {
        // This is trivial, the addresses are dummy 
        Ok(self.workers.clone())
    }
}

pub struct EnvDiscovery {
    cached: Vec<(Host, Arc<dyn WorkerTransport>)>,
}

impl EnvDiscovery {
    pub async fn new() -> Result<Self> {
        let raw = env::var("DD_WORKER_ADDRESSES")
            .context("DD_WORKER_ADDRESSES must be set for EnvDiscovery")?;

        let mut cached = Vec::new();
        for token in raw.split(',').filter(|s| !s.is_empty()) {
            let (name, addr) = match token.split_once('/') {
                Some((n, a)) => (n.to_string(), a.to_string()),
                None => (token.to_string(), token.to_string()),
            };

            let host = Host { name, addr: addr.clone() };
            let transport: Arc<dyn WorkerTransport> =
                GrpcTransport::connect(&addr).await?;
            transport::register(&host, transport.clone());
            cached.push((host, transport));
        }
        Ok(Self { cached })
    }
}

#[async_trait]
impl WorkerDiscovery for EnvDiscovery {
    async fn workers(&self) -> Result<Vec<(Host, Arc<dyn WorkerTransport>)>> {
        // Static list of addresses
        Ok(self.cached.clone())
    }
}

pub struct K8sDiscovery {
    deployment: String,
    namespace:  String,
    port_name:  String, //defaults to first containerPort
}

impl K8sDiscovery {
    pub fn new() -> anyhow::Result<Self> {
        let deployment = env::var("DD_WORKER_DEPLOYMENT")
            .context("DD_WORKER_DEPLOYMENT must be set for K8sDiscovery")?;
       let namespace  = env::var("DD_WORKER_DEPLOYMENT_NAMESPACE")
           .context("DD_WORKER_DEPLOYMENT_NAMESPACE must be set for K8sDiscovery")?;

        Ok(Self {
           deployment,
           namespace,
           port_name: "dd-worker".into(),
       })
    }

    async fn list_pods(&self) -> Result<Vec<Pod>> {
        let client  = Client::try_default().await.context("failed to create kube client")?;
        let pods: Api<Pod> = Api::namespaced(client, &self.namespace);

        // Re-use the selector of the deployment – robust & efficient
        let dep_api: Api<Deployment> = Api::namespaced(pods.clone().into_client(), &self.namespace);
        let dep = dep_api
            .get(&self.deployment)
            .await
            .context("failed to get deployment")?;
        let selector = dep.spec
            .and_then(|s| s.selector.match_labels)
            .ok_or_else(|| anyhow!("deployment has no selector"))?;
        let selector_string = selector.into_iter()
            .map(|(k,v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");

        let lp = ListParams::default().labels(&selector_string);
        Ok(pods.list(&lp).await.context("failed to list pods")?.items)
    }

    async fn pod_to_worker(&self, pod: &Pod) -> Result<(Host, Arc<dyn WorkerTransport>)> {
        let ip   = pod.status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .ok_or_else(|| anyhow!("pod {} has no IP yet", pod.name_any()))?;

        // find the port labelled `dd-worker` or just take the first one
        let port = pod.spec
            .as_ref()
            .and_then(|s| {
                s.containers.iter().flat_map(|c| c.ports.as_ref())
                 .flatten()
                 .find(|p| p.name.as_deref() == Some(&self.port_name))
                 .or_else(|| s.containers.iter()
                                .flat_map(|c| c.ports.as_ref()).flatten().next())
                 .map(|p| p.container_port)
            })
            .ok_or_else(|| anyhow!("pod {} has no container port", pod.name_any()))?;

        let addr = format!("{ip}:{port}");
        let host = Host { name: pod.name_any(), addr: addr.clone() };
        let tx   = GrpcTransport::connect(&addr).await?;
        transport::register(&host, tx.clone());
        Ok((host, tx))
    }
}

#[async_trait]
impl WorkerDiscovery for K8sDiscovery {
    async fn workers(&self) -> Result<Vec<(Host, Arc<dyn WorkerTransport>)>> {
        let pods = self.list_pods().await?;
        let mut out = Vec::with_capacity(pods.len());
        for pod in pods {
            match self.pod_to_worker(&pod).await {
                Ok(pair) => out.push(pair),
                Err(e)   => trace!("skip pod: {e:#}, pod={}", pod.name_any()),
            }
        }
        if out.is_empty() {
            Err(anyhow!(
                "no ready pods found for deployment {} in {}",
                self.deployment, self.namespace
            ))
        } else {
            Ok(out)
        }
    }
}