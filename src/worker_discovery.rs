use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::{anyhow, Context};
use arrow_flight::{Action, FlightClient};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::{
    api::{Api, ResourceExt, WatchEvent, WatchParams},
    Client,
};
use parking_lot::RwLock;
use prost::Message;
use tonic::transport::Channel;

use crate::{
    logging::{debug, error, trace},
    result::Result,
    vocab::Host,
};

static WORKER_DISCOVERY: OnceLock<Result<WorkerDiscovery>> = OnceLock::new();

pub fn get_worker_addresses() -> Result<Vec<Host>> {
    match WORKER_DISCOVERY.get_or_init(WorkerDiscovery::new) {
        Ok(wd) => {
            let worker_addrs = wd.get_addresses();
            debug!(
                "Worker addresses found:\n{}",
                worker_addrs
                    .iter()
                    .map(|host| format!("{host}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            Ok(worker_addrs)
        }
        Err(e) => Err(anyhow!("Failed to initialize WorkerDiscovery: {}", e).into()),
    }
}

struct WorkerDiscovery {
    addresses: Arc<RwLock<HashMap<String, Host>>>,
}

impl WorkerDiscovery {
    pub fn new() -> Result<Self> {
        let wd = WorkerDiscovery {
            addresses: Arc::new(RwLock::new(HashMap::new())),
        };
        wd.start()?;
        Ok(wd)
    }

    fn get_addresses(&self) -> Vec<Host> {
        let guard = self.addresses.read();
        guard.iter().map(|(_ip, host)| host.clone()).collect()
    }

    fn start(&self) -> Result<()> {
        let worker_addrs_env = std::env::var("DD_WORKER_ADDRESSES");
        let worker_deployment_env = std::env::var("DD_WORKER_DEPLOYMENT");
        let worker_deployment_namespace_env = std::env::var("DD_WORKER_DEPLOYMENT_NAMESPACE");

        if worker_addrs_env.is_ok() {
            let addresses = self.addresses.clone();
            tokio::spawn(async move {
                // if the env var is set, use it
                set_worker_addresses_from_env(addresses, worker_addrs_env.unwrap().as_str())
                    .await
                    .expect("Could not set worker addresses from env");
            });
        } else if worker_deployment_namespace_env.is_ok() && worker_deployment_env.is_ok() {
            let addresses = self.addresses.clone();
            let deployment = worker_deployment_env.unwrap();
            let namespace = worker_deployment_namespace_env.unwrap();
            tokio::spawn(async move {
                match watch_deployment_hosts_continuous(addresses, &deployment, &namespace).await {
                    Ok(_) => {}
                    Err(e) => error!("Error starting worker watcher: {:?}", e),
                }
            });
        } else {
            // if neither env var is set, return an error
            return Err(anyhow!(
                "Either DD_WORKER_ADDRESSES or both DD_WORKER_DEPLOYMENT and \
                 DD_WORKER_DEPLOYMENT_NAMESPACE must be set"
            )
            .into());
        }
        Ok(())
    }
}

async fn set_worker_addresses_from_env(
    addresses: Arc<RwLock<HashMap<String, Host>>>,
    env_str: &str,
) -> Result<()> {
    // get addresss from an env var where addresses are split by comans
    // and in the form of name/address,name/address

    for addr in env_str.split(',') {
        let host = get_worker_host(addr.to_string())
            .await
            .context(format!("Failed to get worker host for address: {}", addr))?;
        addresses.write().insert(addr.to_owned(), host);
    }
    Ok(())
}

/// Continuously watch for changes to pods in a Kubernetes deployment and call a
/// handler function whenever the list of hosts changes.
///
/// # Arguments
/// * `deployment_name` - Name of the deployment
/// * `namespace` - Kubernetes namespace where the deployment is located
/// * `handler` - A function to call when the host list changes
///
/// # Returns
/// This function runs indefinitely until an error occurs
///
/// # Errors
/// Returns an error if there's an issue connecting to the Kubernetes API
/// or if the deployment or its pods cannot be found
async fn watch_deployment_hosts_continuous(
    addresses: Arc<RwLock<HashMap<String, Host>>>,
    deployment_name: &str,
    namespace: &str,
) -> Result<()> {
    debug!(
        "Starting to watch deployment {} in namespace {}",
        deployment_name, namespace
    );
    // Initialize the Kubernetes client
    let client = Client::try_default()
        .await
        .context("Failed to create Kubernetes client")?;

    // Access the Deployments API
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    // Get the specific deployment
    let deployment = deployments
        .get(deployment_name)
        .await
        .context(format!("Failed to get deployment {}", deployment_name))?;

    // Extract the selector labels from the deployment
    let selector = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .context("Deployment has no selector labels")?;

    // Convert selector to a string format for the label selector
    let label_selector = selector
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",");

    // Access the Pods API
    let pods: Api<Pod> = Api::namespaced(client, namespace);

    debug!(
        "Watching deployment {} in namespace {} with label selector: {}",
        deployment_name, namespace, label_selector
    );

    let wp = WatchParams::default().labels(&label_selector);

    // Start watching for pod changes
    let mut watcher = pods
        .watch(&wp, "0")
        .await
        .context("could not build watcher")?
        .boxed();

    while let Some(event_result) = watcher
        .try_next()
        .await
        .context("could not get next event from watcher")?
    {
        match &event_result {
            WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                trace!(
                    "Pod event: {:?}, added or modified: {:#?}",
                    event_result,
                    pod
                );
                if let Some(Some(_ip)) = pod.status.as_ref().map(|s| s.pod_ip.as_ref()) {
                    let (pod_ip, host) = get_worker_info_from_pod(pod).await?;
                    debug!(
                        "Pod {} has IP address {}, host {}",
                        pod.name_any(),
                        pod_ip,
                        host
                    );
                    addresses.write().insert(pod_ip, host);
                } else {
                    trace!("Pod {} has no IP address, skipping", pod.name_any());
                }
            }
            WatchEvent::Deleted(pod) => {
                debug!("Pod deleted: {}", pod.name_any());
                if let Some(status) = &pod.status {
                    if let Some(pod_ip) = &status.pod_ip {
                        if !pod_ip.is_empty() {
                            debug!("Removing pod IP: {}", pod_ip);
                            addresses.write().remove(pod_ip);
                        }
                    }
                }
            }
            WatchEvent::Bookmark(_) => {}
            WatchEvent::Error(e) => {
                eprintln!("Watch error: {}", e);
            }
        }
    }

    Ok(())
}

async fn get_worker_host(addr: String) -> Result<Host> {
    let mut client = Channel::from_shared(format!("http://{addr}"))
        .context("Failed to create channel")?
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .map(FlightClient::new)
        .context("Failed to connect to worker")?;

    let action = Action {
        r#type: "get_host".to_string(),
        body: vec![].into(),
    };

    let mut response = client
        .do_action(action)
        .await
        .context("Failed to send action to worker")?;

    Ok(response
        .try_next()
        .await
        .transpose()
        .context("error consuming do_action response")?
        .map(Host::decode)?
        .context("Failed to decode Host from worker response")?)
}

async fn get_worker_info_from_pod(pod: &Pod) -> Result<(String, Host)> {
    let status = pod.status.as_ref().context("Pod has no status")?;
    let pod_ip = status.pod_ip.as_ref().context("Pod has no IP address")?;

    // filter on container name
    let port = pod
        .spec
        .as_ref()
        .and_then(|spec| {
            spec.containers
                .iter()
                .find(|c| c.name == "dd-worker")
                .and_then(|c| {
                    c.ports
                        .as_ref()
                        .and_then(|ports| ports.iter().next().map(|p| p.container_port))
                })
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No could not find container port for container named dd-worker found in pod {}",
                pod.name_any()
            )
        })?;

    if pod_ip.is_empty() {
        Err(anyhow::anyhow!("Pod {} has no IP address", pod.name_any()).into())
    } else {
        let host_str = format!("{}:{}", pod_ip, port);
        let host = get_worker_host(host_str.clone()).await.context(format!(
            "Failed to get worker host for pod {}",
            pod.name_any()
        ))?;
        Ok((pod_ip.to_owned(), host))
    }
}
