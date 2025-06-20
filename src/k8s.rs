use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, anyhow};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::{
    Client,
    api::{Api, ResourceExt, WatchEvent, WatchParams},
};
use parking_lot::RwLock;

use crate::{
    logging::{debug, error, trace},
    result::Result,
};

static WORKER_DISCOVERY: OnceLock<Result<WorkerDiscovery>> = OnceLock::new();

pub fn get_worker_addresses() -> Result<Vec<(String, String)>> {
    match WORKER_DISCOVERY.get_or_init(WorkerDiscovery::new) {
        Ok(wd) => Ok(wd.get_addresses()),
        Err(e) => Err(anyhow!("Failed to initialize WorkerDiscovery: {}", e).into()),
    }
}

struct WorkerDiscovery {
    addresses: Arc<RwLock<HashMap<String, (String, String)>>>,
}

impl WorkerDiscovery {
    pub fn new() -> Result<Self> {
        let wd = WorkerDiscovery {
            addresses: Arc::new(RwLock::new(HashMap::new())),
        };
        wd.start()?;
        Ok(wd)
    }

    fn get_addresses(&self) -> Vec<(String, String)> {
        let guard = self.addresses.read();
        guard
            .iter()
            .map(|(_ip, (name, addr))| (name.clone(), addr.clone()))
            .collect()
    }

    fn start(&self) -> Result<()> {
        let worker_addrs_env = std::env::var("DFRAY_WORKER_ADDRESSES");
        let worker_deployment_env = std::env::var("DFRAY_WORKER_DEPLOYMENT");
        let worker_deployment_namespace_env = std::env::var("DFRAY_WORKER_DEPLOYMENT_NAMESPACE");

        if worker_addrs_env.is_ok() {
            // if the env var is set, use it
            self.set_worker_addresses_from_env(worker_addrs_env.unwrap().as_str())
                .context("Failed to set worker addresses from env var")?;
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
                "Either DFRAY_WORKER_ADDRESSES or both DFRAY_WORKER_DEPLOYMENT and \
                 DFRAY_WORKER_DEPLOYMENT_NAMESPACE must be set"
            )
            .into());
        }
        Ok(())
    }

    fn set_worker_addresses_from_env(&self, env_str: &str) -> Result<()> {
        // get addresss from an env var where addresses are split by comans
        // and in the form of name/address,name/address
        let mut guard = self.addresses.write();

        for addr in env_str.split(',') {
            let parts: Vec<&str> = addr.split('/').collect();
            if parts.len() != 2 {
                return Err(anyhow!("Invalid worker address format: {addr}").into());
            }
            let name = parts[0].to_string();
            let address = parts[1].to_string();
            guard.insert(address.clone(), (name, address));
        }
        Ok(())
    }
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
    addresses: Arc<RwLock<HashMap<String, (String, String)>>>,
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
                    event_result, pod
                );
                if let Some(Some(_ip)) = pod.status.as_ref().map(|s| s.pod_ip.as_ref()) {
                    let (pod_ip, name_str, host_str) = get_worker_info_from_pod(pod)?;
                    debug!(
                        "Pod {} has IP address {}, name {}, host {}",
                        pod.name_any(),
                        pod_ip,
                        name_str,
                        host_str
                    );
                    addresses.write().insert(pod_ip, (name_str, host_str));
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

fn get_worker_info_from_pod(pod: &Pod) -> Result<(String, String, String)> {
    let status = pod.status.as_ref().context("Pod has no status")?;
    let pod_ip = status.pod_ip.as_ref().context("Pod has no IP address")?;

    // filter on container name
    let port = pod
        .spec
        .as_ref()
        .and_then(|spec| {
            spec.containers
                .iter()
                .find(|c| c.name == "dfray-worker")
                .and_then(|c| {
                    c.ports
                        .as_ref()
                        .and_then(|ports| ports.iter().next().map(|p| p.container_port))
                })
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No could not find container port for container named dfray-worker found in pod {}",
                pod.name_any()
            )
        })?;

    if pod_ip.is_empty() {
        Err(anyhow::anyhow!("Pod {} has no IP address", pod.name_any()).into())
    } else {
        let host_str = format!("{}:{}", pod_ip, port);
        let name_str = format!("{}:{}", pod_ip, port); // for now
        Ok((pod_ip.to_owned(), name_str, host_str))
    }
}
