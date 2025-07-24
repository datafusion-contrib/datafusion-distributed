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

/// Global singleton for worker discovery, initialized once and reused
///
/// This static variable holds the WorkerDiscovery instance that manages the process
/// of finding and tracking distributed DataFusion worker nodes. It uses lazy initialization
/// to set up worker discovery when first accessed, supporting both environment variable
/// and Kubernetes-based discovery methods.
static WORKER_DISCOVERY: OnceLock<Result<WorkerDiscovery>> = OnceLock::new();

/// Retrieves the current list of available worker node addresses
///
/// This function provides the main entry point for obtaining worker addresses in the
/// distributed system. It initializes worker discovery on first call and returns
/// the current list of known worker nodes that can accept query execution tasks.
///
/// # Discovery Methods
/// The function supports two discovery approaches:
/// - **Environment Variable**: Direct address specification via DD_WORKER_ADDRESSES
/// - **Kubernetes**: Dynamic discovery via deployment watching (DD_WORKER_DEPLOYMENT)
///
/// # Returns
/// * `Vec<Host>` - List of currently available worker nodes with their addresses
///
/// # Errors
/// Returns an error if:
/// - Worker discovery initialization fails
/// - No discovery method is properly configured
/// - Network communication with workers fails during setup
///
/// # Usage
/// Called by the query planner when distributing stages across available workers.
/// The returned addresses are used to create connections and send execution tasks.
pub fn get_worker_addresses() -> Result<Vec<Host>> {
    match WORKER_DISCOVERY.get_or_init(WorkerDiscovery::new) {
        Ok(wd) => {
            // Get current list of available worker addresses
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

/// Worker discovery service for managing distributed DataFusion worker nodes
///
/// This struct implements the core worker discovery functionality for the distributed
/// system. It maintains a dynamic list of available worker nodes and supports multiple
/// discovery mechanisms including environment variables and Kubernetes deployments.
///
/// # Discovery Mechanisms
/// - **Environment Variable**: Static list from DD_WORKER_ADDRESSES
/// - **Kubernetes Deployment**: Dynamic discovery by watching deployment pods
///
/// # Address Management
/// The service maintains an in-memory cache of worker addresses that is updated
/// dynamically as workers come online or go offline. This enables the system to
/// adapt to changing cluster topology automatically.
///
/// # Thread Safety
/// Uses RwLock to allow concurrent read access while maintaining data consistency
/// during updates from the background discovery tasks.
struct WorkerDiscovery {
    /// Thread-safe cache of worker addresses indexed by IP address
    /// Updated dynamically as workers are discovered or removed
    addresses: Arc<RwLock<HashMap<String, Host>>>,
}

impl WorkerDiscovery {
    /// Creates a new WorkerDiscovery instance and starts the discovery process
    ///
    /// This constructor initializes the worker discovery service and immediately
    /// starts the appropriate discovery mechanism based on environment variables.
    /// It spawns background tasks to continuously monitor for worker changes.
    ///
    /// # Returns
    /// * `WorkerDiscovery` - Configured discovery service with background monitoring
    ///
    /// # Errors
    /// Returns an error if no valid discovery method is configured or if
    /// initial discovery setup fails.
    pub fn new() -> Result<Self> {
        let wd = WorkerDiscovery {
            addresses: Arc::new(RwLock::new(HashMap::new())),
        };
        // Start the appropriate discovery mechanism based on environment
        wd.start()?;
        Ok(wd)
    }

    /// Returns the current list of discovered worker addresses
    ///
    /// This method provides a snapshot of all currently known worker nodes.
    /// The list is dynamically updated by background discovery tasks, so
    /// subsequent calls may return different results as workers join or leave.
    ///
    /// # Returns
    /// * `Vec<Host>` - Current list of available worker nodes
    ///
    /// # Thread Safety
    /// Uses read lock to allow concurrent access without blocking discovery updates.
    fn get_addresses(&self) -> Vec<Host> {
        let guard = self.addresses.read();
        guard.iter().map(|(_ip, host)| host.clone()).collect()
    }

    /// Starts the worker discovery process based on environment configuration
    ///
    /// This method examines environment variables to determine which discovery
    /// mechanism to use and spawns the appropriate background task. It supports
    /// both static configuration via environment variables and dynamic discovery
    /// through Kubernetes API monitoring.
    ///
    /// # Discovery Priority
    /// 1. **DD_WORKER_ADDRESSES**: If set, uses static address list
    /// 2. **DD_WORKER_DEPLOYMENT + DD_WORKER_DEPLOYMENT_NAMESPACE**: Uses Kubernetes
    /// 3. **Neither**: Returns error requiring configuration
    ///
    /// # Background Tasks
    /// Spawns async tasks that run continuously to monitor worker availability
    /// and update the address cache as changes occur.
    ///
    /// # Returns
    /// * `Ok(())` if discovery was successfully started
    /// * `Err()` if no valid discovery method is configured
    fn start(&self) -> Result<()> {
        // Check environment variables to determine discovery method
        let worker_addrs_env = std::env::var("DD_WORKER_ADDRESSES");
        let worker_deployment_env = std::env::var("DD_WORKER_DEPLOYMENT");
        let worker_deployment_namespace_env = std::env::var("DD_WORKER_DEPLOYMENT_NAMESPACE");

        if worker_addrs_env.is_ok() {
            // Use static address list from environment variable
            let addresses = self.addresses.clone();
            tokio::spawn(async move {
                set_worker_addresses_from_env(addresses, worker_addrs_env.unwrap().as_str())
                    .await
                    .expect("Could not set worker addresses from env");
            });
        } else if worker_deployment_namespace_env.is_ok() && worker_deployment_env.is_ok() {
            // Use Kubernetes deployment monitoring for dynamic discovery
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
            // No valid discovery method configured
            return Err(anyhow!(
                "Either DD_WORKER_ADDRESSES or both DD_WORKER_DEPLOYMENT and \
                 DD_WORKER_DEPLOYMENT_NAMESPACE must be set"
            )
            .into());
        }
        Ok(())
    }
}

/// Populates worker addresses from environment variable configuration
///
/// This function parses a comma-separated list of worker addresses from an environment
/// variable and populates the worker discovery cache. It validates each address by
/// attempting to connect and retrieve host information from the worker.
///
/// # Address Format
/// The environment variable should contain addresses in the format:
/// `address1,address2,address3` where each address includes port (e.g., `host:port`)
///
/// # Validation Process
/// For each address:
/// 1. Attempts to connect to the worker
/// 2. Sends a "get_host" action to retrieve worker information
/// 3. Adds validated worker to the address cache
///
/// # Arguments
/// * `addresses` - Shared address cache to populate
/// * `env_str` - Comma-separated string of worker addresses
///
/// # Returns
/// * `Ok(())` if all addresses were successfully validated and added
/// * `Err()` if any address fails validation or connection
///
/// # Example
/// ```text
/// DD_WORKER_ADDRESSES="worker1:8080,worker2:8080,worker3:8080"
/// ```
async fn set_worker_addresses_from_env(
    addresses: Arc<RwLock<HashMap<String, Host>>>,
    env_str: &str,
) -> Result<()> {
    // Parse comma-separated addresses and validate each one
    for addr in env_str.split(',') {
        // Connect to worker and retrieve host information
        let host = get_worker_host(addr.to_string())
            .await
            .context(format!("Failed to get worker host for address: {}", addr))?;
        // Add validated worker to the address cache
        addresses.write().insert(addr.to_owned(), host);
    }
    Ok(())
}

/// Continuously monitors Kubernetes deployment pods for worker discovery
///
/// This function implements dynamic worker discovery by watching a Kubernetes deployment
/// for pod changes. It monitors pod lifecycle events (add, modify, delete) and updates
/// the worker address cache accordingly, enabling automatic adaptation to cluster scaling.
///
/// # Kubernetes Integration
/// - Connects to Kubernetes API using default cluster configuration
/// - Watches specific deployment pods based on selector labels
/// - Monitors pod status changes and IP address assignments
/// - Handles pod lifecycle events (creation, updates, deletion)
///
/// # Pod Requirements
/// Workers must be deployed with:
/// - Container named "dd-worker" with exposed port
/// - Proper label selectors matching the deployment
/// - Network accessibility for health checks
///
/// # Event Processing
/// - **Added/Modified**: Validates pod IP and adds to worker cache
/// - **Deleted**: Removes worker from cache when pod terminates
/// - **Error**: Logs errors but continues monitoring
///
/// # Arguments
/// * `addresses` - Shared address cache to update with discovered workers
/// * `deployment_name` - Name of the Kubernetes deployment to monitor
/// * `namespace` - Kubernetes namespace containing the deployment
///
/// # Returns
/// This function runs indefinitely, returning only on unrecoverable errors
///
/// # Errors
/// Returns an error if:
/// - Kubernetes API connection fails
/// - Deployment or namespace cannot be found
/// - Required pod selectors are missing
/// - Worker health check communication fails
async fn watch_deployment_hosts_continuous(
    addresses: Arc<RwLock<HashMap<String, Host>>>,
    deployment_name: &str,
    namespace: &str,
) -> Result<()> {
    debug!(
        "Starting to watch deployment {} in namespace {}",
        deployment_name, namespace
    );
    // Initialize connection to Kubernetes API
    let client = Client::try_default()
        .await
        .context("Failed to create Kubernetes client")?;

    // Access the Deployments API for the specified namespace
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    // Retrieve the target deployment configuration
    let deployment = deployments
        .get(deployment_name)
        .await
        .context(format!("Failed to get deployment {}", deployment_name))?;

    // Extract label selectors from deployment spec for pod filtering
    let selector = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .context("Deployment has no selector labels")?;

    // Convert selector labels to Kubernetes label selector format
    let label_selector = selector
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",");

    // Access the Pods API for the specified namespace
    let pods: Api<Pod> = Api::namespaced(client, namespace);

    debug!(
        "Watching deployment {} in namespace {} with label selector: {}",
        deployment_name, namespace, label_selector
    );

    // Configure watch parameters with label selector
    let wp = WatchParams::default().labels(&label_selector);

    // Start watching for pod changes matching the deployment
    let mut watcher = pods
        .watch(&wp, "0")
        .await
        .context("could not build watcher")?
        .boxed();

    // Process pod events continuously
    while let Some(event_result) = watcher
        .try_next()
        .await
        .context("could not get next event from watcher")?
    {
        match &event_result {
            // Handle pod creation and modification events
            WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                trace!(
                    "Pod event: {:?}, added or modified: {:#?}",
                    event_result,
                    pod
                );
                if let Some(Some(_ip)) = pod.status.as_ref().map(|s| s.pod_ip.as_ref()) {
                    // Extract worker information from pod and validate connectivity
                    let (pod_ip, host) = get_worker_info_from_pod(pod).await?;
                    debug!(
                        "Pod {} has IP address {}, host {}",
                        pod.name_any(),
                        pod_ip,
                        host
                    );
                    // Add validated worker to the address cache
                    addresses.write().insert(pod_ip, host);
                } else {
                    trace!("Pod {} has no IP address, skipping", pod.name_any());
                }
            }
            // Handle pod deletion events
            WatchEvent::Deleted(pod) => {
                debug!("Pod deleted: {}", pod.name_any());
                if let Some(status) = &pod.status {
                    if let Some(pod_ip) = &status.pod_ip {
                        if !pod_ip.is_empty() {
                            debug!("Removing pod IP: {}", pod_ip);
                            // Remove worker from cache when pod is deleted
                            addresses.write().remove(pod_ip);
                        }
                    }
                }
            }
            // Ignore bookmark events (used for watch synchronization)
            WatchEvent::Bookmark(_) => {}
            // Log but continue on watch errors
            WatchEvent::Error(e) => {
                eprintln!("Watch error: {}", e);
            }
        }
    }

    Ok(())
}

/// Retrieves worker host information by connecting and querying a worker node
///
/// This function establishes a connection to a worker at the specified address
/// and sends a "get_host" action to retrieve the worker's self-reported host
/// information. This validation ensures the worker is responsive and accessible.
///
/// # Connection Process
/// 1. Creates gRPC channel to the worker address
/// 2. Establishes Arrow Flight client connection
/// 3. Sends "get_host" action to retrieve worker metadata
/// 4. Decodes and returns the worker's Host information
///
/// # Timeout Configuration
/// Uses a 2-second connection timeout to prevent hanging on unreachable workers.
/// This enables fast failure detection during worker discovery.
///
/// # Arguments
/// * `addr` - Worker address in "host:port" format
///
/// # Returns
/// * `Host` - Worker's self-reported host information including name and address
///
/// # Errors
/// Returns an error if:
/// - Network connection to worker fails
/// - Worker doesn't respond to get_host action
/// - Response cannot be decoded as valid Host data
/// - Connection times out (>2 seconds)
async fn get_worker_host(addr: String) -> Result<Host> {
    // Create gRPC channel with connection timeout
    let mut client = Channel::from_shared(format!("http://{addr}"))
        .context("Failed to create channel")?
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .map(FlightClient::new)
        .context("Failed to connect to worker")?;

    // Send get_host action to retrieve worker information
    let action = Action {
        r#type: "get_host".to_string(),
        body: vec![].into(),
    };

    // Execute the action and get response stream
    let mut response = client
        .do_action(action)
        .await
        .context("Failed to send action to worker")?;

    // Decode the first response as Host information
    Ok(response
        .try_next()
        .await
        .transpose()
        .context("error consuming do_action response")?
        .map(Host::decode)?
        .context("Failed to decode Host from worker response")?)
}

/// Extracts worker connection information from a Kubernetes pod specification
///
/// This function analyzes a Kubernetes pod to determine the worker's network
/// address and validates connectivity. It looks for the "dd-worker" container
/// and extracts its port configuration, then validates the worker is responsive.
///
/// # Pod Analysis Process
/// 1. Extracts pod IP address from status
/// 2. Locates "dd-worker" container in pod spec
/// 3. Retrieves container port configuration
/// 4. Constructs worker address (IP:port)
/// 5. Validates worker connectivity via get_worker_host
///
/// # Container Requirements
/// The pod must contain a container named "dd-worker" with at least one
/// exposed port. This port is used for Arrow Flight communication.
///
/// # Arguments
/// * `pod` - Kubernetes pod object containing worker container
///
/// # Returns
/// * `(String, Host)` - Tuple of (pod_ip, validated_host_info)
///
/// # Errors
/// Returns an error if:
/// - Pod has no IP address assigned
/// - No "dd-worker" container found in pod
/// - Container has no port configuration
/// - Worker connectivity validation fails
async fn get_worker_info_from_pod(pod: &Pod) -> Result<(String, Host)> {
    // Extract pod status and IP address
    let status = pod.status.as_ref().context("Pod has no status")?;
    let pod_ip = status.pod_ip.as_ref().context("Pod has no IP address")?;

    // Find the dd-worker container and extract its port configuration
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

    // Validate pod has a valid IP address
    if pod_ip.is_empty() {
        Err(anyhow::anyhow!("Pod {} has no IP address", pod.name_any()).into())
    } else {
        // Construct worker address and validate connectivity
        let host_str = format!("{}:{}", pod_ip, port);
        let host = get_worker_host(host_str.clone()).await.context(format!(
            "Failed to get worker host for pod {}",
            pod.name_any()
        ))?;
        Ok((pod_ip.to_owned(), host))
    }
}
