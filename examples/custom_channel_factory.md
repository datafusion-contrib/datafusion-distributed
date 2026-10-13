# Custom channel factory

```bash
cargo run --features integration --example custom_channel_factory
```

The example starts a loopback worker and connects the logical URL
`http://worker.example:<port>` using a custom TCP connector. The hostname is not
resolved: the application's connector chooses the socket address.

`DefaultChannelResolver::with_channel_factory` keeps the library's readiness
check, cache, concurrent connection sharing, and failed-connection eviction.
Two worker-info requests print one connection-opening message and two version
responses. Cloning the resolver shares the same cache.

The connector here is plaintext. An application can instead return a channel
using its own TLS connector and credential provider; this hook does not choose
trust roots or authentication policy. Configure the worker server consistently,
and register the same resolver on coordinator and worker sessions as described
in [Custom worker connections](../docs/source/user-guide/06-channel-resolver.md).
