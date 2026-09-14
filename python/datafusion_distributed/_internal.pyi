"""Types implemented by the local native extension.

Configuration and object-store classes intentionally duplicate names exposed by
``datafusion._internal``: PyO3 class identities are extension-local and these
opaque values have no cross-extension import/export protocol. Expressions and
supported UDFs instead use the protobuf bridge declared below.
"""

from typing import Any

class SessionConfig:
    def __init__(self, config_options: dict[str, str] | None = None) -> None: ...

class RuntimeEnvBuilder:
    def __init__(self) -> None: ...

class object_store:
    class AmazonS3:
        def __init__(
            self,
            bucket_name: str,
            region: str | None = None,
            access_key_id: str | None = None,
            secret_access_key: str | None = None,
            session_token: str | None = None,
            endpoint: str | None = None,
            allow_http: bool = False,
            imdsv1_fallback: bool = False,
        ) -> None: ...

    class GoogleCloud:
        def __init__(
            self,
            bucket_name: str,
            service_account_path: str | None = None,
        ) -> None: ...

    class Http:
        def __init__(self, url: str) -> None: ...

    class LocalFileSystem:
        def __init__(self, prefix: str | None = None) -> None: ...

    class MicrosoftAzure:
        def __init__(
            self,
            container_name: str,
            account: str | None = None,
            access_key: str | None = None,
            bearer_token: str | None = None,
            client_id: str | None = None,
            client_secret: str | None = None,
            tenant_id: str | None = None,
            sas_query_pairs: list[tuple[str, str]] | None = None,
            use_emulator: bool | None = None,
            allow_http: bool | None = None,
            use_fabric_endpoint: bool | None = None,
        ) -> None: ...

class Worker:
    def __init__(self) -> None: ...
    def start(self, host: str, port: int) -> str: ...
    def stop(self) -> None: ...
    def run(self, host: str, port: int) -> None: ...

def create_distributed_session(
    worker_resolver: Any,
    config: Any | None = None,
    runtime: Any | None = None,
) -> Any: ...
def rebind_distributed_planner(context: Any) -> None: ...
def deserialize_expression(context: Any, expression: bytes) -> Any: ...
def serialize_expression(context: Any, expression: Any) -> bytes: ...
def register_scalar_udf(context: Any, expression: bytes) -> None: ...
def register_aggregate_udf(context: Any, expression: bytes) -> None: ...
def register_window_udf(context: Any, expression: bytes) -> None: ...
