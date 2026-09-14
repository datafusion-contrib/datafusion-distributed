from pathlib import Path


REPOSITORY_ROOT = Path(__file__).parents[2].as_posix()


def anonymize_snapshot(value: str) -> str:
    """Remove the checkout-specific prefix from snapshot values."""
    return value.replace(REPOSITORY_ROOT, "").replace(
        REPOSITORY_ROOT.removeprefix("/"), ""
    )
