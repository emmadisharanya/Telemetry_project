from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


def list_parquet_files(base_path: str | Path) -> List[Path]:
    base = Path(base_path)
    if not base.exists():
        return []
    return [p for p in base.rglob("*.parquet")]


def iter_parquet_paths_for_customers(
    base_path: str | Path, customers: Iterable[str] | None = None
) -> List[Path]:
    """
    Narrow scan to specific customers if provided, otherwise return all files.
    """
    base = Path(base_path)
    if not base.exists():
        return []

    if not customers:
        return list_parquet_files(base)

    out: List[Path] = []
    customer_set = {str(c) for c in customers}
    for p in base.rglob("customer_id=*"):
        # directory like customer_id=foo
        name = p.name
        if "=" in name:
            _, cid = name.split("=", 1)
            if cid in customer_set:
                out.extend(list(p.rglob("*.parquet")))
    return out