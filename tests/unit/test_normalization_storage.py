"""Schema/domain portion of UT-N-04/05; no service/runtime integration."""

import ast
import importlib.util
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import DefaultClause, MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

import models  # noqa: F401
from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    NormalizationSnapshot,
    PageMapping,
    RevisionStatus,
    ScanCheckpoint,
    StorageLease,
    UUIDRedirect,
)
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from models.pages import pages
from utils.basemodel import metadata

ROOT = Path(__file__).resolve().parents[2]
NAMES = {
    "normalization_tenants",
    "normalization_revisions",
    "normalization_snapshots",
    "normalization_scans",
    "normalization_mappings",
    "normalization_redirects",
    "normalization_inbox",
    "normalization_outbox",
}


def ddl(schema: MetaData) -> dict[str, tuple[str, list[str]]]:
    dialect = postgresql.dialect()
    return {
        name: (
            str(CreateTable(schema.tables[name]).compile(dialect=dialect)),
            sorted(str(CreateIndex(i).compile(dialect=dialect)) for i in schema.tables[name].indexes),
        )
        for name in NAMES
    }


def test_normalization_migration_matches_metadata() -> None:
    path = ROOT / "src/migration/versions/20261102_0004_normalization_storage.py"
    spec = importlib.util.spec_from_file_location("normalization_storage_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.revision == "20261102_0004"
    assert module.down_revision == "20261028_0003"
    assert ddl(module.metadata) == ddl(metadata)
    assert pages.c.scan_seq.nullable is False
    assert isinstance(pages.c.scan_seq.server_default, DefaultClause)
    default_arg = pages.c.scan_seq.server_default.arg
    assert not isinstance(default_arg, str)
    assert str(default_arg.compile(dialect=postgresql.dialect())) == "nextval('pages_scan_seq')"
    indexes = {str(i.name): i for i in pages.indexes}
    assert indexes["ux_pages_scan_seq"].unique
    assert [c.name for c in indexes["ix_pages_tenant_scan_seq"].columns] == ["equestrian_id", "scan_seq"]


def test_normalization_schema_has_tenant_qualified_links_and_durable_aliases() -> None:
    for name in NAMES - {"normalization_tenants", "normalization_revisions", "normalization_inbox"}:
        table = metadata.tables[name]
        fk = next(iter(table.foreign_key_constraints))
        assert [c.name for c in fk.columns] == ["equestrian_id", "operation_id"]
    for name in ("normalization_redirects", "normalization_mappings"):
        # UUID ledgers survive physical source deletion; membership is checked by repositories.
        assert all(fk.referred_table.name != "pages" for fk in metadata.tables[name].foreign_key_constraints)
    statements = "\n".join(ddl(metadata)["normalization_revisions"][1])
    assert "UNIQUE INDEX ux_normalization_active_tenant" in statements
    assert "status NOT IN ('pending', 'finalized')" in statements


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cursor": -1},
        {"cursor": 2, "high_water": 1},
        {"sealed": True},
        {"cursor": 1, "high_water": 2, "sealed": True},
        {"delete_cursor": -1},
        {"next_part_index": -1},
    ],
)
def test_normalization_checkpoint_rejects_invalid_bounds(kwargs) -> None:
    with pytest.raises(ValueError):
        ScanCheckpoint(**kwargs)


def test_normalization_checkpoint_and_snapshot_are_immutable() -> None:
    cp = ScanCheckpoint(cursor=10, high_water=10, sealed=True)
    with pytest.raises(FrozenInstanceError):
        cp.__setattr__("cursor", 11)
    snapshot = NormalizationSnapshot(query_params=("utm",), hashes=("top",))
    assert snapshot.query_params == ("utm",)


def test_normalization_mapping_redirect_and_cleanup_identity_guards() -> None:
    source, target, operation, tenant = (uuid4() for _ in range(4))
    assert PageMapping(source, target, 1, 0).source_id == source
    for args in ((source, source, 1, 0), (source, target, 0, 0), (source, target, 1, -1)):
        with pytest.raises(ValueError):
            PageMapping(*args)
    with pytest.raises(ValueError):
        UUIDRedirect(source, source, operation)
    snapshot = NormalizationSnapshot((), ())
    assert NormalizationRevision(tenant, operation, 1, RevisionStatus.PENDING, snapshot).revision == 1
    with pytest.raises(ValueError):
        NormalizationRevision(tenant, operation, 1, RevisionStatus.PENDING, snapshot, cleanup_seq=1)
    with pytest.raises(ValueError):
        NormalizationRevision(tenant, operation, 1, RevisionStatus.PENDING, snapshot, cleanup_operation_id=uuid4())
    with pytest.raises(ValueError):
        NormalizationRevision(tenant, operation, 1, RevisionStatus.PENDING, snapshot, part_count=-1)


def test_normalization_lease_and_message_guards() -> None:
    with pytest.raises(ValueError):
        StorageLease(uuid4(), datetime(2026, 1, 1), 1)
    with pytest.raises(ValueError):
        StorageLease(uuid4(), datetime.now(UTC), 0)
    assert StorageLease(uuid4(), datetime.now(UTC), 1).attempts == 1
    message = MaintenanceMessage(uuid4(), uuid4(), uuid4(), "mappings", "{}", "0" * 64)
    assert message.part_index is None
    with pytest.raises(ValueError):
        replace(message, kind="")
    with pytest.raises(ValueError):
        replace(message, payload_hash="z" * 64)
    with pytest.raises(ValueError):
        replace(message, part_index=-1)


def test_normalization_domain_and_protocol_do_not_import_infrastructure() -> None:
    for relative in ("core/entities/normalization.py", "core/protocols/repositories/normalization_repository.py"):
        tree = ast.parse((ROOT / "src" / relative).read_text())
        imports = [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        imports += [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        assert not any(
            m and m.split(".")[0] in {"sqlalchemy", "models", "repositories", "settings", "clients"} for m in imports
        )
    assert vars(NormalizationRepositoryProtocol)["_is_protocol"]


def test_normalization_migration_offline_sql_orders_guard_before_drop() -> None:
    from io import StringIO

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = ROOT / "src/migration/versions/20261102_0004_normalization_storage.py"
    spec = importlib.util.spec_from_file_location("normalization_offline", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    with Operations.context(context):
        module.upgrade()
        module.downgrade()
    sql = output.getvalue()
    assert "CREATE SEQUENCE pages_scan_seq" in sql
    assert "scan_seq BIGINT DEFAULT nextval('pages_scan_seq') NOT NULL" in sql
    for name in NAMES:
        assert f"CREATE TABLE {name}" in sql
        assert f"EXISTS (SELECT 1 FROM {name})" in sql
    assert sql.index("RAISE EXCEPTION") < sql.index("DROP TABLE")
    assert sql.index("DROP COLUMN scan_seq") < sql.index("DROP SEQUENCE pages_scan_seq")
