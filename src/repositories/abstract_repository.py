from abc import ABC
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID, uuid4

from sqlalchemy import ColumnElement, Table, any_, bindparam, delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import ARRAY, BYTEA
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import Entity
from core.exceptions import AlreadyExistsError, ClientError, NotFoundError, RepositoryError


@contextmanager
def repository_errors(unique_constraint: str | None = None) -> Iterator[None]:
    """Never expose driver messages or bound values. The caller owns rollback."""
    try:
        yield
    except IntegrityError as exc:
        original = exc.orig
        cause = getattr(original, "__cause__", None)
        sqlstate = getattr(original, "sqlstate", None)
        constraint = getattr(cause, "constraint_name", None)
        if sqlstate == "23505" and unique_constraint is not None and constraint == unique_constraint:
            raise AlreadyExistsError() from None
        raise RepositoryError() from None
    except SQLAlchemyError:
        raise RepositoryError() from None


class AbstractRepository[E: Entity](ABC):
    """Tenant-scoped settings CRUD. Never commits or rolls back the session."""

    table: Table
    entity: type[E]
    unique_constraint: str

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_all(self, *, equestrian_id: UUID) -> list[E]:
        with repository_errors():
            rows = await self.session.execute(
                select(self.table)
                .where(self.table.c.equestrian_id == equestrian_id)
                .order_by(self.table.c.value.asc())
            )
            return [self.entity.model_validate(dict(row)) for row in rows.mappings()]

    async def get_by_id(self, id: UUID, *, equestrian_id: UUID) -> E | None:
        with repository_errors():
            rows = await self.session.execute(
                select(self.table).where(self.table.c.id == id, self.table.c.equestrian_id == equestrian_id)
            )
            mapping = rows.mappings().first()
            return self.entity.model_validate(dict(mapping)) if mapping is not None else None

    async def get_by_value(self, value: str, *, equestrian_id: UUID) -> E | None:
        with repository_errors():
            rows = await self.session.execute(
                select(self.table).where(self.table.c.value == value, self.table.c.equestrian_id == equestrian_id)
            )
            mapping = rows.mappings().first()
            return self.entity.model_validate(dict(mapping)) if mapping is not None else None

    async def get_values(self, *, equestrian_id: UUID) -> frozenset[str]:
        with repository_errors():
            rows = await self.session.execute(
                select(self.table.c.value).where(self.table.c.equestrian_id == equestrian_id)
            )
            return frozenset(rows.scalars())

    async def create(self, entity: E, *, equestrian_id: UUID) -> E:
        # The selector is authoritative; a supplied entity cannot choose another tenant.
        data = entity.model_dump()
        data["equestrian_id"] = equestrian_id
        with repository_errors(self.unique_constraint):
            rows = await self.session.execute(insert(self.table).values(**data).returning(self.table))
            return self.entity.model_validate(dict(rows.mappings().one()))

    async def update(self, entity: E, *, equestrian_id: UUID) -> E:
        with repository_errors(self.unique_constraint):
            rows = await self.session.execute(
                update(self.table)
                .where(self.table.c.id == entity.id, self.table.c.equestrian_id == equestrian_id)
                .values(value=entity.model_dump()["value"], updated_at=datetime.now(UTC))
                .returning(self.table)
            )
            mapping = rows.mappings().first()
            if mapping is None:
                raise NotFoundError()
            return self.entity.model_validate(dict(mapping))

    async def delete(self, id: UUID, *, equestrian_id: UUID) -> None:
        with repository_errors():
            result = await self.session.execute(
                delete(self.table)
                .where(self.table.c.id == id, self.table.c.equestrian_id == equestrian_id)
                .returning(self.table.c.id)
            )
            if result.scalar_one_or_none() is None:
                raise NotFoundError()


class ResolvableRepository[E: Entity](ABC):
    """Immutable entities: only resolve and listing, no inherited settings CRUD."""

    table: Table
    entity: type[E]
    value_column: str
    hash_column: str

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def resolve(self, *, equestrian_id: UUID, values: Sequence[str]) -> dict[str, UUID]:
        if not values:
            return {}
        hashes = {value: sha256(value.encode("utf-8")).digest() for value in values}
        pairs = sorted(hashes.items(), key=lambda pair: pair[1])
        data = [
            {"id": uuid4(), "equestrian_id": equestrian_id, self.value_column: value, self.hash_column: digest}
            for value, digest in pairs
        ]
        with repository_errors():
            await self.session.execute(
                pg_insert(self.table)
                .values(data)
                .on_conflict_do_nothing(index_elements=[self.table.c.equestrian_id, self.table.c[self.hash_column]])
            )
            rows = await self.session.execute(
                select(self.table.c.id, self.table.c[self.hash_column]).where(
                    self.table.c.equestrian_id == equestrian_id,
                    self.table.c[self.hash_column] == any_(bindparam("hashes", type_=ARRAY(BYTEA))),
                ),
                {"hashes": list(hashes.values())},
            )
            ids = {row[1]: row[0] for row in rows}
            if any(digest not in ids for digest in hashes.values()):
                raise RepositoryError()
            return {value: ids[digest] for value, digest in hashes.items()}

    def _filter_conditions(self, *, equestrian_id: UUID, search: str | None) -> list[ColumnElement[bool]]:
        conditions = [self.table.c.equestrian_id == equestrian_id]
        if search is not None and (search := search.strip()):
            escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            conditions.append(self.table.c[self.value_column].ilike(f"%{escaped}%", escape="\\"))
        return conditions

    async def count_filtered(self, *, equestrian_id: UUID, search: str | None = None) -> int:
        with repository_errors():
            rows = await self.session.execute(
                select(func.count())
                .select_from(self.table)
                .where(*self._filter_conditions(equestrian_id=equestrian_id, search=search))
            )
            return rows.scalar_one()

    def _get_order(self, ordering: str | None) -> list[ColumnElement]:
        if ordering is None:
            return [self.table.c.created_at.desc(), self.table.c.id.desc()]
        elif ordering == self.value_column:
            return [self.table.c[self.value_column].asc(), self.table.c.id.asc()]
        elif ordering == f"-{self.value_column}":
            return [self.table.c[self.value_column].desc(), self.table.c.id.asc()]
        raise ClientError()

    async def get_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[E]:
        order = self._get_order(ordering)
        with repository_errors():
            rows = await self.session.execute(
                select(self.table)
                .where(*self._filter_conditions(equestrian_id=equestrian_id, search=search))
                .order_by(*order)
                .limit(limit)
                .offset(offset)
            )
            return [self.entity.model_validate(dict(row)) for row in rows.mappings()]
