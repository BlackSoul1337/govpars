from __future__ import annotations

from dataclasses import dataclass

from procurement_parser.config.settings import Settings
from procurement_parser.domain.models import Source
from procurement_parser.infrastructure.captcha.solvers import build_captcha_solver
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.repositories import (
    PostgresEntityRepository,
    PostgresFrontierRepository,
)
from procurement_parser.infrastructure.persistence.postgres.runtime_state import (
    PostgresRuntimeStateRepository,
)
from procurement_parser.infrastructure.sources.eep_mitwork.adapter import EepMitworkAdapter
from procurement_parser.infrastructure.sources.zakup_sk.adapter import ZakupSkAdapter


@dataclass(slots=True)
class RuntimeContext:
    settings: Settings
    database: Database
    frontier: PostgresFrontierRepository
    entities: PostgresEntityRepository
    adapter: EepMitworkAdapter | ZakupSkAdapter
    captcha_solver: object
    runtime_state: PostgresRuntimeStateRepository

    async def close(self) -> None:
        await self.adapter.close()
        close = getattr(self.captcha_solver, "close", None)
        if close:
            await close()
        await self.database.close()


def build_context(settings: Settings) -> RuntimeContext:
    database = Database(settings.database)
    frontier = PostgresFrontierRepository(database)
    entities = PostgresEntityRepository(
        database,
        revision_mode=settings.app.revision_mode,
    )
    runtime_state = PostgresRuntimeStateRepository(database)
    captcha_solver = build_captcha_solver(settings.captcha, runtime_state)
    if settings.source.name == Source.EEP_MITWORK.value:
        adapter = EepMitworkAdapter(
            settings.source,
            settings.network,
            runtime_state,
        )
    elif settings.source.name == Source.ZAKUP_SK.value:
        adapter = ZakupSkAdapter(
            settings.source,
            settings.network,
            settings.captcha,
            captcha_solver,
            settings.runtime,
            runtime_state=runtime_state,
        )
    else:
        raise ValueError(f"Unsupported source: {settings.source.name}")
    return RuntimeContext(
        settings=settings,
        database=database,
        frontier=frontier,
        entities=entities,
        adapter=adapter,
        captcha_solver=captcha_solver,
        runtime_state=runtime_state,
    )
