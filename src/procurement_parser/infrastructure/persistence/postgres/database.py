from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from procurement_parser.config.settings import DatabaseSettings


class Database:
    def __init__(self, settings: DatabaseSettings) -> None:
        self.engine: AsyncEngine = create_async_engine(
            settings.url,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_pre_ping=True,
            connect_args={
                "server_settings": {
                    "statement_timeout": str(settings.statement_timeout_seconds * 1000)
                }
            },
        )
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        await self.engine.dispose()

