from __future__ import annotations

import asyncio
import time

import httpx

from aegisdb.config import Settings


class ClusterManager:
    def __init__(
        self,
        settings: Settings,
    ):
        self.settings = settings

        self.health: dict[str, dict] = {
            node_id: {
                "healthy":
                    node_id
                    == settings.node_id,

                "last_seen":
                    (
                        time.time()
                        if node_id
                        == settings.node_id
                        else None
                    ),

                "latency_ms":
                    0.0,
            }
            for node_id
            in settings.cluster_nodes
        }

        self.client = httpx.AsyncClient(
            timeout=
                settings.heartbeat_timeout
        )

        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True

        self._task = (
            asyncio.create_task(
                self._heartbeat_loop()
            )
        )

    async def stop(self) -> None:
        self._running = False

        if self._task is not None:
            self._task.cancel()

            try:
                await self._task

            except asyncio.CancelledError:
                pass

        await self.client.aclose()

    async def _heartbeat_loop(
        self,
    ) -> None:
        while self._running:
            await asyncio.gather(
                *[
                    self._check_node(
                        node_id
                    )
                    for node_id
                    in self.settings.cluster_nodes
                    if node_id
                    != self.settings.node_id
                ],
                return_exceptions=True,
            )

            self.health[
                self.settings.node_id
            ] = {
                "healthy": True,
                "last_seen": time.time(),
                "latency_ms": 0.0,
            }

            await asyncio.sleep(
                self.settings.heartbeat_interval
            )

    async def _check_node(
        self,
        node_id: str,
    ) -> None:
        start = time.perf_counter()

        try:
            response = await self.client.get(
                self.url(
                    node_id,
                    "/internal/health",
                )
            )

            response.raise_for_status()

            elapsed = (
                time.perf_counter()
                - start
            ) * 1000

            self.health[node_id] = {
                "healthy": True,
                "last_seen": time.time(),
                "latency_ms": round(
                    elapsed,
                    2,
                ),
            }

        except Exception:
            previous = (
                self.health.get(
                    node_id,
                    {},
                )
            )

            self.health[node_id] = {
                "healthy": False,
                "last_seen":
                    previous.get(
                        "last_seen"
                    ),
                "latency_ms": None,
            }

    def url(
        self,
        node_id: str,
        path: str,
    ) -> str:
        return (
            self.settings.cluster_nodes[
                node_id
            ]
            + path
        )

    def is_healthy(
        self,
        node_id: str,
    ) -> bool:
        if (
            node_id
            == self.settings.node_id
        ):
            return True

        return bool(
            self.health.get(
                node_id,
                {},
            ).get(
                "healthy",
                False,
            )
        )

    async def get(
        self,
        node_id: str,
        path: str,
    ) -> httpx.Response:
        return await self.client.get(
            self.url(
                node_id,
                path,
            )
        )

    async def post(
        self,
        node_id: str,
        path: str,
        json: dict | None = None,
    ) -> httpx.Response:
        return await self.client.post(
            self.url(
                node_id,
                path,
            ),
            json=json,
        )

    async def delete(
        self,
        node_id: str,
        path: str,
    ) -> httpx.Response:
        return await self.client.delete(
            self.url(
                node_id,
                path,
            )
        )