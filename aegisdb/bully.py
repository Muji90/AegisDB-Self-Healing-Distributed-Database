from __future__ import annotations

import asyncio
import re
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aegisdb.cluster import ClusterManager
    from aegisdb.config import Settings


class BullyCoordinator:
    FOLLOWER = "FOLLOWER"
    ELECTING = "ELECTING"
    LEADER = "LEADER"

    # Leader sends a heartbeat this often (seconds).
    HEARTBEAT_INTERVAL = 0.6

    # A follower that hears nothing from the leader for this long
    # treats the leader as failed and starts an election.
    LEADER_TIMEOUT = 2.0

    # Second Bully timeout: how long to wait for COORDINATOR after
    # a higher node has answered our ELECTION message.
    COORDINATOR_TIMEOUT = 3.0

    # Delay before the first election after the node starts, so that
    # the other nodes have time to come online.
    STARTUP_DELAY = 1.0

    def __init__(
        self,
        settings: Settings,
        cluster: ClusterManager,
    ):
        self.settings = settings
        self.cluster = cluster

        self.state = self.FOLLOWER

        # Election number. Each new leader uses a higher term than any
        # term it has seen, so leadership changes can be tracked.
        self.current_term = 0
        self.leader_id: str | None = None

        self.elections_started = 0

        self.lock = asyncio.Lock()

        self._running = False
        self._paused = False
        self._electing = False

        self._task: asyncio.Task | None = None
        self._background: set[asyncio.Task] = set()

        self._coordinator_event = asyncio.Event()
        self._last_leader_contact = time.monotonic()

    # ------------------------------------------------------------------
    # Priorities
    # ------------------------------------------------------------------

    @staticmethod
    def priority(node_id: str) -> tuple[int, str]:
        """
        Priority used by the Bully algorithm: the number at the end of the
        node ID (node5 -> 5). The ID string breaks ties, as the lecture
        suggests combining the attribute with the process ID.
        """

        match = re.search(r"(\d+)$", node_id)
        number = int(match.group(1)) if match else -1

        return (number, node_id)

    def _outranks_me(self, node_id: str) -> bool:
        return (
            self.priority(node_id)
            > self.priority(self.settings.node_id)
        )

    def _higher_nodes(self) -> list[str]:
        return [
            node_id
            for node_id in self.settings.cluster_nodes
            if self._outranks_me(node_id)
        ]

    def _other_nodes(self) -> list[str]:
        return [
            node_id
            for node_id in self.settings.cluster_nodes
            if node_id != self.settings.node_id
        ]

    def _spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._paused = False

        # Make the first election happen STARTUP_DELAY seconds from now.
        self._last_leader_contact = (
            time.monotonic()
            - self.LEADER_TIMEOUT
            + self.STARTUP_DELAY
        )

        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False

        tasks = list(self._background)

        if self._task:
            tasks.append(self._task)

        for task in tasks:
            task.cancel()

        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def pause(self) -> None:
        """
        Simulates this node failing: it stops sending and answering
        election messages. The dashboard/admin API stays reachable.
        """

        async with self.lock:
            self._paused = True

            self.state = self.FOLLOWER
            self.leader_id = None

        # Wake an election that may be waiting, so it can stop.
        self._coordinator_event.set()

    async def resume(self) -> None:
        """
        A recovered node rejoins the cluster. As in the Bully algorithm,
        it immediately holds an election: if it has the highest priority
        among live nodes, it takes leadership back ("bullies" the others).
        """

        async with self.lock:
            self._paused = False

            self.state = self.FOLLOWER
            self.leader_id = None
            self._last_leader_contact = time.monotonic()

        self._spawn(self.start_election())

    # ------------------------------------------------------------------
    # Main loop: leader heartbeats and follower failure detection
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        while self._running:

            if self._paused:
                await asyncio.sleep(0.1)
                continue

            if self.state == self.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                continue

            leader_silent_for = (
                time.monotonic() - self._last_leader_contact
            )

            if (
                not self._electing
                and leader_silent_for >= self.LEADER_TIMEOUT
            ):
                await self.start_election()

            await asyncio.sleep(0.1)

    # ------------------------------------------------------------------
    # Running an election
    # ------------------------------------------------------------------

    async def start_election(self) -> None:
        async with self.lock:
            if (
                self._paused
                or self._electing
                or not self._running
                or self.state == self.LEADER
            ):
                return

            self._electing = True
            self.state = self.ELECTING
            self.leader_id = None
            self.elections_started += 1

            self._coordinator_event.clear()

        try:
            while not self._paused:

                higher = self._higher_nodes()

                # Highest-priority node: no one can outrank it.
                if not higher:
                    await self._become_leader()
                    return

                # Send ELECTION to every higher-priority node at once.
                answers = await asyncio.gather(
                    *(self._send_election(node_id) for node_id in higher),
                    return_exceptions=True,
                )

                if self._paused:
                    return

                # First timeout expired with no ANSWER: every higher node
                # is down, so this node wins.
                if not any(answer is True for answer in answers):
                    await self._become_leader()
                    return

                # A higher node answered. Wait for its COORDINATOR message.
                try:
                    await asyncio.wait_for(
                        self._coordinator_event.wait(),
                        self.COORDINATOR_TIMEOUT,
                    )
                    return

                except asyncio.TimeoutError:
                    # Second timeout: the higher node failed before
                    # announcing itself. Start a new election run.
                    self._coordinator_event.clear()

        finally:
            async with self.lock:
                self._electing = False

                if self.state == self.ELECTING:
                    self.state = self.FOLLOWER

                self._last_leader_contact = time.monotonic()

    async def _send_election(self, node_id: str) -> bool:
        """
        Sends ELECTION to a higher node. Returns True if it answered.
        """

        try:
            response = await self.cluster.post(
                node_id,
                "/internal/election/election",
                {
                    "term": self.current_term,
                    "candidate_id": self.settings.node_id,
                },
            )

            if response.status_code != 200:
                return False

            return bool(response.json().get("answer"))

        except Exception:
            return False

    async def _become_leader(self) -> None:
        async with self.lock:
            if self._paused:
                return

            self.current_term += 1

            self.state = self.LEADER
            self.leader_id = self.settings.node_id

            term = self.current_term

        await self._broadcast(
            "/internal/election/coordinator",
            term,
        )

    # ------------------------------------------------------------------
    # Sending COORDINATOR and HEARTBEAT messages
    # ------------------------------------------------------------------

    async def _broadcast(self, path: str, term: int) -> None:
        if self._paused:
            return

        await asyncio.gather(
            *(
                self._send_leader_message(node_id, path, term)
                for node_id in self._other_nodes()
            ),
            return_exceptions=True,
        )

    async def _send_leader_message(
        self,
        node_id: str,
        path: str,
        term: int,
    ) -> None:
        try:
            await self.cluster.post(
                node_id,
                path,
                {
                    "term": term,
                    "leader_id": self.settings.node_id,
                },
            )
        except Exception:
            pass

    async def _send_heartbeats(self) -> None:
        await self._broadcast(
            "/internal/election/heartbeat",
            self.current_term,
        )

    async def _reassert_leadership(self) -> None:
        """
        Used when a lower-priority node claims to be leader while this
        node is already leader: take a new, higher term and re-announce.
        """

        async with self.lock:
            if self._paused or self.state != self.LEADER:
                return

            self.current_term += 1
            term = self.current_term

        await self._broadcast(
            "/internal/election/coordinator",
            term,
        )

    # ------------------------------------------------------------------
    # Handling incoming messages
    # ------------------------------------------------------------------

    async def handle_election(
        self,
        term: int,
        candidate_id: str,
    ) -> dict:
        """
        ELECTION from a lower-priority node. Reply with ANSWER (disagree)
        and start our own election, as the Bully algorithm requires.
        """

        if self._paused:
            return {
                "answer": False,
                "node_id": self.settings.node_id,
            }

        async with self.lock:
            self.current_term = max(self.current_term, term)

        if self._outranks_me(candidate_id):
            # ELECTION is only meant for higher nodes; ignore it.
            return {
                "answer": False,
                "node_id": self.settings.node_id,
            }

        if self.state == self.LEADER:
            # Already leader: remind the candidate who the leader is.
            self._spawn(
                self._broadcast(
                    "/internal/election/coordinator",
                    self.current_term,
                )
            )
        else:
            self._spawn(self.start_election())

        return {
            "answer": True,
            "node_id": self.settings.node_id,
        }

    async def handle_coordinator(
        self,
        term: int,
        leader_id: str,
    ) -> dict:
        return await self._accept_leader(term, leader_id)

    async def handle_heartbeat(
        self,
        term: int,
        leader_id: str,
    ) -> dict:
        return await self._accept_leader(term, leader_id)

    async def _accept_leader(
        self,
        term: int,
        leader_id: str,
    ) -> dict:
        if self._paused:
            return {
                "accepted": False,
                "term": self.current_term,
                "leader_id": self.leader_id,
            }

        if leader_id == self.settings.node_id:
            return {
                "accepted": True,
                "term": self.current_term,
                "leader_id": self.leader_id,
            }

        if self._outranks_me(leader_id):
            # A higher-priority node is leader: follow it.
            async with self.lock:
                self.current_term = max(self.current_term, term)

                self.state = (
                    self.ELECTING
                    if self._electing
                    else self.FOLLOWER
                )
                self.leader_id = leader_id
                self._last_leader_contact = time.monotonic()

            self._coordinator_event.set()

            return {
                "accepted": True,
                "term": self.current_term,
                "leader_id": self.leader_id,
            }

        # A lower-priority node claims leadership while this node is
        # alive: bully it by holding an election (or re-announcing).
        async with self.lock:
            self.current_term = max(self.current_term, term)

        if self.state == self.LEADER:
            self._spawn(self._reassert_leadership())
        else:
            self._spawn(self.start_election())

        return {
            "accepted": False,
            "term": self.current_term,
            "leader_id": self.leader_id,
        }

    # ------------------------------------------------------------------
    # Status for the dashboard
    # ------------------------------------------------------------------

    def status(self) -> dict:
        return {
            "algorithm": "Bully",
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "priority": self.priority(self.settings.node_id)[0],
            "elections_started": self.elections_started,
            "paused": self._paused,
        }