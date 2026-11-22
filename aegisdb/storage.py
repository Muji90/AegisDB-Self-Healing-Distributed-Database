from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class StorageEngine:
    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.lock = threading.RLock()

        self.connection = sqlite3.connect(
            str(database_path),
            check_same_thread=False,
        )

        self.connection.row_factory = sqlite3.Row

        self._create_tables()

    def _create_tables(self) -> None:
        with self.lock:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    vector_clock TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    tombstone INTEGER NOT NULL DEFAULT 0
                )
                """
            )

            self.connection.commit()

    def put(
        self,
        key: str,
        value: Any,
        vector_clock: dict[str, int],
        updated_at: float,
        tombstone: bool = False,
    ) -> None:
        value_json = json.dumps(value)

        clock_json = json.dumps(
            vector_clock,
            sort_keys=True,
        )

        with self.lock:
            self.connection.execute(
                """
                INSERT INTO kv_store (
                    key,
                    value,
                    vector_clock,
                    updated_at,
                    tombstone
                )
                VALUES (?, ?, ?, ?, ?)

                ON CONFLICT(key)
                DO UPDATE SET
                    value = excluded.value,
                    vector_clock = excluded.vector_clock,
                    updated_at = excluded.updated_at,
                    tombstone = excluded.tombstone
                """,
                (
                    key,
                    value_json,
                    clock_json,
                    updated_at,
                    int(tombstone),
                ),
            )

            self.connection.commit()

    def get(
        self,
        key: str,
    ) -> dict | None:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT *
                FROM kv_store
                WHERE key = ?
                """,
                (key,),
            ).fetchone()

        if row is None:
            return None

        return self._row_to_dict(row)

    def delete_physical(
        self,
        key: str,
    ) -> bool:
        with self.lock:
            cursor = self.connection.execute(
                """
                DELETE FROM kv_store
                WHERE key = ?
                """,
                (key,),
            )

            self.connection.commit()

            return cursor.rowcount > 0

    def list_records(
        self,
        limit: int = 100,
    ) -> list[dict]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT *
                FROM kv_store
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            self._row_to_dict(row)
            for row in rows
        ]

    def count(self) -> int:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM kv_store
                WHERE tombstone = 0
                """
            ).fetchone()

        return int(row["total"])

    def _row_to_dict(
        self,
        row: sqlite3.Row,
    ) -> dict:
        return {
            "key": row["key"],
            "value": json.loads(
                row["value"]
            ),
            "vector_clock": json.loads(
                row["vector_clock"]
            ),
            "updated_at": row["updated_at"],
            "tombstone": bool(
                row["tombstone"]
            ),
        }