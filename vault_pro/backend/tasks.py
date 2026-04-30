"""In-memory task state machine for collection jobs.

A task progresses through:
    pending  -> running -> done    (success path)
                        -> failed  (exception path)

The collected items are kept on the task object until the task is GC'd
(after a TTL since completion). Frontend polls GET /collect/{task_id} to
read status / progress / items.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional


TaskStatus = str  # "pending" | "running" | "done" | "failed"


@dataclass
class Task:
    id: str
    status: TaskStatus = "pending"
    progress: float = 0.0  # 0.0 - 1.0
    message: str = ""
    items: List[Dict[str, Any]] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "progress": round(self.progress, 3),
            "message": self.message,
            "items": self.items if self.status in ("done", "failed") else [],
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class TaskRegistry:
    """Tracks live tasks. Thread/async safe via a single asyncio.Lock."""

    TTL_SECONDS = 60 * 30  # keep finished tasks for 30 min

    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def create(self) -> Task:
        async with self._lock:
            task = Task(id=uuid.uuid4().hex[:12])
            self._tasks[task.id] = task
            self._gc_locked()
            return task

    async def get(self, task_id: str) -> Optional[Task]:
        async with self._lock:
            return self._tasks.get(task_id)

    async def update(
        self,
        task_id: str,
        *,
        status: Optional[TaskStatus] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        items: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            if status is not None:
                task.status = status
                if status in ("done", "failed"):
                    task.finished_at = time.time()
            if progress is not None:
                task.progress = max(0.0, min(1.0, progress))
            if message is not None:
                task.message = message
            if items is not None:
                task.items = items

    def _gc_locked(self) -> None:
        now = time.time()
        expired = [
            tid
            for tid, t in self._tasks.items()
            if t.finished_at and now - t.finished_at > self.TTL_SECONDS
        ]
        for tid in expired:
            self._tasks.pop(tid, None)


registry = TaskRegistry()


def spawn(
    task: Task,
    coro_factory: Callable[[Task], Awaitable[None]],
) -> None:
    """Start the long-running coroutine in the background.

    The coroutine is responsible for calling registry.update() to report
    progress / final status. We wrap it with a top-level try/except so any
    unhandled exception still flips the task to failed.
    """

    async def _runner() -> None:
        try:
            await registry.update(task.id, status="running", progress=0.0, message="启动中...")
            await coro_factory(task)
            current = await registry.get(task.id)
            if current and current.status == "running":
                await registry.update(task.id, status="done", progress=1.0)
        except Exception as exc:  # noqa: BLE001 - we want to capture all failures
            await registry.update(
                task.id,
                status="failed",
                message=f"{type(exc).__name__}: {exc}",
            )

    asyncio.create_task(_runner())
