"""FastAPI entry point for Vault PRO backend.

Endpoints:
    GET  /health              - liveness probe used by frontend
    POST /collect             - kick off a collection task, returns {task_id}
    GET  /collect/{task_id}   - poll task progress & items

CORS is intentionally permissive (allow_origin_regex=".*") because the
frontend may be opened via file:// (no Origin header) or http://localhost
on any port. The server only binds to 127.0.0.1, so this is local-only.
"""
from __future__ import annotations

import asyncio
import logging
import json
import os
import random
import re
import socket
import ssl
import subprocess
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import normalize
from .collectors import AnjukeCollector, Community58PriceCollector, Tongcheng58Collector
from .collectors.base import DEFAULT_UA, USER_DATA_ROOT
from .collectors.base import CollectorContext
from .tasks import Task, registry, spawn


app = FastAPI(title="Vault PRO Backend", version="0.1.0")

RUNTIME_LOG_MAX = 800
_runtime_logs = deque(maxlen=RUNTIME_LOG_MAX)


class RuntimeLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        name = record.name or ""
        msg = record.getMessage()
        # Keep logs focused on this app and collectors.
        if not (
            name.startswith("vault_pro")
            or name.startswith("backend")
            or name.startswith("uvicorn")
        ):
            return
        if name.startswith("uvicorn.access") and "/health" in msg:
            return
        _runtime_logs.append(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "name": name,
                "message": msg,
            }
        )


def _install_runtime_log_handler() -> None:
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, RuntimeLogHandler):
            return
    handler = RuntimeLogHandler(level=logging.INFO)
    root.addHandler(handler)
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)


_install_runtime_log_handler()

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


COLLECTOR_FACTORIES = {
    "anjuke": AnjukeCollector,
    "58": Tongcheng58Collector,
}

DEFAULT_PLATFORM_HOSTS = {
    "anjuke": "lvliang.anjuke.com",
    "58": "lvliang.58.com",
}


class CollectRequest(BaseModel):
    platforms: List[str] = Field(default_factory=lambda: ["anjuke", "58"])
    regions: List[str] = Field(default_factory=list)
    target: int = Field(default=80, ge=10, le=100)
    since_days: int = Field(default=1, ge=0, le=1825)
    fixed_window: bool = Field(
        default=True,
        description="True=固定窗口低频模式（单页采集）",
    )
    throttle_seconds: float = Field(
        default=3.0,
        ge=0.0,
        le=30.0,
        description="采集翻页节流秒数（系统会自动附加随机行为抖动）",
    )
    risk_wait_seconds: int = Field(
        default=180,
        ge=60,
        le=1800,
        description="验证码/风控等待秒数",
    )
    platform_hosts: Dict[str, str] = Field(
        default_factory=dict,
        description="平台域名覆盖，如 {'anjuke':'lvliang.anjuke.com'}",
    )
    auto_downgrade_58_ephemeral: bool = Field(
        default=True,
        description="58 启动失败时自动降级到无持久会话并重试一次",
    )
    manual_region_confirm_58: bool = Field(
        default=True,
        description="58 采集前是否要求人工在页面先选区并确认开始",
    )
    anti_detection_level: str = Field(
        default="medium",
        description="反反爬强度：low/medium/high",
    )
    anti_detection_mode: bool = Field(
        default=True,
        description="兼容旧版本开关（建议改用 anti_detection_level）",
    )
    exclude_hash_keys: List[str] = Field(
        default_factory=list,
        description="前端已入库记录 hashKey 列表（采集时自动过滤且不计入数量）",
    )
    exclude_source_urls: List[str] = Field(
        default_factory=list,
        description="前端已入库记录 sourceUrl 列表（采集时自动过滤且不计入数量）",
    )


class CollectResponse(BaseModel):
    task_id: str


class CommunityPriceCollectRequest(BaseModel):
    start_url: str = Field(default="https://lvliang.58.com/xiaoqu/")
    max_pages: int = Field(default=6, ge=1, le=20)
    risk_wait_seconds: int = Field(default=180, ge=60, le=1800)
    anti_detection_level: str = Field(default="medium", description="low/medium/high")


class CommunityPriceCollectResponse(BaseModel):
    task_id: str


class PlatformCheckItem(BaseModel):
    platform: str
    display_name: str
    status: str
    message: str


class RuntimeLogResponse(BaseModel):
    items: List[dict]


class DecorationRepairItem(BaseModel):
    id: str
    sourceUrl: str = Field(default="")
    platform: str = Field(default="")


class DecorationRepairRequest(BaseModel):
    items: List[DecorationRepairItem] = Field(default_factory=list)
    timeout_seconds: int = Field(default=18, ge=8, le=60)


class DecorationRepairResultItem(BaseModel):
    id: str
    decoration: str
    decoration_raw: str = Field(default="")
    sourceUrl: str


class DecorationRepairFailureItem(BaseModel):
    id: str
    sourceUrl: str
    reason: str


class DecorationRepairResponse(BaseModel):
    scanned: int
    updated: List[DecorationRepairResultItem]
    failed: List[DecorationRepairFailureItem]


class RecordRepairRequest(BaseModel):
    item: DecorationRepairItem
    timeout_seconds: int = Field(default=20, ge=8, le=80)


class RecordRepairResultItem(BaseModel):
    id: str
    sourceUrl: str
    totalPrice: Optional[float] = None
    unitPrice: Optional[int] = None
    area: Optional[float] = None
    layout: str = Field(default="")
    decoration: str = Field(default="")
    decoration_raw: str = Field(default="")
    orientation: str = Field(default="")
    floor: str = Field(default="")


class RecordRepairResponse(BaseModel):
    ok: bool
    updated: Optional[RecordRepairResultItem] = None
    reason: str = Field(default="")
    reason_code: str = Field(default="")


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "name": "vault_pro_backend", "version": "0.1.0"}


@app.get("/platform_check")
async def platform_check(platforms: Optional[str] = None, hosts: Optional[str] = None) -> dict:
    requested = [p.strip() for p in (platforms or "").split(",") if p.strip()]
    chosen = requested or list(COLLECTOR_FACTORIES.keys())
    chosen = [p for p in chosen if p in COLLECTOR_FACTORIES]
    if not chosen:
        raise HTTPException(status_code=400, detail="未选择有效平台 (anjuke/58)")
    host_overrides = _parse_platform_hosts_query(hosts)
    resolved_hosts = _resolve_platform_hosts(host_overrides)

    results: List[PlatformCheckItem] = []
    for platform in chosen:
        cls = COLLECTOR_FACTORIES[platform]
        name = getattr(cls, "display_name", platform)
        host = resolved_hosts.get(platform) or DEFAULT_PLATFORM_HOSTS.get(platform, "")
        err = _precheck_platform_tls(platform, host=host)
        if err:
            results.append(
                PlatformCheckItem(
                    platform=platform,
                    display_name=name,
                    status="down",
                    message=f"连接失败（{host}）：{err}",
                )
            )
            continue
        if platform == "58":
            results.append(
                PlatformCheckItem(
                    platform=platform,
                    display_name=name,
                    status="warn",
                    message=f"连接正常（{host}），但很可能触发验证码/人机验证",
                )
            )
            continue
        results.append(
            PlatformCheckItem(
                platform=platform,
                display_name=name,
                status="ok",
                message=f"连接正常（{host}），可开始采集",
            )
        )
    return {"items": [x.model_dump() for x in results]}


@app.get("/runtime_logs", response_model=RuntimeLogResponse)
async def runtime_logs(limit: int = 200) -> RuntimeLogResponse:
    n = max(20, min(800, int(limit)))
    return RuntimeLogResponse(items=list(_runtime_logs)[-n:])


@app.post("/collect", response_model=CollectResponse)
async def collect(req: CollectRequest) -> CollectResponse:
    chosen: List[str] = [p for p in req.platforms if p in COLLECTOR_FACTORIES]
    if not chosen:
        raise HTTPException(status_code=400, detail="未选择有效平台 (anjuke/58)")

    task: Task = await registry.create()
    resolved_hosts = _resolve_platform_hosts(req.platform_hosts or {})
    exclude_hash_keys = {
        str(x or "").strip()
        for x in (req.exclude_hash_keys or [])
        if str(x or "").strip()
    }
    exclude_source_urls = {
        str(x or "").strip()
        for x in (req.exclude_source_urls or [])
        if str(x or "").strip()
    }

    async def runner(t: Task) -> None:
        ctx = CollectorContext(
            target=req.target,
            regions=req.regions or [],
            since_days=req.since_days,
            fixed_window=req.fixed_window,
            throttle_seconds=req.throttle_seconds,
            risk_wait_seconds=req.risk_wait_seconds,
            manual_region_confirm_58=req.manual_region_confirm_58,
            anti_detection_level=req.anti_detection_level,
            anti_detection_mode=req.anti_detection_mode,
            exclude_hash_keys=exclude_hash_keys,
            exclude_source_urls=exclude_source_urls,
            on_progress=_make_progress_callback(t.id),
        )

        all_items: List[dict] = []
        platform_failures: List[str] = []
        per_platform_quota = max(10, req.target // max(1, len(chosen)))

        for idx, platform in enumerate(chosen):
            if len(all_items) >= req.target:
                break
            host = resolved_hosts.get(platform) or DEFAULT_PLATFORM_HOSTS.get(platform, "")
            preflight_err = _precheck_platform_tls(platform, host=host)
            if preflight_err:
                name = platform
                cls = COLLECTOR_FACTORIES[platform]
                try:
                    name = cls.display_name
                except Exception:
                    pass
                platform_failures.append(f"{name}: 站点连接失败（{host}，{preflight_err}）")
                continue
            cls = COLLECTOR_FACTORIES[platform]
            collector = cls()
            collector.set_runtime_host(host)
            remaining = req.target - len(all_items)
            try:
                await registry.update(
                    t.id,
                    message=f"[{idx + 1}/{len(chosen)}] 正在采集 {collector.display_name}（{host}）...",
                )
                items = await collector.collect(
                    ctx, max_items=min(remaining, per_platform_quota * 2)
                )
                all_items.extend(items)
            except Exception as exc:  # noqa: BLE001
                if (
                    platform == "58"
                    and req.auto_downgrade_58_ephemeral
                    and _is_browser_startup_failure(exc)
                ):
                    try:
                        await registry.update(
                            t.id,
                            message=(
                                f"[{idx + 1}/{len(chosen)}] {collector.display_name} 启动异常，"
                                "正在降级为无持久会话模式重试一次..."
                            ),
                        )
                        collector.set_force_ephemeral_profile(True)
                        items = await collector.collect(
                            ctx, max_items=min(remaining, per_platform_quota * 2)
                        )
                        all_items.extend(items)
                        continue
                    except Exception as retry_exc:  # noqa: BLE001
                        platform_failures.append(
                            f"{collector.display_name}: 首次失败({exc})；降级重试仍失败({retry_exc})"
                        )
                        continue
                platform_failures.append(f"{collector.display_name}: {exc}")

        if not all_items and platform_failures:
            raise RuntimeError("; ".join(platform_failures))

        if not all_items:
            await registry.update(
                t.id,
                status="done",
                progress=1.0,
                message="今日无更新（可在前端二次确认扩大时间范围）",
                items=[],
            )
            return

        all_items = all_items[: req.target]
        msg = f"完成：共 {len(all_items)} 条"
        if platform_failures:
            msg += f"；部分平台失败：{'; '.join(platform_failures)}"
        await registry.update(
            t.id,
            status="done",
            progress=1.0,
            message=msg,
            items=all_items,
        )

    spawn(task, runner)
    return CollectResponse(task_id=task.id)


@app.get("/collect/{task_id}")
async def collect_status(task_id: str) -> dict:
    task = await registry.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found or expired")
    return task.to_dict()


@app.post("/community_price_collect", response_model=CommunityPriceCollectResponse)
async def community_price_collect(req: CommunityPriceCollectRequest) -> CommunityPriceCollectResponse:
    task: Task = await registry.create()

    async def runner(t: Task) -> None:
        collector = Community58PriceCollector()
        await registry.update(t.id, message=f"[小区均价] 启动采集（最多 {req.max_pages} 页）...")
        items = await collector.collect_prices(
            start_url=req.start_url,
            max_pages=req.max_pages,
            risk_wait_seconds=req.risk_wait_seconds,
            anti_detection_level=req.anti_detection_level,
            on_progress=_make_progress_callback(t.id),
        )
        msg = f"完成：共采集 {len(items)} 条小区均价"
        await registry.update(
            t.id,
            status="done",
            progress=1.0,
            message=msg,
            items=items,
        )

    spawn(task, runner)
    return CommunityPriceCollectResponse(task_id=task.id)


@app.get("/community_price_collect/{task_id}")
async def community_price_collect_status(task_id: str) -> dict:
    task = await registry.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found or expired")
    return task.to_dict()


@app.post("/shutdown")
async def shutdown() -> dict:
    async def _exit_later() -> None:
        await asyncio.sleep(0.2)
        os._exit(0)

    asyncio.create_task(_exit_later())
    return {"ok": True, "message": "backend shutting down"}


@app.get("/oneclick/status")
async def oneclick_status() -> dict:
    supported = os.name == "nt"
    return {"supported": supported, "registered": _is_vaultpro_protocol_registered() if supported else False}


@app.post("/oneclick/register")
async def oneclick_register() -> dict:
    if os.name != "nt":
        raise HTTPException(status_code=400, detail="仅 Windows 支持一键连接安装")
    ok, message = _register_vaultpro_protocol()
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return {"ok": True, "message": message}


@app.post("/repair/decorations", response_model=DecorationRepairResponse)
async def repair_decorations(req: DecorationRepairRequest) -> DecorationRepairResponse:
    items = [x for x in req.items if (x.sourceUrl or "").strip()]
    if not items:
        return DecorationRepairResponse(scanned=0, updated=[], failed=[])
    # Keep batch small to avoid long single requests.
    capped = items[:120]
    updated, failed = await _repair_decorations_from_urls(
        capped, timeout_seconds=req.timeout_seconds
    )
    return DecorationRepairResponse(
        scanned=len(capped),
        updated=updated,
        failed=failed,
    )


@app.post("/repair/record", response_model=RecordRepairResponse)
async def repair_record(req: RecordRepairRequest) -> RecordRepairResponse:
    item = req.item
    source_url = (item.sourceUrl or "").strip()
    if not source_url:
        return RecordRepairResponse(ok=False, reason="缺少 sourceUrl", reason_code="invalid_url")
    if not source_url.startswith(("http://", "https://")):
        return RecordRepairResponse(ok=False, reason="sourceUrl 非 http(s) 地址", reason_code="invalid_url")
    updated, reason, reason_code = await _repair_single_record_from_url(
        item,
        timeout_seconds=req.timeout_seconds,
    )
    if not updated:
        return RecordRepairResponse(
            ok=False,
            reason=reason or "未识别到可补齐字段",
            reason_code=reason_code or "unknown",
        )
    return RecordRepairResponse(ok=True, updated=updated, reason="", reason_code="")


def _make_progress_callback(task_id: str):
    async def _update(progress: float, message: Optional[str] = None) -> None:
        await registry.update(task_id, progress=progress, message=message)

    return _update


def _precheck_platform_tls(platform: str, host: Optional[str] = None) -> Optional[str]:
    host = host or DEFAULT_PLATFORM_HOSTS.get(platform)
    if not host:
        return None
    try:
        with socket.create_connection((host, 443), timeout=8) as sock:
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(sock, server_hostname=host):
                return None
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "getaddrinfo failed" in msg.lower():
            return f"域名解析失败（{host} 可能无独立站点）"
        return msg


_HOST_RE = re.compile(r"^[a-z0-9.-]+$")


def _sanitize_host(raw: str) -> Optional[str]:
    txt = (raw or "").strip()
    if not txt:
        return None
    parsed = urlparse(txt if "://" in txt else f"https://{txt}")
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if not host:
        return None
    host = host.split("/")[0].split("?")[0]
    if ":" in host:
        host = host.split(":", 1)[0]
    if "." not in host:
        return None
    if not _HOST_RE.fullmatch(host):
        return None
    return host


def _resolve_platform_hosts(overrides: Dict[str, str]) -> Dict[str, str]:
    resolved = dict(DEFAULT_PLATFORM_HOSTS)
    for platform, raw_host in (overrides or {}).items():
        if platform not in resolved:
            continue
        host = _sanitize_host(raw_host)
        if host:
            resolved[platform] = host
    return resolved


def _parse_platform_hosts_query(hosts: Optional[str]) -> Dict[str, str]:
    if not hosts:
        return {}
    try:
        data = json.loads(hosts)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def _is_vaultpro_protocol_registered() -> bool:
    if os.name != "nt":
        return False
    try:
        import winreg  # type: ignore

        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Classes\vaultpro\shell\open\command",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "")
            return bool(str(value or "").strip())
    except Exception:
        return False


def _register_vaultpro_protocol() -> tuple[bool, str]:
    script = Path(__file__).resolve().parent.parent / "register_vaultpro_protocol.ps1"
    if not script.exists():
        return False, f"未找到安装脚本：{script}"
    try:
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script),
            ],
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"执行安装脚本失败：{exc}"

    output = "\n".join(
        [x for x in [proc.stdout.strip(), proc.stderr.strip()] if x]
    ).strip()
    if proc.returncode != 0:
        return False, output or f"安装脚本退出码 {proc.returncode}"
    if not _is_vaultpro_protocol_registered():
        return False, output or "安装脚本执行成功，但未检测到协议注册"
    return True, output or "安装成功"


def _is_browser_startup_failure(exc: Exception) -> bool:
    msg = str(exc).lower()
    markers = (
        "target page, context or browser has been closed",
        "browser has been closed",
        "browser closed",
        "failed to launch",
        "process did exit",
        "exitcode",
    )
    return any(m in msg for m in markers)


_BLOCK_PAGE_HINTS = (
    "疑似使用网页抓取工具",
    "访问频繁",
    "人机验证",
    "安全验证",
    "验证码",
    "系统检测到",
    "请卸载删除后访问",
    "秒后自动为您返回",
    "ip:",
)

_LOGIN_PAGE_HINTS = (
    "请先登录",
    "登录后查看",
    "扫码登录",
    "去登录",
)


def _is_block_or_verify_page(text: str) -> bool:
    cleaned = re.sub(r"\s+", "", str(text or "")).lower()
    if not cleaned:
        return False
    return any(h.lower() in cleaned for h in _BLOCK_PAGE_HINTS)


def _is_login_required_page(text: str) -> bool:
    cleaned = re.sub(r"\s+", "", str(text or ""))
    if not cleaned:
        return False
    return any(h in cleaned for h in _LOGIN_PAGE_HINTS)


_DECO_CONTEXT_RE = re.compile(
    r"(?:装修|装潢|装饰|交房|交付|房屋状态|房源亮点)[^。；\n]{0,32}"
)
_DECO_RAW_CONTEXT_RE = re.compile(
    r"(?:装修|装潢|装饰|交房标准|交付标准)\s*[:：]?\s*([^\s，。；|/]{1,12})"
)
_DECO_RAW_TOKEN_RE = re.compile(
    r"(精装修?|豪华装修|高档装修|豪装|精装|简装修?|普通装修|中装|普装|毛坯房?|毛胚房?|毛坏|清水房?|未装修|无装修|拎包入住|带装修|有装修)"
)
_LAYOUT_TOKEN_RE = re.compile(r"(\d+\s*室\s*\d+\s*厅(?:\s*\d+\s*卫)?)")
_ORIENTATION_TOKEN_RE = re.compile(r"(南北通透|南北|东西|东南|东北|西南|西北|朝南|朝北|朝东|朝西)")
_FLOOR_TOKEN_RE = re.compile(
    r"(\d+\s*/\s*\d+|(?:低|中|高|底|顶|地下)(?:楼)?层(?:\s*\(\s*共?\d+层\s*\))?|\d+层(?:\s*\(\s*共?\d+层\s*\))?)"
)


def _pick_first_parsed_text(
    fragments: List[str],
    parser,
    *,
    skip_values: tuple[str, ...] = ("", "未知"),
) -> str:
    for raw in fragments:
        parsed = str(parser(str(raw or "")) or "").strip()
        if parsed and parsed not in skip_values:
            return parsed
    merged = " ".join(x for x in fragments if x)
    parsed = str(parser(merged) or "").strip()
    if parsed and parsed not in skip_values:
        return parsed
    return ""


def _pick_first_parsed_num(fragments: List[str], parser):
    for raw in fragments:
        parsed = parser(str(raw or ""))
        if parsed is None:
            continue
        if isinstance(parsed, (int, float)) and parsed > 0:
            return parsed
    merged = " ".join(x for x in fragments if x)
    parsed = parser(merged)
    if isinstance(parsed, (int, float)) and parsed > 0:
        return parsed
    return None


def _extract_record_meta_from_fragments(fragments: List[str]) -> dict:
    merged = " ".join(x for x in fragments if x)
    layout = ""
    for raw in fragments + [merged]:
        txt = str(raw or "")
        if not txt:
            continue
        m = _LAYOUT_TOKEN_RE.search(txt)
        if m:
            layout = normalize.parse_layout(m.group(1))
            if layout:
                break

    orientation = ""
    for raw in fragments + [merged]:
        txt = str(raw or "")
        if not txt:
            continue
        m = _ORIENTATION_TOKEN_RE.search(txt)
        if not m:
            continue
        orientation = normalize.parse_orientation(m.group(1).replace("朝", ""))
        if orientation:
            break
    if not orientation:
        orientation = _pick_first_parsed_text(fragments, normalize.parse_orientation)

    floor = ""
    for raw in fragments + [merged]:
        txt = str(raw or "")
        if not txt:
            continue
        m = _FLOOR_TOKEN_RE.search(txt)
        if not m:
            continue
        floor = normalize.parse_floor(m.group(1))
        if floor and floor != "未知":
            break
    if not floor:
        floor = _pick_first_parsed_text(fragments, normalize.parse_floor)

    deco, deco_raw = _extract_decoration_from_text_fragments(fragments)
    total_price = _pick_first_parsed_num(fragments, normalize.parse_total_price)
    unit_price = _pick_first_parsed_num(fragments, normalize.parse_unit_price)
    area = _pick_first_parsed_num(fragments, normalize.parse_area)

    return {
        "layout": layout,
        "orientation": orientation,
        "floor": floor if floor != "未知" else "",
        "decoration": deco,
        "decoration_raw": deco_raw or deco,
        "totalPrice": round(float(total_price), 1) if total_price else None,
        "unitPrice": int(unit_price) if unit_price else None,
        "area": round(float(area), 1) if area else None,
    }


def _extract_decoration_raw_text(fragments: List[str]) -> str:
    for raw in fragments:
        txt = str(raw or "")
        if not txt:
            continue
        m_ctx = _DECO_RAW_CONTEXT_RE.search(txt)
        if m_ctx:
            return m_ctx.group(1).strip()
        m_token = _DECO_RAW_TOKEN_RE.search(txt)
        if m_token:
            return m_token.group(1).strip()
    merged = " ".join(x for x in fragments if x)
    m_ctx = _DECO_RAW_CONTEXT_RE.search(merged)
    if m_ctx:
        return m_ctx.group(1).strip()
    m_token = _DECO_RAW_TOKEN_RE.search(merged)
    if m_token:
        return m_token.group(1).strip()
    return ""


def _extract_decoration_from_text_fragments(fragments: List[str]) -> tuple[str, str]:
    for raw in fragments:
        deco = normalize.parse_decoration(raw or "")
        if deco:
            return deco, _extract_decoration_raw_text([raw])
    merged = " ".join(x for x in fragments if x)
    if not merged:
        return "", ""
    for m in _DECO_CONTEXT_RE.finditer(merged):
        snippet = m.group(0)
        deco = normalize.parse_decoration(snippet)
        if deco:
            raw = _extract_decoration_raw_text([snippet]) or snippet.strip()[:16]
            return deco, raw
    deco = normalize.parse_decoration(merged)
    raw = _extract_decoration_raw_text([merged])
    return deco, raw


def _is_retryable_repair_reason(reason: str) -> bool:
    txt = str(reason or "")
    return bool(re.search(r"风控|反爬|访问频繁|人机验证|验证码|登录", txt, re.IGNORECASE))


def _classify_record_repair_reason_code(reason: str) -> str:
    txt = str(reason or "")
    if not txt:
        return "unknown"
    if re.search(r"缺少\s*sourceurl|非\s*http", txt, re.IGNORECASE):
        return "invalid_url"
    if re.search(r"风控|反爬|访问频繁|人机验证|验证码|拦截", txt, re.IGNORECASE):
        return "blocked"
    if re.search(r"登录|请先登录|登录后", txt, re.IGNORECASE):
        return "login_required"
    if re.search(r"未识别|未识别到可用字段|未命中", txt, re.IGNORECASE):
        return "not_found"
    if re.search(r"timeout|超时", txt, re.IGNORECASE):
        return "timeout"
    if re.search(r"playwright", txt, re.IGNORECASE):
        return "runtime_error"
    return "unknown"


async def _wait_for_manual_unblock(page: object, *, max_wait_seconds: int) -> bool:
    """Give user a visible-window interval to finish captcha/login once."""
    wait_seconds = max(10, int(max_wait_seconds or 0))
    for _ in range(wait_seconds):
        try:
            snapshot = await page.evaluate(
                """
                () => {
                  const body = (document.body && document.body.innerText) ? document.body.innerText : '';
                  return {
                    title: document.title || '',
                    body: body.slice(0, 12000),
                    url: location.href || '',
                  };
                }
                """
            )
            inspect_text = " ".join(
                [
                    str((snapshot or {}).get("title") or ""),
                    str((snapshot or {}).get("url") or ""),
                    str((snapshot or {}).get("body") or ""),
                ]
            )
        except Exception:  # noqa: BLE001
            inspect_text = ""

        if inspect_text and not _is_block_or_verify_page(inspect_text) and not _is_login_required_page(inspect_text):
            return True
        await asyncio.sleep(1)
    return False


def _normalize_repair_platform(raw_platform: str, source_url: str) -> str:
    """Resolve collector key from payload platform or URL host."""
    raw = str(raw_platform or "").strip().lower()
    alias_map = {
        "58": "58",
        "58.com": "58",
        "58同城": "58",
        "tongcheng58": "58",
        "anjuke": "anjuke",
        "安居客": "anjuke",
    }
    if raw in COLLECTOR_FACTORIES:
        return raw
    if raw in alias_map:
        return alias_map[raw]

    host = ""
    try:
        host = str(urlparse(str(source_url or "")).hostname or "").lower()
    except Exception:  # noqa: BLE001
        host = ""

    if host.endswith("58.com"):
        return "58"
    if host.endswith("anjuke.com"):
        return "anjuke"
    return ""


async def _extract_decoration_for_url(
    page: object,
    *,
    source_url: str,
    timeout_ms: int,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=timeout_ms)
        html_text = ""
        try:
            # Some sites render key labels in scripts/SSR payloads, not visible text.
            html_text = await page.content()
        except Exception:  # noqa: BLE001
            html_text = ""
        page_data = await page.evaluate(
            """
            () => {
              const pick = (sel) =>
                Array.from(document.querySelectorAll(sel))
                  .map(el => (el && el.innerText) ? el.innerText : '')
                  .filter(Boolean)
                  .join(' ');
              const meta = Array.from(
                document.querySelectorAll('meta[name="description"], meta[property="og:description"], meta[name="keywords"]')
              ).map(el => el.content || '').filter(Boolean).join(' ');
              const structured = Array.from(
                document.querySelectorAll('script[type="application/ld+json"], script#__NEXT_DATA__, script[id*="__NUXT"]')
              ).map(el => (el && el.textContent) ? el.textContent.slice(0, 12000) : '').filter(Boolean).join(' ');
              const body = (document.body && document.body.innerText) ? document.body.innerText : '';
              return {
                title: document.title || '',
                focus: pick('.houseInfo, .baseinfo, .property-content-info-text, .property-content-info, .details-item, .house-desc, .desc, .house-title, .house-label, .house-tag, .base, .overview, .info'),
                meta,
                body,
                structured,
              };
            }
            """
        )
        title = str((page_data or {}).get("title") or "")
        focus = str((page_data or {}).get("focus") or "")
        meta = str((page_data or {}).get("meta") or "")
        body = str((page_data or {}).get("body") or "")
        structured = str((page_data or {}).get("structured") or "")
        inspect_text = " ".join([title, focus, meta, body[:16000], structured[:16000], html_text[:24000]])
        if _is_block_or_verify_page(inspect_text):
            return None, None, "疑似触发网站风控/反爬拦截（请切换网络、降低频率后重试）"
        if _is_login_required_page(inspect_text):
            return None, None, "页面需要登录后才能识别装修信息"
        deco, deco_raw = _extract_decoration_from_text_fragments(
            [focus, meta, body[:22000], structured[:32000], html_text[:64000], title]
        )
        if deco:
            return deco, (deco_raw or deco), None
        return None, None, "页面中未识别到装修关键词"
    except Exception as exc:  # noqa: BLE001
        return None, None, str(exc)


async def _extract_record_meta_for_url(
    page: object,
    *,
    source_url: str,
    timeout_ms: int,
) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    try:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=timeout_ms)
        html_text = ""
        try:
            html_text = await page.content()
        except Exception:  # noqa: BLE001
            html_text = ""
        page_data = await page.evaluate(
            """
            () => {
              const pick = (sel) =>
                Array.from(document.querySelectorAll(sel))
                  .map(el => (el && el.innerText) ? el.innerText : '')
                  .filter(Boolean)
                  .join(' ');
              const meta = Array.from(
                document.querySelectorAll('meta[name="description"], meta[property="og:description"], meta[name="keywords"]')
              ).map(el => el.content || '').filter(Boolean).join(' ');
              const structured = Array.from(
                document.querySelectorAll('script[type="application/ld+json"], script#__NEXT_DATA__, script[id*="__NUXT"]')
              ).map(el => (el && el.textContent) ? el.textContent.slice(0, 16000) : '').filter(Boolean).join(' ');
              const body = (document.body && document.body.innerText) ? document.body.innerText : '';
              const focus = pick('.houseInfo, .baseinfo, .property-content-info-text, .property-content-info, .details-item, .house-desc, .desc, .house-title, .house-label, .house-tag, .base, .overview, .info, .main-info, .baseInfo, .houseInfo-wrap, .basic-info');
              const pricing = pick('.price, .house-price, .price-container, .price-box, .unit-price, .overview .price, .main-info-price, .sum');
              return {
                title: document.title || '',
                focus,
                pricing,
                meta,
                body,
                structured,
              };
            }
            """
        )
        title = str((page_data or {}).get("title") or "")
        focus = str((page_data or {}).get("focus") or "")
        pricing = str((page_data or {}).get("pricing") or "")
        meta = str((page_data or {}).get("meta") or "")
        body = str((page_data or {}).get("body") or "")
        structured = str((page_data or {}).get("structured") or "")
        inspect_text = " ".join(
            [title, pricing, focus, meta, body[:16000], structured[:16000], html_text[:24000]]
        )
        if _is_block_or_verify_page(inspect_text):
            return None, "疑似触发网站风控/反爬拦截（请切换网络、降低频率后重试）", "blocked"
        if _is_login_required_page(inspect_text):
            return None, "页面需要登录后才能识别房源信息", "login_required"

        fragments = [pricing, focus, meta, body[:26000], structured[:36000], html_text[:64000], title]
        parsed = _extract_record_meta_from_fragments(fragments)
        if any(
            [
                parsed.get("layout"),
                parsed.get("orientation"),
                parsed.get("floor"),
                parsed.get("decoration"),
                parsed.get("totalPrice"),
                parsed.get("unitPrice"),
                parsed.get("area"),
            ]
        ):
            return parsed, None, None
        return None, "页面中未识别到可用字段", "not_found"
    except Exception as exc:  # noqa: BLE001
        reason = str(exc)
        return None, reason, _classify_record_repair_reason_code(reason)


async def _repair_single_record_from_url(
    item: DecorationRepairItem,
    *,
    timeout_seconds: int,
) -> tuple[Optional[RecordRepairResultItem], Optional[str], Optional[str]]:
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        reason = f"Playwright 不可用：{exc}"
        return None, reason, "runtime_error"

    timeout_ms = max(8000, int(timeout_seconds * 1000))
    source_url = str(item.sourceUrl or "").strip()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(user_agent=DEFAULT_UA)
        page = await context.new_page()
        try:
            parsed, reason, reason_code = await _extract_record_meta_for_url(
                page,
                source_url=source_url,
                timeout_ms=timeout_ms,
            )
            if parsed:
                return (
                    RecordRepairResultItem(
                        id=item.id,
                        sourceUrl=source_url,
                        totalPrice=parsed.get("totalPrice"),
                        unitPrice=parsed.get("unitPrice"),
                        area=parsed.get("area"),
                        layout=str(parsed.get("layout") or ""),
                        decoration=str(parsed.get("decoration") or ""),
                        decoration_raw=str(parsed.get("decoration_raw") or ""),
                        orientation=str(parsed.get("orientation") or ""),
                        floor=str(parsed.get("floor") or ""),
                    ),
                    None,
                    None,
                )
            final_reason = reason or "页面中未识别到可用字段"
            return None, final_reason, (reason_code or _classify_record_repair_reason_code(final_reason))
        finally:
            await context.close()
            await browser.close()

async def _repair_decorations_from_urls(
    items: List[DecorationRepairItem],
    *,
    timeout_seconds: int,
) -> tuple[List[DecorationRepairResultItem], List[DecorationRepairFailureItem]]:
    updated: List[DecorationRepairResultItem] = []
    failed: List[DecorationRepairFailureItem] = []
    if not items:
        return updated, failed

    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        reason = f"Playwright 不可用：{exc}"
        return (
            updated,
            [
                DecorationRepairFailureItem(
                    id=x.id,
                    sourceUrl=x.sourceUrl,
                    reason=reason,
                )
                for x in items
            ],
        )

    timeout_ms = max(8000, int(timeout_seconds * 1000))
    item_map = {(str(x.id), str(x.sourceUrl or "").strip()): x for x in items}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(user_agent=DEFAULT_UA)
        page = await context.new_page()
        try:
            for item in items:
                source_url = (item.sourceUrl or "").strip()
                if not source_url:
                    failed.append(
                        DecorationRepairFailureItem(
                            id=item.id,
                            sourceUrl=source_url,
                            reason="缺少 sourceUrl",
                        )
                    )
                    continue
                if not source_url.startswith(("http://", "https://")):
                    failed.append(
                        DecorationRepairFailureItem(
                            id=item.id,
                            sourceUrl=source_url,
                            reason="sourceUrl 非 http(s) 地址",
                        )
                    )
                    continue
                deco, deco_raw, reason = await _extract_decoration_for_url(
                    page, source_url=source_url, timeout_ms=timeout_ms
                )
                if deco:
                    updated.append(
                        DecorationRepairResultItem(
                            id=item.id,
                            decoration=deco,
                            decoration_raw=deco_raw or deco,
                            sourceUrl=source_url,
                        )
                    )
                else:
                    failed.append(
                        DecorationRepairFailureItem(
                            id=item.id,
                            sourceUrl=source_url,
                            reason=reason or "页面中未识别到装修关键词",
                        )
                    )
                await asyncio.sleep(0.6 + random.random() * 0.9)
        finally:
            await context.close()
            await browser.close()

        # Retry blocked/login failures with persistent per-platform sessions
        # so existing login cookies/profile fingerprints can help pass checks.
        if failed:
            failed_map = {(x.id, x.sourceUrl): x for x in failed}
            retry_groups: Dict[str, List[DecorationRepairItem]] = {}
            for f in failed:
                if not _is_retryable_repair_reason(f.reason):
                    continue
                src_item = item_map.get((str(f.id), str(f.sourceUrl or "").strip()))
                platform = _normalize_repair_platform(
                    (src_item.platform if src_item else ""),
                    str(f.sourceUrl or ""),
                )
                if platform not in COLLECTOR_FACTORIES:
                    continue
                retry_item = src_item or DecorationRepairItem(
                    id=str(f.id),
                    sourceUrl=str(f.sourceUrl or ""),
                    platform=platform,
                )
                retry_groups.setdefault(platform, []).append(retry_item)

            for platform, group in retry_groups.items():
                profile_dir = USER_DATA_ROOT / platform
                profile_dir.mkdir(parents=True, exist_ok=True)
                try:
                    pctx = await pw.chromium.launch_persistent_context(
                        user_data_dir=str(profile_dir),
                        headless=False,
                        user_agent=DEFAULT_UA,
                        viewport={"width": 1366, "height": 900},
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--no-default-browser-check",
                        ],
                    )
                except Exception:
                    continue
                try:
                    ppage = await pctx.new_page()
                    manual_wait_attempted = False
                    for item in group:
                        source_url = (item.sourceUrl or "").strip()
                        deco, deco_raw, reason = await _extract_decoration_for_url(
                            ppage,
                            source_url=source_url,
                            timeout_ms=max(timeout_ms, 20_000),
                        )
                        if (
                            not deco
                            and reason
                            and _is_retryable_repair_reason(reason)
                            and not manual_wait_attempted
                        ):
                            manual_wait_attempted = True
                            waited = await _wait_for_manual_unblock(
                                ppage,
                                max_wait_seconds=max(90, min(300, int(timeout_seconds or 18) * 8)),
                            )
                            if waited:
                                deco, deco_raw, reason = await _extract_decoration_for_url(
                                    ppage,
                                    source_url=source_url,
                                    timeout_ms=max(timeout_ms, 25_000),
                                )
                            else:
                                reason = f"{reason}（等待人工验证超时）"
                        key = (item.id, source_url)
                        if deco:
                            updated.append(
                                DecorationRepairResultItem(
                                    id=item.id,
                                    decoration=deco,
                                    decoration_raw=deco_raw or deco,
                                    sourceUrl=source_url,
                                )
                            )
                            failed_map.pop(key, None)
                        else:
                            current = failed_map.get(key)
                            if current and reason:
                                current.reason = f"{reason}（会话重试后仍失败）"
                        await asyncio.sleep(1.0 + random.random() * 1.2)
                finally:
                    await pctx.close()

            failed = list(failed_map.values())
    return updated, failed
