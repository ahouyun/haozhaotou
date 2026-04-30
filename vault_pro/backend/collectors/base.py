"""Base class & shared Playwright context for all collectors.

Design:
- Every collector subclasses BaseCollector and implements `_collect_one_platform`.
- Persistent Chromium contexts per platform, located at backend/.user_data/<platform>/.
  This keeps your login cookie / device fingerprint across runs.
- Headful mode by default (so you can pass slider verification on first run).
  After successful login, future runs are mostly silent though the window
  still appears. We deliberately do NOT use stealth tricks beyond a sane UA.
- Random throttling between page navigations (1.5 - 3.5s) to mimic browsing.
- Each collector enforces its own page cap; the orchestrator stops once the
  global target is met.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:  # Playwright is optional at import-time so unit tests don't blow up.
    from playwright.async_api import (
        BrowserContext,
        Page,
        async_playwright,
    )
except Exception:  # noqa: BLE001
    BrowserContext = Any  # type: ignore
    Page = Any  # type: ignore
    async_playwright = None  # type: ignore


log = logging.getLogger("vault_pro.collector")

USER_DATA_ROOT = Path(__file__).resolve().parent.parent / ".user_data"
USER_DATA_ROOT.mkdir(parents=True, exist_ok=True)

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


ProgressCallback = Callable[[float, Optional[str]], Awaitable[None]]


@dataclass
class CollectorContext:
    """Inputs shared by all collectors for a single /collect call."""

    target: int = 80
    regions: List[str] = field(default_factory=list)
    since_days: int = 1
    fixed_window: bool = True
    throttle_seconds: float = 3.0
    risk_wait_seconds: int = 180
    manual_region_confirm_58: bool = True
    anti_detection_level: str = "medium"
    anti_detection_mode: bool = True
    exclude_hash_keys: set[str] = field(default_factory=set)
    exclude_source_urls: set[str] = field(default_factory=set)
    on_progress: Optional[ProgressCallback] = None

    async def report(self, progress: float, message: Optional[str] = None) -> None:
        if self.on_progress:
            try:
                await self.on_progress(progress, message)
            except Exception:  # noqa: BLE001
                pass


class BaseCollector:
    """Subclasses set `key`, `display_name`, and implement `_collect_one_platform`."""

    key: str = "base"
    display_name: str = "Base"
    max_pages: int = 8
    risk_control_keywords = (
        "访问过于频繁",
        "访问频繁",
        "异常访问",
        "安全验证",
        "人机验证",
        "验证码",
        "滑动验证",
        "请完成验证",
        "疑似使用网页抓取工具",
        "请卸载删除后访问",
        "秒后自动为您返回",
        "captcha",
        "verify",
    )
    _RELATIVE_TIME_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>分钟|小时|天|个月|月|年)前")

    def __init__(self) -> None:
        self.user_data_dir = USER_DATA_ROOT / self.key
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        self.force_ephemeral_profile: bool = False
        self._ephemeral_profile_dir: Optional[Path] = None
        self.last_goto_error: str = ""
        self.last_risk_control_reason: str = ""
        self.throttle_seconds: float = 3.0
        self.runtime_host: Optional[str] = None
        self.anti_detection_level: str = "medium"
        self.anti_detection_mode: bool = True

    # -- Public API -------------------------------------------------------

    async def collect(self, ctx: CollectorContext, max_items: int) -> List[Dict[str, Any]]:
        if async_playwright is None:
            raise RuntimeError(
                "Playwright 未安装。请运行 `pip install playwright` 与 `python -m playwright install chromium`。"
            )

        self.throttle_seconds = max(0.0, float(ctx.throttle_seconds or 0.0))
        self.anti_detection_level = self._normalize_anti_detection_level(
            getattr(ctx, "anti_detection_level", "medium")
        )
        self.anti_detection_mode = bool(ctx.anti_detection_mode)
        if not self.anti_detection_mode:
            self.anti_detection_level = "low"
        await ctx.report(0.02, f"启动 {self.display_name} ...")
        async with async_playwright() as pw:
            context = await self._launch_with_fallback(pw)
            try:
                page = await context.new_page()
                await self._stealth(page)
                items = await self._collect_one_platform(
                    context, page, ctx, max_items=max_items
                )
                return items
            finally:
                await context.close()
                self._cleanup_ephemeral_profile_dir()

    # -- Subclass hook ----------------------------------------------------

    async def _collect_one_platform(
        self,
        context: BrowserContext,
        page: Page,
        ctx: CollectorContext,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    # -- Helpers ----------------------------------------------------------

    async def _launch_with_fallback(self, playwright: Any) -> BrowserContext:
        user_data_dir = self._pick_user_data_dir()
        channels = self._fallback_channels()
        launch_kwargs = {
            "user_data_dir": str(user_data_dir),
            "headless": self._headless(),
            "user_agent": DEFAULT_UA,
            "viewport": {"width": 1366, "height": 900},
            "locale": "zh-CN",
            "timezone_id": "Asia/Shanghai",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-default-browser-check",
            ],
        }

        if self._prefer_system_channel_launch():
            for channel in channels:
                try:
                    log.info("[%s] Prefer launch with system channel=%s", self.key, channel)
                    return await playwright.chromium.launch_persistent_context(
                        channel=channel, **launch_kwargs
                    )
                except Exception as pref_exc:  # noqa: BLE001
                    log.warning(
                        "[%s] Preferred channel launch failed channel=%s; err=%s",
                        self.key,
                        channel,
                        pref_exc,
                    )

        try:
            return await playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as first_exc:  # noqa: BLE001
            # 1) 先尝试切换浏览器通道/路径（不仅是 Executable 缺失时）。
            last_exc: Exception = first_exc
            for channel in channels:
                try:
                    log.warning(
                        "[%s] Browser launch failed once, fallback to channel=%s; err=%s",
                        self.key,
                        channel,
                        first_exc,
                    )
                    return await playwright.chromium.launch_persistent_context(
                        channel=channel, **launch_kwargs
                    )
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc

            for exe_path in self._fallback_executable_paths():
                try:
                    log.warning(
                        "[%s] Channel fallback failed, try executable_path=%s; err=%s",
                        self.key,
                        exe_path,
                        first_exc,
                    )
                    return await playwright.chromium.launch_persistent_context(
                        executable_path=exe_path, **launch_kwargs
                    )
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc

            # 2) 若疑似用户目录损坏/占用，自动换干净目录重试。
            if self._is_startup_crash(first_exc):
                self._cleanup_stale_recovery_profiles(keep=3)
                for recovery_dir in self._recovery_profile_candidates():
                    try:
                        recovery_dir.mkdir(parents=True, exist_ok=True)
                        recovery_kwargs = dict(launch_kwargs)
                        recovery_kwargs["user_data_dir"] = str(recovery_dir)
                        log.warning(
                            "[%s] Retry launch with clean profile: %s; err=%s",
                            self.key,
                            recovery_dir,
                            first_exc,
                        )
                        return await playwright.chromium.launch_persistent_context(
                            **recovery_kwargs
                        )
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc

                    for channel in channels:
                        try:
                            recovery_kwargs = dict(launch_kwargs)
                            recovery_kwargs["user_data_dir"] = str(recovery_dir)
                            log.warning(
                                "[%s] Retry clean profile + channel=%s: %s",
                                self.key,
                                channel,
                                recovery_dir,
                            )
                            return await playwright.chromium.launch_persistent_context(
                                channel=channel, **recovery_kwargs
                            )
                        except Exception as exc:  # noqa: BLE001
                            last_exc = exc

            self._cleanup_ephemeral_profile_dir()
            raise RuntimeError(
                "浏览器启动失败：已尝试默认配置、系统浏览器通道与干净用户目录仍未成功。"
                "请关闭所有残留 Chrome/Edge 后重试；若仍失败，可删除 "
                f"`{self.user_data_dir}` 后再试。原始错误：{first_exc}"
            ) from last_exc

    def _is_startup_crash(self, exc: Exception) -> bool:
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

    def _recovery_profile_candidates(self) -> List[Path]:
        ts = int(time.time())
        base = USER_DATA_ROOT / "_recovery"
        return [
            base / f"{self.key}_{ts}",
            base / f"{self.key}_{ts}_2",
        ]

    def _cleanup_stale_recovery_profiles(self, keep: int = 3) -> None:
        base = USER_DATA_ROOT / "_recovery"
        if not base.exists():
            return
        prefixes = [f"{self.key}_"]
        candidates = [p for p in base.iterdir() if p.is_dir() and any(p.name.startswith(x) for x in prefixes)]
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for old in candidates[max(0, keep):]:
            try:
                shutil.rmtree(old, ignore_errors=True)
            except Exception:  # noqa: BLE001
                pass

    def set_runtime_host(self, host: Optional[str]) -> None:
        self.runtime_host = (host or "").strip() or None

    def set_force_ephemeral_profile(self, enabled: bool) -> None:
        self.force_ephemeral_profile = bool(enabled)

    def resolve_base_url(self, default_url: str) -> str:
        host = (self.runtime_host or "").strip()
        if not host:
            return default_url
        return f"https://{host}"

    def _fallback_channels(self) -> List[str]:
        env = os.environ.get("VAULT_PRO_BROWSER_CHANNELS", "").strip()
        if env:
            channels = [x.strip() for x in env.split(",") if x.strip()]
            if channels:
                return channels

        has_chrome = shutil.which("chrome") is not None
        has_edge = shutil.which("msedge") is not None
        if has_chrome and has_edge:
            return ["chrome", "msedge"]
        if has_chrome:
            return ["chrome"]
        if has_edge:
            return ["msedge"]
        # Still attempt both once; Playwright can find app paths even when
        # binaries are not directly on PATH.
        return ["chrome", "msedge"]

    def _fallback_executable_paths(self) -> List[str]:
        env = os.environ.get("VAULT_PRO_BROWSER_PATHS", "").strip()
        if env:
            values = [x.strip() for x in env.split(";") if x.strip()]
            if values:
                return values

        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
        return [p for p in candidates if Path(p).exists()]

    def _prefer_system_channel_launch(self) -> bool:
        return False

    def _headless(self) -> bool:
        # Allow override via env var. Default to headful so the user can
        # pass the slider on first run.
        env = os.environ.get("VAULT_PRO_HEADLESS", "").strip().lower()
        if env in {"1", "true", "yes"}:
            return True
        return False

    def _pick_user_data_dir(self) -> Path:
        self._ephemeral_profile_dir = None
        if not self.force_ephemeral_profile:
            return self.user_data_dir
        base = USER_DATA_ROOT / "_ephemeral"
        base.mkdir(parents=True, exist_ok=True)
        stamp = f"{self.key}_{int(time.time())}_{random.randint(1000, 9999)}"
        path = base / stamp
        self._ephemeral_profile_dir = path
        return path

    def _cleanup_ephemeral_profile_dir(self) -> None:
        path = self._ephemeral_profile_dir
        self._ephemeral_profile_dir = None
        if not path:
            return
        try:
            shutil.rmtree(path, ignore_errors=True)
        except Exception:  # noqa: BLE001
            pass

    async def _stealth(self, page: Page) -> None:
        try:
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            if self.anti_detection_mode:
                await page.add_init_script(
                    """
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-CN', 'zh', 'en-US', 'en']
                    });
                    Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
                    """
                )
        except Exception:  # noqa: BLE001
            pass

    async def throttle(self, base: Optional[float] = None, jitter: Optional[float] = None) -> None:
        if base is None and jitter is None:
            wait_base = self.throttle_seconds * 0.65
            level_jitter = {
                "low": 0.45,
                "medium": 0.95,
                "high": 1.35,
            }.get(self.anti_detection_level, 0.95)
            wait_jitter = self.throttle_seconds * level_jitter
        else:
            wait_base = self.throttle_seconds if base is None else max(0.0, float(base))
            wait_jitter = (
                self.throttle_seconds * 0.35 if jitter is None else max(0.0, float(jitter))
            )
        await asyncio.sleep(wait_base + random.random() * wait_jitter)

    async def safe_goto(self, page: Page, url: str, timeout_ms: int = 25_000) -> bool:
        try:
            self.last_goto_error = ""
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if self.anti_detection_mode:
                try:
                    idle_timeout = {
                        "low": 600,
                        "medium": 1200,
                        "high": 1800,
                    }.get(self.anti_detection_level, 1200)
                    await page.wait_for_load_state("networkidle", timeout=idle_timeout)
                except Exception:  # noqa: BLE001
                    pass
            if self.anti_detection_mode:
                await self._simulate_human_actions(page)
            return True
        except Exception as exc:  # noqa: BLE001
            self.last_goto_error = str(exc)
            log.warning("[%s] goto %s failed: %s", self.key, url, exc)
            return False

    async def _simulate_human_actions(self, page: Page) -> None:
        """Inject human-like interactions after navigation."""
        try:
            viewport = page.viewport_size or {"width": 1366, "height": 900}
            width = max(900, int(viewport.get("width") or 1366))
            height = max(700, int(viewport.get("height") or 900))

            profile = {
                "low": {
                    "settle": (0.12, 0.35),
                    "moves": (1, 2),
                    "move_steps": (8, 16),
                    "scrolls": (1, 2),
                    "hover_prob": 0.18,
                    "key_prob": 0.12,
                },
                "medium": {
                    "settle": (0.28, 0.72),
                    "moves": (2, 4),
                    "move_steps": (10, 26),
                    "scrolls": (1, 3),
                    "hover_prob": 0.35,
                    "key_prob": 0.30,
                },
                "high": {
                    "settle": (0.45, 1.1),
                    "moves": (3, 6),
                    "move_steps": (14, 34),
                    "scrolls": (2, 5),
                    "hover_prob": 0.58,
                    "key_prob": 0.50,
                },
            }.get(self.anti_detection_level, {})

            settle_min, settle_rand = profile.get("settle", (0.28, 0.72))
            await asyncio.sleep(settle_min + random.random() * settle_rand)

            current_x = random.randint(int(width * 0.18), int(width * 0.82))
            current_y = random.randint(int(height * 0.18), int(height * 0.72))
            min_moves, max_moves = profile.get("moves", (2, 4))
            min_steps, max_steps = profile.get("move_steps", (10, 26))
            for _ in range(random.randint(min_moves, max_moves)):
                next_x = random.randint(int(width * 0.12), int(width * 0.9))
                next_y = random.randint(int(height * 0.12), int(height * 0.84))
                await page.mouse.move(next_x, next_y, steps=random.randint(min_steps, max_steps))
                current_x, current_y = next_x, next_y
                await asyncio.sleep(0.05 + random.random() * 0.14)

            min_scrolls, max_scrolls = profile.get("scrolls", (1, 3))
            for _ in range(random.randint(min_scrolls, max_scrolls)):
                delta = random.randint(180, 520)
                if random.random() < 0.24:
                    delta = -random.randint(70, 220)
                await page.mouse.wheel(0, delta)
                await asyncio.sleep(0.18 + random.random() * 0.36)

            if random.random() < float(profile.get("hover_prob", 0.35)):
                try:
                    hover_target = page.locator(
                        "a[href], .property, .list-item, .house-list-wrap li, .listUl li"
                    ).first
                    await hover_target.hover(timeout=900)
                    await asyncio.sleep(0.08 + random.random() * 0.22)
                except Exception:  # noqa: BLE001
                    pass

            if random.random() < float(profile.get("key_prob", 0.3)):
                try:
                    await page.keyboard.press("PageDown")
                    await asyncio.sleep(0.12 + random.random() * 0.28)
                    if random.random() < 0.35:
                        await page.keyboard.press("PageUp")
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass

    def make_record_hash_key(
        self,
        *,
        community: Any,
        area: Any,
        total_price: Any,
        floor: Any,
    ) -> str:
        """Generate a stable fingerprint compatible with frontend makeHash()."""
        try:
            area_num = float(area or 0.0)
        except Exception:  # noqa: BLE001
            area_num = 0.0
        try:
            total_num = float(total_price or 0.0)
        except Exception:  # noqa: BLE001
            total_num = 0.0
        raw = (
            f"{str(community or '').strip()}|"
            f"{area_num:.1f}|"
            f"{total_num:.1f}|"
            f"{str(floor or '').strip()}"
        )
        h = 0x811C9DC5
        for ch in raw:
            h ^= ord(ch)
            h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) & 0xFFFFFFFF
        return f"{h:08x}"

    def should_exclude_record(self, rec: Dict[str, Any], ctx: CollectorContext) -> bool:
        source_url = str(rec.get("sourceUrl") or "").strip()
        if source_url and source_url in ctx.exclude_source_urls:
            return True
        hash_key = self.make_record_hash_key(
            community=rec.get("community"),
            area=rec.get("area"),
            total_price=rec.get("totalPrice"),
            floor=rec.get("floor"),
        )
        return bool(hash_key and hash_key in ctx.exclude_hash_keys)

    async def has_next_page(self, page: Page) -> bool:
        """Best-effort pagination check: return True only when next-page is available."""
        try:
            state = await page.evaluate(
                """
                () => {
                  const selectors = [
                    'a[rel="next"]',
                    '.next',
                    '.next-page',
                    '.pager-next',
                    '.pagination-next',
                    '.page-next',
                    '.multi-page a',
                    '.pager a',
                    '.pagination a',
                    '.page-box a'
                  ];
                  const links = Array.from(
                    new Set(
                      selectors.flatMap(sel => Array.from(document.querySelectorAll(sel)))
                    )
                  );
                  const isVisible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden') return false;
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                  };
                  const canUse = (el) => {
                    if (!el || !isVisible(el)) return false;
                    if (el.matches('[disabled], .disabled, .is-disabled, .off, .inactive')) return false;
                    if (el.getAttribute('aria-disabled') === 'true') return false;
                    const cls = String(el.className || '').toLowerCase();
                    if (/(disabled|inactive|forbid)/.test(cls)) return false;
                    const txt = String(el.textContent || '').replace(/\\s+/g, '').toLowerCase();
                    const title = String(el.getAttribute('title') || '').replace(/\\s+/g, '').toLowerCase();
                    const label = txt || title;
                    if (!label) return false;
                    if (!(/下一页|下页|next|>|>>/.test(label))) return false;
                    const href = String(el.getAttribute('href') || '').trim().toLowerCase();
                    if (href && href !== '#' && !href.startsWith('javascript:')) return true;
                    return el.tagName.toLowerCase() === 'button';
                  };
                  return links.some(canUse);
                }
                """
            )
            return bool(state)
        except Exception:  # noqa: BLE001
            return True

    def _normalize_anti_detection_level(self, raw: Any) -> str:
        level = str(raw or "").strip().lower()
        if level in {"low", "medium", "high"}:
            return level
        return "medium"

    async def maybe_wait_for_login(
        self, page: Page, ctx: CollectorContext, signal_text: str
    ) -> None:
        """Detect login/captcha walls and give the user up to 90s to pass them.

        If the page contains the platform's "after-login" anchor (signal_text),
        we move on immediately. Otherwise we poll once per second.
        """
        deadline = 90
        for elapsed in range(deadline):
            try:
                content = await page.content()
            except Exception:  # noqa: BLE001
                content = ""
            if signal_text and signal_text in content:
                return
            if elapsed == 0:
                await ctx.report(
                    0.05,
                    f"[{self.display_name}] 等待人工登录/滑块（最多 {deadline}s）...",
                )
            await asyncio.sleep(1)
        # Don't raise: the caller may still be able to extract some data even
        # when our heuristic misses the success signal.

    async def is_risk_control_page(self, page: Page) -> bool:
        """Best-effort check for anti-bot / captcha pages."""
        self.last_risk_control_reason = ""
        try:
            url = (page.url or "").lower()
        except Exception:  # noqa: BLE001
            url = ""
        if any(token in url for token in ("antibot", "verify", "captcha", "safe")):
            self.last_risk_control_reason = "命中风控/验证路由"
            return True

        try:
            # Use visible body text first to avoid false positives from scripts.
            visible_text = await page.locator("body").inner_text(timeout=3000)
        except Exception:  # noqa: BLE001
            visible_text = ""

        compact_text = re.sub(r"\s+", "", visible_text).lower()
        if any(k.lower() in compact_text for k in self.risk_control_keywords):
            if self._is_hard_ip_block(compact_text):
                self.last_risk_control_reason = (
                    "检测到站点按公网 IP 封禁（疑似网页抓取工具拦截），请切换网络后重试"
                )
            else:
                self.last_risk_control_reason = "检测到验证码/风控页"
            return True

        # Fallback to full HTML only for strong Chinese markers. Terms like
        # "verify" are too common in inline scripts and cause mis-detection.
        try:
            content = (await page.content()).lower()
        except Exception:  # noqa: BLE001
            return False
        strong_markers = (
            "访问过于频繁",
            "访问频繁",
            "异常访问",
            "安全验证",
            "人机验证",
            "验证码",
            "滑动验证",
            "请完成验证",
            "callback.58.com/antibot",
            "/antibot/verifycode",
            "namespace=cloud_58_fangyuan_pc",
            "疑似使用网页抓取工具",
            "请卸载删除后访问",
        )
        blocked = any(k in content for k in strong_markers)
        if blocked:
            content_compact = re.sub(r"\s+", "", content)
            if self._is_hard_ip_block(content_compact):
                self.last_risk_control_reason = (
                    "检测到站点按公网 IP 封禁（疑似网页抓取工具拦截），请切换网络后重试"
                )
            else:
                self.last_risk_control_reason = "检测到验证码/风控页"
        return blocked

    async def wait_for_risk_control_clear(
        self,
        page: Page,
        ctx: CollectorContext,
        *,
        max_wait_seconds: int = 180,
    ) -> bool:
        """Keep browser open while user finishes captcha/login manually.

        Returns True once risk-control markers disappear, otherwise False on timeout.
        """
        await ctx.report(
            0.08,
            f"[{self.display_name}] 检测到验证码/风控，请在弹窗浏览器完成验证（最多等待 {max_wait_seconds}s）...",
        )
        for elapsed in range(max_wait_seconds):
            blocked = await self.is_risk_control_page(page)

            if not blocked:
                await ctx.report(
                    0.12,
                    f"[{self.display_name}] 已检测到验证通过，立即恢复采集 ...",
                )
                return True

            if self.last_risk_control_reason and "公网 IP 封禁" in self.last_risk_control_reason:
                await ctx.report(
                    0.08,
                    f"[{self.display_name}] {self.last_risk_control_reason}",
                )
                return False

            # Keep the frontend updated more frequently so users know the task
            # is alive while they are solving slider/captcha in browser.
            if elapsed > 0 and elapsed % 3 == 0:
                remain = max_wait_seconds - elapsed
                await ctx.report(
                    0.08,
                    f"[{self.display_name}] 仍在等待验证完成（剩余约 {remain}s，完成后会自动继续）...",
                )
            await asyncio.sleep(1)
        return False

    def _is_hard_ip_block(self, compact_text: str) -> bool:
        txt = str(compact_text or "")
        if not txt:
            return False
        return (
            "疑似使用网页抓取工具" in txt
            or "请卸载删除后访问" in txt
            or ("ip:" in txt and "自动为您返回" in txt)
        )

    def within_since_days(self, text: str, since_days: int) -> bool:
        """Best-effort relative time filter used by collectors.

        When parsing fails, we keep the record to avoid false negatives.
        """
        days_limit = int(since_days or 0)
        if days_limit <= 0:
            return True
        raw = (text or "").strip().lower()
        if not raw:
            return True

        if "今天" in raw or "刚刚" in raw:
            return True
        if "昨天" in raw:
            return days_limit >= 1

        m = self._RELATIVE_TIME_RE.search(raw)
        if not m:
            # Cannot parse - keep item instead of dropping potentially useful data.
            return True

        num = int(m.group("num"))
        unit = m.group("unit")
        if unit in ("分钟", "小时"):
            age_days = 0
        elif unit == "天":
            age_days = num
        elif unit in ("个月", "月"):
            age_days = num * 30
        elif unit == "年":
            age_days = num * 365
        else:
            return True
        return age_days <= days_limit
