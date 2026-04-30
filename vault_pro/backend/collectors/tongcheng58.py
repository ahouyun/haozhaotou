"""58 同城 (lvliang.58.com) second-hand collector.

58 is the most fragile of the three: heavy ad insertion, listings keep
moving between routes, and 58 fan ti (反爬) sometimes blanks the page.
We use lvliang.58.com/ershoufang/ as the entry, sorted newest-first via
the /pn{n}/ paging convention, and tolerate empty responses by stopping
at the first blank page.

The collector emits the same canonical schema as Beike via normalize.finalise.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List
from urllib.parse import quote, unquote, urljoin

from .base import BaseCollector, BrowserContext, CollectorContext, Page
from .. import normalize


log = logging.getLogger("vault_pro.58")

BASE_URL = "https://lvliang.58.com"
_AREA_TEXT_RE = re.compile(r"\d[\d,]*(?:\.\d+)?\s*(?:㎡|m²|m2|平米|平方米|平)\b", re.IGNORECASE)
_TOTAL_PRICE_RE = re.compile(r"\d[\d,]*(?:\.\d+)?\s*(?:万|元)")
_UNIT_PRICE_RE = re.compile(
    r"\d[\d,]*(?:\.\d+)?\s*元(?:/|／)?\s*(?:㎡|m²|m2|平米|平方米|平)",
    re.IGNORECASE,
)
_FLOOR_TEXT_RE = re.compile(
    r"(?:\d+\s*/\s*\d+|(?:低|中|高|底|顶|地下)?楼?层(?:\s*\(共\d+层\))?)"
)

REGION_QUERY: Dict[str, str] = {
    # 与前端区域选项保持一致，确保“选择哪个区就抓哪个区”。
    "离石": "lishi",
    "孝义": "xiaoyi",
    "汾阳": "fenyang",
    "文水": "wenshui",
    "临县": "linxian",
    "交城": "jiaocheng",
    "柳林": "liulin",
    "中阳": "zhongyangxian",
    "方山县": "fangshan",
    "岚县": "lanxian",
    "交口县": "jiaokou",
    "兴县": "xingxian",
    "石楼县": "shilou",
    "吕梁周边": "lvliangzhoubian",
}


class Tongcheng58Collector(BaseCollector):
    key = "58"
    display_name = "58 同城·吕梁"
    max_pages = 6
    _REGION_ALIASES: Dict[str, tuple[str, ...]] = {
        "离石": ("离石区",),
        "孝义": ("孝义市",),
        "汾阳": ("汾阳市",),
        "文水": ("文水县",),
        "交城": ("交城县",),
        "临县": ("临县",),
        "柳林": ("柳林县",),
        "中阳": ("中阳县",),
        "方山县": ("方山",),
        "岚县": ("岚县",),
        "交口县": ("交口",),
        "兴县": ("兴县",),
        "石楼县": ("石楼",),
        "吕梁周边": ("周边",),
    }
    _SLUG_TO_REGION: Dict[str, str] = {v: k for k, v in REGION_QUERY.items() if v}

    def _base_url(self) -> str:
        return self.resolve_base_url(BASE_URL)

    def _prefer_system_channel_launch(self) -> bool:
        # 58 is especially sensitive to bundled-automation fingerprints.
        # Prefer real installed Chrome/Edge first when available.
        return True

    def _default_regions(self, ctx: CollectorContext) -> List[str]:
        if ctx.regions:
            return ctx.regions
        host = (self.runtime_host or "").lower()
        if "lvliang" in host or host.startswith("ll."):
            return list(REGION_QUERY.keys())
        return ["全城"]

    async def _collect_one_platform(
        self,
        context: BrowserContext,
        page: Page,
        ctx: CollectorContext,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        region_failures: List[str] = []
        regions = self._default_regions(ctx)

        for region in regions:
            if len(results) >= max_items:
                break
            region_key = self._normalize_region_input(region)
            slug = REGION_QUERY.get(region_key, "")
            try:
                picked = await self._collect_region(
                    page, ctx, region=region_key, slug=slug, want=max_items - len(results)
                )
                results.extend(picked)
            except Exception as exc:  # noqa: BLE001
                # When users choose multiple regions, keep partial success
                # instead of failing the whole platform on a single region.
                log.warning(
                    "[%s] region collect failed region=%s err=%s",
                    self.key,
                    region_key,
                    exc,
                )
                await ctx.report(
                    min(0.9, 0.15 + len(results) / max(1, max_items) * 0.6),
                    f"[{self.display_name}] 区域“{region_key}”采集失败，已跳过并继续其它区域：{exc}",
                )
                region_failures.append(f"{region_key}: {exc}")
        if region_failures and not results:
            brief = "; ".join(region_failures[:3])
            if len(region_failures) > 3:
                brief += f" ...（共 {len(region_failures)} 个区域失败）"
            raise RuntimeError(f"全部所选区域采集失败：{brief}")
        return results

    def _normalize_region_input(self, raw_region: str) -> str:
        txt = str(raw_region or "").strip()
        if txt in REGION_QUERY:
            return txt
        for region, aliases in self._REGION_ALIASES.items():
            if txt == region or txt in aliases:
                return region
        return txt or "全城"

    async def _collect_region(
        self,
        page: Page,
        ctx: CollectorContext,
        *,
        region: str,
        slug: str,
        want: int,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen_keys: set[tuple[str, str, str, str]] = set()
        prev_page_sources: set[str] = set()
        max_pages = 1 if ctx.fixed_window else self.max_pages
        strict_region_mode = bool(ctx.regions and region and region != "全城")
        operator_confirmed = False
        for page_no in range(1, max_pages + 1):
            if len(out) >= want:
                break
            urls = self._build_urls(region, slug, page_no)
            if page_no > 1:
                urls = self._merge_unique_urls(
                    urls + self._build_urls_from_current(page.url, page_no)
                )
            if operator_confirmed and page_no == 1:
                # After manual area selection on page 1, keep current URL only.
                # Retrying alternate route templates can wipe the chosen filters.
                urls = [page.url]
            cards: List[Dict[str, Any]] = []
            for try_idx, url in enumerate(urls, start=1):
                await ctx.report(
                    min(0.95, 0.1 + page_no * 0.08),
                    f"[{self.display_name}] {region} pn{page_no}（尝试 {try_idx}/{len(urls)}）...",
                )
                ok = await self.safe_goto(page, url)
                if not ok:
                    continue
                await self.maybe_wait_for_login(page, ctx, signal_text="ershoufang")
                if strict_region_mode:
                    region_ok = await self._is_region_context_valid(page, region=region, slug=slug)
                    if not region_ok:
                        await ctx.report(
                            0.13,
                            f"[{self.display_name}] 当前页面区域与目标“{region}”不一致，跳过该路由并尝试下一候选 ...",
                        )
                        await self.throttle(base=0.4, jitter=0.3)
                        continue
                if page_no > 1:
                    reached_expected_page = await self._is_expected_page_no(page, page_no)
                    if not reached_expected_page:
                        await ctx.report(
                            0.14,
                            f"[{self.display_name}] pn{page_no} 未生效（当前 URL: {page.url}），尝试备用路由 ...",
                        )
                        await self.throttle(base=0.5, jitter=0.2)
                        continue
                if (
                    strict_region_mode
                    and ctx.manual_region_confirm_58
                    and page_no == 1
                    and try_idx == 1
                ):
                    confirmed = await self._wait_operator_region_confirm(
                        page, ctx, region=region, slug=slug
                    )
                    if not confirmed:
                        raise RuntimeError(
                            f"{region} 人工选区确认超时，请在 58 页面完成选区并点击“开始采集”后重试"
                        )
                    operator_confirmed = True
                    # 58 often performs an in-page refresh after area interaction.
                    # Wait briefly so extraction runs on the stabilized DOM.
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=5000)
                    except Exception:  # noqa: BLE001
                        pass
                    await self.throttle(base=1.0, jitter=0.4)
                # 58 页面经常同时包含风控脚本和正常列表。优先按列表提取，
                # 只有首屏提取不到时再进入风控等待，避免误判导致卡住不采集。
                cards = await self._extract_cards(page, region, ctx)
                if not cards and operator_confirmed and page_no == 1:
                    # In manual mode, avoid extra navigations and keep polling current page.
                    for retry in range(1, 7):
                        remain = (7 - retry) * 2
                        await ctx.report(
                            0.13,
                            f"[{self.display_name}] 已人工确认区域，等待页面稳定后重试提取（剩余约 {remain}s）...",
                        )
                        await self.throttle(base=1.6, jitter=0.6)
                        cards = await self._extract_cards(page, region, ctx)
                        if cards:
                            break
                if not cards and page_no == 1 and try_idx == 1 and not operator_confirmed:
                    # 58 often returns transient/anti-bot pages before list DOM is ready.
                    # On first page, always give users a manual verification window.
                    cleared = await self.wait_for_risk_control_clear(
                        page,
                        ctx,
                        max_wait_seconds=max(60, int(ctx.risk_wait_seconds or 180)),
                    )
                    if not cleared:
                        reason = self.last_risk_control_reason or (
                            f"验证等待超时（{max(60, int(ctx.risk_wait_seconds or 180))}s）"
                        )
                        raise RuntimeError(
                            f"{region} {reason}，请完成验证或切换网络后重试"
                        )
                    await ctx.report(
                        0.14,
                        f"[{self.display_name}] 验证已通过，正在重载列表页并重试提取 ...",
                    )
                    await self.safe_goto(page, url)
                    await self.throttle(base=0.8, jitter=0.6)
                    cards = await self._extract_cards(page, region, ctx)
                    if not cards:
                        # Give the page extra time to recover after manual actions.
                        for retry in range(1, 11):
                            remain = (10 - retry) * 3
                            await ctx.report(
                                0.14,
                                f"[{self.display_name}] 首屏暂无房源，等待页面稳定后重试（剩余约 {remain}s）...",
                            )
                            await self.throttle(base=2.0, jitter=1.2)
                            cards = await self._extract_cards(page, region, ctx)
                            if cards:
                                break
                if cards and page_no > 1:
                    page_sources = {
                        str(x.get("sourceUrl") or "").strip()
                        for x in cards
                        if str(x.get("sourceUrl") or "").strip()
                    }
                    if page_sources and prev_page_sources and page_sources == prev_page_sources:
                        await ctx.report(
                            0.14,
                            f"[{self.display_name}] pn{page_no} 与上一页房源完全一致，疑似仍停留首页，继续尝试其它翻页路由 ...",
                        )
                        cards = []
                        await self.throttle(base=0.5, jitter=0.2)
                        continue
                if cards:
                    break
                if operator_confirmed and page_no == 1:
                    # After operator confirmation, never probe fallback route URLs.
                    # Keep the currently selected regional page to avoid route reset.
                    break
                await self.throttle(base=0.4, jitter=0.4)
            if not cards:
                if page_no == 1:
                    diag: Dict[str, Any] = {}
                    try:
                        diag = await page.evaluate(
                            """
                            () => ({
                              url: location.href,
                              property: document.querySelectorAll('.property').length,
                              houseListWrap: document.querySelectorAll('.house-list-wrap > li').length,
                              listUl: document.querySelectorAll('.listUl > li').length,
                              houseCell: document.querySelectorAll('li.house-cell').length,
                              listWrap: document.querySelectorAll('.list-wrap li, .list li[logr], .list li').length,
                              bodySnippet: (document.body && document.body.innerText || '').slice(0, 120),
                            })
                            """
                        )
                    except Exception:  # noqa: BLE001
                        diag = {}
                    raise RuntimeError(
                        f"{region} 首屏未提取到匹配区域的数据（可能是风控、区域路由失效或页面结构变更）；"
                        f"diag={diag}"
                    )
                break
            page_sources = {
                str(x.get("sourceUrl") or "").strip()
                for x in cards
                if str(x.get("sourceUrl") or "").strip()
            }
            if page_sources:
                prev_page_sources = page_sources
            appended = 0
            for rec in cards:
                dedupe_key = (
                    str(rec.get("sourceUrl") or "").strip(),
                    str(rec.get("community") or "").strip(),
                    str(rec.get("totalPrice") or rec.get("totalPriceText") or "").strip(),
                    str(rec.get("area") or rec.get("areaText") or "").strip(),
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                if self.should_exclude_record(rec, ctx):
                    continue
                out.append(rec)
                appended += 1
            if appended == 0 and page_no > 1:
                await ctx.report(
                    0.15,
                    f"[{self.display_name}] pn{page_no} 提取结果全部重复，停止继续翻页以避免重复入库",
                )
                break
            if page_no < max_pages:
                has_next = await self.has_next_page(page)
                if not has_next:
                    await ctx.report(
                        min(0.96, 0.12 + page_no * 0.08),
                        f"[{self.display_name}] {region} 当前已到末页，停止翻页以避免无效刷新",
                    )
                    break
            await self.throttle()

        return out[:want]

    async def _is_region_context_valid(self, page: Page, *, region: str, slug: str) -> bool:
        """Check whether current page really points to the target region.

        We validate across multiple weak signals and require at least one
        strong signal hit to avoid accidental cross-region collection.
        """
        aliases = self._REGION_ALIASES.get(region, ())
        tokens = [region, *aliases]
        tokens_js = "[" + ",".join(repr(x) for x in tokens if x) + "]"
        slug_js = repr((slug or "").lower())
        try:
            state = await page.evaluate(
                f"""
                () => {{
                  const tokens = {tokens_js};
                  const slug = {slug_js};
                  const normalize = (txt) => String(txt || '').replace(/\\s+/g, '').toLowerCase();
                  const path = normalize(location.pathname || '');
                  const activeArea = normalize(
                    (
                      document.querySelector('.filter-wrap a.active, .filter-wrap .active a, .search-nav a.active, .crumb a.active')
                      || document.querySelector('.content-side-left .active, .house-area .active')
                    )?.textContent || ''
                  );
                  const tokenMatchedInActive = tokens.some(t => normalize(t) && activeArea.includes(normalize(t)));
                  const slugMatchedInPath =
                    !!slug && (
                      path.includes('/' + slug + '/')
                      || path.includes('/ershoufang/' + slug + '/')
                      || path.includes('-' + slug + '-')
                    );
                  const hasList =
                    document.querySelectorAll('.property').length > 0
                    || document.querySelectorAll('.house-list-wrap > li').length > 0
                    || document.querySelectorAll('.listUl > li').length > 0
                    || document.querySelectorAll('li.house-cell').length > 0
                    || document.querySelectorAll('.list-wrap li, .list li[logr], .list li').length > 0;
                  return {{
                    slugMatchedInPath,
                    tokenMatchedInActive,
                    hasList,
                  }};
                }}
                """
            )
        except Exception:  # noqa: BLE001
            return False

        slug_ok = bool((state or {}).get("slugMatchedInPath"))
        active_ok = bool((state or {}).get("tokenMatchedInActive"))
        has_list = bool((state or {}).get("hasList"))
        if not has_list:
            return False
        # Only trust strong route/filter signals; avoid body-text fallback
        # because district names often appear globally in filter menus.
        return bool(slug_ok or active_ok)

    async def _wait_operator_region_confirm(
        self,
        page: Page,
        ctx: CollectorContext,
        *,
        region: str,
        slug: str,
    ) -> bool:
        max_wait = max(30, int(ctx.risk_wait_seconds or 180))
        aliases = self._REGION_ALIASES.get(region, ())
        hint_tokens = [region, *aliases]
        hint_js = "[" + ",".join(repr(x) for x in hint_tokens if x) + "]"
        slug_js = repr((slug or "").lower())
        await ctx.report(
            0.11,
            f"[{self.display_name}] 请在 58 页面手动点击“{region}”区域并点站内“开始采集”（无需二次确认，最多 {max_wait}s）...",
        )
        for elapsed in range(max_wait):
            try:
                state = await page.evaluate(
                    f"""
                    () => {{
                      const tokens = {hint_js};
                      const slug = {slug_js};
                      const normalize = (txt) => String(txt || '').replace(/\\s+/g, '').toLowerCase();
                      const activeArea = normalize(
                        (
                          document.querySelector('.filter-wrap a.active, .filter-wrap .active a, .search-nav a.active, .crumb a.active')
                          || document.querySelector('.content-side-left .active, .house-area .active')
                        )?.textContent || ''
                      );
                      const activeMatched = tokens.some(t => normalize(t) && activeArea.includes(normalize(t)));
                      const path = (location.pathname || '').toLowerCase();
                      const slugMatched = !!slug && (
                        path.includes('/' + slug + '/')
                        || path.includes('/ershoufang/' + slug + '/')
                      );
                      const hasList =
                        document.querySelectorAll('.property').length > 0
                        || document.querySelectorAll('.house-list-wrap > li').length > 0
                        || document.querySelectorAll('.listUl > li').length > 0
                        || document.querySelectorAll('li.house-cell').length > 0
                        || document.querySelectorAll('.list-wrap li, .list li[logr], .list li').length > 0;
                      const ready = hasList && (slugMatched || activeMatched);
                      return {{
                        ready,
                        activeMatched,
                        slugMatched,
                        hasList,
                      }};
                    }}
                    """
                )
            except Exception:  # noqa: BLE001
                state = {"ready": False}

            if state.get("ready"):
                await ctx.report(0.12, f"[{self.display_name}] 已收到人工确认，开始采集 {region} ...")
                return True

            if elapsed > 0 and elapsed % 5 == 0:
                remain = max_wait - elapsed
                await ctx.report(
                    0.11,
                    f"[{self.display_name}] 等待人工确认中（{region}，剩余约 {remain}s）...",
                )
            await self.throttle(base=1.0, jitter=0.2)
        return False

    def _build_urls(self, region: str, slug: str, page_no: int) -> List[str]:
        # 58 支持多种区域路由。不同城市站点模板不同，顺序尝试。
        suffix = f"pn{page_no}/" if page_no > 1 else ""
        base_url = self._base_url()
        if not slug:
            return [urljoin(base_url + "/", f"ershoufang/{suffix}")]
        return [
            urljoin(base_url + "/", f"{slug}/ershoufang/{suffix}"),
            urljoin(base_url + "/", f"ershoufang/{slug}/{suffix}"),
            urljoin(base_url + "/", f"ershoufang/{suffix}?key={quote(region)}"),
        ]

    def _build_urls_from_current(self, current_url: str, page_no: int) -> List[str]:
        if page_no <= 1:
            return []
        src = str(current_url or "").strip()
        if not src or "ershoufang" not in src:
            return []
        candidates: List[str] = []
        replaced = re.sub(r"/pn\d+/", f"/pn{page_no}/", src)
        if replaced != src:
            candidates.append(replaced)
        else:
            m = re.search(r"(ershoufang/)", src)
            if m:
                candidates.append(src[: m.end()] + f"pn{page_no}/" + src[m.end() :])
            if src.endswith("/"):
                candidates.append(src + f"pn{page_no}/")
            else:
                candidates.append(src + f"/pn{page_no}/")
        return self._merge_unique_urls(candidates)

    def _merge_unique_urls(self, urls: List[str]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for raw in urls:
            u = str(raw or "").strip()
            if not u or u in seen:
                continue
            seen.add(u)
            out.append(u)
        return out

    async def _is_expected_page_no(self, page: Page, page_no: int) -> bool:
        if page_no <= 1:
            return True
        try:
            state = await page.evaluate(
                """
                () => {
                  const active = document.querySelector(
                    '.pager .cur, .pager a.active, .pager .active, .page .cur, .page a.active, .house-pages .cur'
                  );
                  const activeText = active ? (active.textContent || '').trim() : '';
                  return {
                    url: location.href || '',
                    activeText,
                  };
                }
                """
            )
        except Exception:  # noqa: BLE001
            state = {"url": (page.url or ""), "activeText": ""}

        url_txt = str((state or {}).get("url") or page.url or "").lower()
        if f"/pn{page_no}/" in url_txt or f"pn{page_no}" in url_txt:
            return True

        active_text = str((state or {}).get("activeText") or "").strip()
        if active_text.isdigit() and int(active_text) == page_no:
            return True
        return False

    async def _extract_cards(
        self,
        page: Page,
        region: str,
        ctx: CollectorContext,
    ) -> List[Dict[str, Any]]:
        try:
            await page.wait_for_selector(
                ".property, .house-list, .house-list-wrap, .listUl, li.house-cell, li[logr], .list-wrap",
                timeout=8000,
            )
        except Exception:  # noqa: BLE001
            return []

        raw_list: List[Dict[str, Any]] = await page.evaluate(
            """
            () => {
              // 58 has rotated through three list templates. Try the newest
              // first (.property), then .house-list-wrap, then the older
              // .listUl. Whichever returns rows wins.
              const select = (sel, root) => Array.from((root||document).querySelectorAll(sel));
              const pickDecorationNearArea = (card) => {
                const norm = (txt) => String(txt || '').trim().replace(/\\s+/g, ' ');
                const hasDeco = (txt) => /(精装|精装修|简装|简装修|毛坯|毛胚|清水|装修|拎包入住)/.test(txt);
                const hasArea = (txt) => /\\d[\\d,.]*\\s*(㎡|m²|m2|平米|平方米|平)/i.test(txt);
                const seq = select(
                  '.property-content-info-text span, .property-content-info-text div, .property-content-info-text p, '
                  + '.property-content-info-attribute span, .property-content-info-attribute div, .property-content-info-attribute p, '
                  + '.property-content-detail span, .property-content-detail div, .property-content-detail p, '
                  + '.house-tags span, .tags span, .tags-wrap span, .baseinfo span',
                  card
                )
                  .map(el => norm(el.textContent || ''))
                  .filter(Boolean);
                for (let i = 0; i < seq.length; i++) {
                  if (!hasArea(seq[i])) continue;
                  for (let j = i + 1; j <= Math.min(i + 3, seq.length - 1); j++) {
                    if (hasDeco(seq[j])) return seq[j];
                  }
                }
                const direct = seq.find(hasDeco);
                return direct || '';
              };

              let cards = select('.property');
              let mode = 'property';
              if (cards.length === 0) { cards = select('.house-list-wrap > li'); mode = 'house-list'; }
              if (cards.length === 0) { cards = select('.listUl > li.list-item, .listUl > li'); mode = 'listUl'; }
              if (cards.length === 0) { cards = select('li.house-cell, .house-list-wrap li, .house-list li'); mode = 'house-cell'; }
              if (cards.length === 0) { cards = select('.list-wrap li, .list li[logr], .list li'); mode = 'list-wrap'; }

              return cards.map(card => {
                const t = sel => {
                  const el = card.querySelector(sel);
                  return el ? el.textContent.trim().replace(/\\s+/g, ' ') : '';
                };
                const fullText = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                const link = card.querySelector('a[href*="ershoufang"], a[href*="zufang"], a');
                const url = link ? link.href : '';
                const linkText = link ? (link.getAttribute('title') || link.textContent || '').trim().replace(/\\s+/g, ' ') : '';
                const fallbackTitle = (linkText || fullText.split(' ').slice(0, 3).join(' ') || '').trim();
                const decorationText = pickDecorationNearArea(card);
                if (mode === 'property') {
                  return {
                    mode,
                    title: t('.property-content-title-name, .property-content-title a, .property-title, .title, h3 a') || fallbackTitle,
                    community: t('.property-content-info-comm-name, .property-content-info-comm a, .property-content-info a, .baseinfo a'),
                    region: t('.property-content-info-comm-address, .property-content-info-comm-address a, .address, .add'),
                    houseInfo: t('.property-content-info-text, .property-content-info, .baseinfo'),
                    timeText: t('.property-content-info-comm-address, .property-content-info-comm-name'),
                    totalPriceText: t('.property-price-total'),
                    unitPriceText: t('.property-price-average'),
                    rawMetaText: t('.property-content-info-attribute, .property-content-detail, .house-tags, .tags-wrap, .tags'),
                    decorationText,
                    sourceUrl: url,
                    fullText,
                  };
                }
                if (mode === 'house-list') {
                  return {
                    mode,
                    title: t('.title'),
                    community: t('.baseinfo .listTitCon, .baseinfo a'),
                    region: t('.baseinfo span:last-child'),
                    houseInfo: t('.baseinfo'),
                    timeText: t('.baseinfo, .update, .time'),
                    totalPriceText: t('.price .sum, .price b'),
                    unitPriceText: t('.price .unit'),
                    rawMetaText: t('.property-content-info-attribute, .property-content-detail, .house-tags, .tags-wrap, .tags'),
                    decorationText,
                    sourceUrl: url,
                    fullText,
                  };
                }
                if (mode === 'house-cell' || mode === 'list-wrap') {
                  return {
                    mode,
                    title: t('.title a, .title, h3 a, h2 a, a[title]') || fallbackTitle,
                    community: t('.baseinfo a, .list-info a, .house-title a, .title a'),
                    region: t('.baseinfo span:last-child, .baseinfo, .address, .house-address'),
                    houseInfo: t('.baseinfo, .room, .details-item'),
                    timeText: t('.update, .time, .baseinfo, .address'),
                    totalPriceText: t('.price .sum, .price b, .sum, .money strong'),
                    unitPriceText: t('.price .unit, .unit, .money span'),
                    rawMetaText: t('.property-content-info-attribute, .property-content-detail, .house-tags, .tags-wrap, .tags'),
                    decorationText,
                    sourceUrl: url,
                    fullText,
                  };
                }
                return {
                  mode,
                  title: t('.title') || fallbackTitle,
                  community: t('.baseinfo a, .add a'),
                  region: t('.add'),
                  houseInfo: t('.baseinfo'),
                  timeText: t('.baseinfo, .add, .time'),
                  totalPriceText: t('.price .sum, .price b, .sum'),
                  unitPriceText: t('.price .unit, .unit'),
                  rawMetaText: t('.property-content-info-attribute, .property-content-detail, .house-tags, .tags-wrap, .tags'),
                  decorationText,
                  sourceUrl: url,
                  fullText,
                };
              }).filter(c => c.title || c.community || c.fullText);
            }
            """
        )

        out: List[Dict[str, Any]] = []
        strict_region_mode = bool(ctx.regions and region and region != "全城")
        page_region_hint = self._infer_region(page.url)
        for raw in raw_list:
            full_text = raw.get("fullText") or ""
            time_text = raw.get("timeText") or full_text
            if not self.within_since_days(time_text, ctx.since_days):
                continue
            detected_region = self._infer_region(
                raw.get("region"),
                raw.get("houseInfo"),
                raw.get("title"),
                raw.get("community"),
                raw.get("sourceUrl"),
                raw.get("fullText"),
            )
            resolved_region = detected_region or page_region_hint
            # 严格模式：必须识别到区县且与目标区县一致，避免“选文水/交城却入库离石”。
            if strict_region_mode and resolved_region != region:
                continue
            # 非严格模式下，仅过滤掉明确识别为其它区县的数据。
            if not strict_region_mode and resolved_region and resolved_region != region:
                continue
            info = raw.get("houseInfo") or full_text
            # Common pattern: "3室2厅 | 105㎡ | 南北 | 中楼层(共15层)"
            parts = [p.strip() for p in info.replace("·", "|").split("|") if p.strip()]
            layout = next((p for p in parts if "室" in p), "")
            if layout and "卫" not in layout:
                m_layout_full = re.search(r"(\d+)\s*室\s*(\d+)\s*厅\s*(\d+)\s*卫", info)
                if not m_layout_full and full_text:
                    m_layout_full = re.search(
                        r"(\d+)\s*室\s*(\d+)\s*厅\s*(\d+)\s*卫",
                        full_text,
                    )
                if m_layout_full:
                    layout = (
                        f"{m_layout_full.group(1)}室"
                        f"{m_layout_full.group(2)}厅"
                        f"{m_layout_full.group(3)}卫"
                    )
            area_text = next((p for p in parts if normalize.parse_area(p) is not None), "")
            if not area_text and full_text:
                m_area = _AREA_TEXT_RE.search(full_text)
                area_text = m_area.group(0) if m_area else ""
            if not area_text and raw.get("title"):
                m_area = _AREA_TEXT_RE.search(raw.get("title") or "")
                area_text = m_area.group(0) if m_area else ""
            orientation = next((p for p in parts if any(c in p for c in "东南西北")), "")
            if not orientation and full_text:
                orientation = normalize.parse_orientation(full_text)
            decoration = next(
                (p for p in parts if any(k in p for k in ("精装", "简装", "毛坯", "毛胚", "装修"))),
                "",
            )
            if not decoration:
                decoration = normalize.parse_decoration(raw.get("decorationText") or "")
            if not decoration:
                decoration = normalize.parse_decoration(raw.get("rawMetaText") or "")
            if not decoration and full_text:
                decoration = normalize.parse_decoration(full_text)
            floor_text = next((p for p in parts if "层" in p), "")
            if not floor_text and full_text:
                m_floor = _FLOOR_TEXT_RE.search(full_text)
                floor_text = m_floor.group(0).strip() if m_floor else ""
            total_price_text = raw.get("totalPriceText") or ""
            if not total_price_text and full_text:
                m_total = _TOTAL_PRICE_RE.search(full_text)
                total_price_text = m_total.group(0) if m_total else ""
            if not total_price_text and raw.get("title"):
                m_total = _TOTAL_PRICE_RE.search(raw.get("title") or "")
                total_price_text = m_total.group(0) if m_total else ""
            unit_price_text = raw.get("unitPriceText") or ""
            if not unit_price_text and full_text:
                m_unit = _UNIT_PRICE_RE.search(full_text)
                unit_price_text = m_unit.group(0) if m_unit else ""
            if not unit_price_text and raw.get("title"):
                m_unit = _UNIT_PRICE_RE.search(raw.get("title") or "")
                unit_price_text = m_unit.group(0) if m_unit else ""
            parsed_total = normalize.parse_total_price(total_price_text)
            parsed_area = normalize.parse_area(area_text)
            parsed_unit = normalize.parse_unit_price(unit_price_text)
            if parsed_total is None and parsed_area and parsed_unit:
                parsed_total = round(parsed_area * parsed_unit / 10000.0, 1)
            if parsed_area is None and parsed_total and parsed_unit:
                parsed_area = round(parsed_total * 10000.0 / parsed_unit, 1)
            if not parsed_area and full_text:
                parsed_area = normalize.parse_area(full_text)

            rec = normalize.finalise(
                {
                    "community": raw.get("community")
                    or raw.get("title")
                    or (full_text.split(" ")[0] if full_text else ""),
                    "region": resolved_region or region,
                    "floor": floor_text,
                    "totalPrice": parsed_total,
                    "totalPriceText": total_price_text,
                    "unitPrice": parsed_unit,
                    "unitPriceText": unit_price_text,
                    "area": parsed_area,
                    "areaText": area_text,
                    "layout": layout,
                    "decoration": decoration,
                    "orientation": orientation,
                    "houseInfo": info,
                    "sourceUrl": raw.get("sourceUrl"),
                },
                platform=self.key,
            )
            if normalize.is_complete(rec):
                out.append(rec)
        return out

    def _infer_region(self, *values: Any) -> str:
        text = " ".join(str(v or "") for v in values)
        if not text:
            return ""
        compact = unquote(text).replace(" ", "")
        compact_lower = compact.lower()
        for slug, region in self._SLUG_TO_REGION.items():
            if f"/{slug}/" in compact_lower or f"-{slug}-" in compact_lower:
                return region
        for region, aliases in self._REGION_ALIASES.items():
            # 兼容卡片文案常见短写（如“离石”“交城”），避免只命中“离石区/交城县”。
            if region in compact:
                return region
            if any(alias in compact for alias in aliases):
                return region
        return ""
