"""Anjuke (lvliang.anjuke.com) second-hand listing collector.

Collector strategy:
- Use Playwright with real browser context (persistent profile).
- Turn pages by URL pattern analysis ("/sale/.../p{n}/"), not forced DOM clicks.
- Keep a manual risk-control window when captcha/verification appears.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List
from urllib.parse import quote, unquote, urljoin

from .base import BaseCollector, BrowserContext, CollectorContext, Page
from .. import normalize


log = logging.getLogger("vault_pro.anjuke")

BASE_URL = "https://lvliang.anjuke.com"
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
    "离石": "lishi",
    "孝义": "xiaoyi",
    "汾阳": "fenyang",
    "文水": "wenshui",
    "临县": "linxian",
    "交城": "jiaocheng",
    "柳林": "liulin",
    "中阳": "zhongyang",
    "方山县": "fangshan",
    "岚县": "lanxian",
    "交口县": "jiaokou",
    "兴县": "xingxian",
    "石楼县": "shilou",
    "吕梁周边": "lvliangzhoubian",
}


class AnjukeCollector(BaseCollector):
    key = "anjuke"
    display_name = "安居客·吕梁"
    max_pages = 6
    _REGION_ALIASES: Dict[str, tuple[str, ...]] = {
        "离石": ("离石区",),
        "孝义": ("孝义市",),
        "汾阳": ("汾阳市",),
        "文水": ("文水县",),
        "临县": ("临县",),
        "交城": ("交城县",),
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
                # Multi-region mode should be resilient: one district failure
                # must not abort already collected data from other districts.
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
        entry_url = await self._resolve_region_entry_url(page, ctx=ctx, region=region, slug=slug)
        for page_no in range(1, max_pages + 1):
            if len(out) >= want:
                break
            urls = self._build_urls(region, slug, page_no, entry_url=entry_url)
            if page_no > 1:
                urls = self._merge_unique_urls(urls + self._build_urls_from_current(page.url, page_no))
            cards: List[Dict[str, Any]] = []
            for try_idx, url in enumerate(urls, start=1):
                await ctx.report(
                    min(0.95, 0.1 + page_no * 0.08),
                    f"[{self.display_name}] {region} p{page_no}（尝试 {try_idx}/{len(urls)}）...",
                )
                ok = await self.safe_goto(page, url)
                if not ok:
                    continue
                await self.maybe_wait_for_login(page, ctx, signal_text="sale")
                if page_no > 1:
                    reached_expected_page = await self._is_expected_page_no(page, page_no)
                    if not reached_expected_page:
                        await self.throttle(base=0.5, jitter=0.2)
                        continue
                cards = await self._extract_cards(page, region, ctx)
                if not cards and page_no == 1 and try_idx == 1:
                    cleared = await self.wait_for_risk_control_clear(
                        page,
                        ctx,
                        max_wait_seconds=max(60, int(ctx.risk_wait_seconds or 180)),
                    )
                    if not cleared:
                        reason = self.last_risk_control_reason or (
                            f"验证等待超时（{max(60, int(ctx.risk_wait_seconds or 180))}s）"
                        )
                        raise RuntimeError(f"{region} {reason}，请完成验证或切换网络后重试")
                    await self.safe_goto(page, url)
                    await self.throttle(base=0.8, jitter=0.6)
                    cards = await self._extract_cards(page, region, ctx)
                if cards and page_no > 1:
                    page_sources = {
                        str(x.get("sourceUrl") or "").strip()
                        for x in cards
                        if str(x.get("sourceUrl") or "").strip()
                    }
                    if page_sources and prev_page_sources and page_sources == prev_page_sources:
                        cards = []
                        await self.throttle(base=0.4, jitter=0.3)
                        continue
                if cards:
                    break
                await self.throttle(base=0.4, jitter=0.4)
            if not cards:
                if page_no == 1:
                    raise RuntimeError(f"{region} 首屏未提取到数据（可能触发验证或页面结构变更）")
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

    async def _resolve_region_entry_url(
        self,
        page: Page,
        *,
        ctx: CollectorContext,
        region: str,
        slug: str,
    ) -> str:
        """Resolve exact area-list URL from Anjuke page navigation when possible."""
        base_url = self._base_url()
        if region == "全城":
            return urljoin(base_url + "/", "sale/")

        aliases = self._REGION_ALIASES.get(region, ())
        tokens = [region, *aliases]
        tokens_js = "[" + ",".join(repr(x) for x in tokens if x) + "]"
        probe_urls = self._merge_unique_urls(
            [
                urljoin(base_url + "/", f"sale/{slug}/") if slug else "",
                urljoin(base_url + "/", "sale/"),
            ]
        )
        for probe in probe_urls:
            ok = await self.safe_goto(page, probe)
            if not ok:
                continue
            await self.maybe_wait_for_login(page, ctx, signal_text="sale")
            try:
                nav_href = await page.evaluate(
                    f"""
                    () => {{
                      const normalize = (txt) => String(txt || '').replace(/\\s+/g, '');
                      const tokens = {tokens_js}.map(normalize).filter(Boolean);
                      const selectors = [
                        '.items a', '.sub-items a', '.region-list a', '.filter-area a',
                        '.filter-item a', '.search-nav a', '.filter-wrap a'
                      ];
                      const links = selectors.flatMap(sel => Array.from(document.querySelectorAll(sel)));
                      const unique = Array.from(new Set(links));
                      const pickBy = (matcher) => unique.find(a => matcher(normalize(a.textContent || '')));
                      let hit =
                        pickBy(txt => tokens.some(t => txt === t)) ||
                        pickBy(txt => tokens.some(t => txt.includes(t))) ||
                        pickBy(txt => tokens.some(t => t.includes(txt)));
                      if (!hit) return '';
                      return String(hit.href || '').trim();
                    }}
                    """
                )
            except Exception:  # noqa: BLE001
                nav_href = ""
            href = str(nav_href or "").strip()
            if href:
                return href

        if slug:
            return urljoin(base_url + "/", f"sale/{slug}/")
        return urljoin(base_url + "/", f"sale/?kw={quote(region)}")

    def _build_urls(self, region: str, slug: str, page_no: int, *, entry_url: str = "") -> List[str]:
        base_url = self._base_url()
        from_entry = self._build_urls_from_entry(entry_url, page_no)
        if from_entry:
            fallback = []
            if slug:
                fallback = (
                    [urljoin(base_url + "/", f"sale/{slug}/p{page_no}/"), urljoin(base_url + "/", f"sale/p{page_no}/?kw={quote(region)}")]
                    if page_no > 1
                    else [urljoin(base_url + "/", f"sale/{slug}/"), urljoin(base_url + "/", f"sale/?kw={quote(region)}")]
                )
            else:
                suffix = f"sale/p{page_no}/" if page_no > 1 else "sale/"
                fallback = [urljoin(base_url + "/", suffix)]
            return self._merge_unique_urls(from_entry + fallback)
        if not slug:
            suffix = f"sale/p{page_no}/" if page_no > 1 else "sale/"
            return [urljoin(base_url + "/", suffix)]
        if page_no > 1:
            return [
                urljoin(base_url + "/", f"sale/{slug}/p{page_no}/"),
                urljoin(base_url + "/", f"sale/p{page_no}/?kw={quote(region)}"),
            ]
        return [
            urljoin(base_url + "/", f"sale/{slug}/"),
            urljoin(base_url + "/", f"sale/?kw={quote(region)}"),
        ]

    def _build_urls_from_entry(self, entry_url: str, page_no: int) -> List[str]:
        src = str(entry_url or "").strip()
        if not src:
            return []
        if page_no <= 1:
            return [src]

        candidates: List[str] = []
        replaced = re.sub(r"/p\d+/", f"/p{page_no}/", src)
        if replaced != src:
            candidates.append(replaced)
        else:
            if "?" in src:
                replaced_q = re.sub(r"(\?.*)$", f"/p{page_no}/\\1", src)
                if replaced_q != src:
                    candidates.append(replaced_q)
            if src.endswith("/"):
                candidates.append(src + f"p{page_no}/")
            else:
                candidates.append(src + f"/p{page_no}/")
        return self._merge_unique_urls(candidates)

    def _build_urls_from_current(self, current_url: str, page_no: int) -> List[str]:
        if page_no <= 1:
            return []
        src = str(current_url or "").strip()
        if not src or "/sale/" not in src:
            return []
        candidates: List[str] = []
        replaced = re.sub(r"/p\d+/", f"/p{page_no}/", src)
        if replaced != src:
            candidates.append(replaced)
        else:
            if src.endswith("/"):
                candidates.append(src + f"p{page_no}/")
            else:
                candidates.append(src + f"/p{page_no}/")
        if "?" in src:
            candidates.append(re.sub(r"(\?.*)$", f"/p{page_no}/\\1", src))
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
                    '.multi-page .aN, .multi-page .curr, .page-box .cur, .pagination .cur, .pager .cur'
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
        if f"/p{page_no}/" in url_txt:
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
                ".property, .list-item, .house-list-item, .list-content, .sale-house",
                timeout=8000,
            )
        except Exception:  # noqa: BLE001
            return []

        raw_list: List[Dict[str, Any]] = await page.evaluate(
            """
            () => {
              const select = (sel, root) => Array.from((root||document).querySelectorAll(sel));
              const pickDecorationNearArea = (card) => {
                const norm = (txt) => String(txt || '').trim().replace(/\\s+/g, ' ');
                const hasDeco = (txt) => /(精装|精装修|简装|简装修|毛坯|毛胚|清水|装修|拎包入住)/.test(txt);
                const hasArea = (txt) => /\\d[\\d,.]*\\s*(㎡|m²|m2|平米|平方米|平)/i.test(txt);
                const seq = select(
                  '.property-content-info-text span, .property-content-info-text div, .property-content-info-text p, '
                  + '.property-content-info-attribute span, .property-content-info-attribute div, .property-content-info-attribute p, '
                  + '.property-content-detail span, .property-content-detail div, .property-content-detail p, '
                  + '.house-tags span, .tags span, .tags-wrap span',
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
              if (cards.length === 0) { cards = select('.list-item, .house-list-item'); mode = 'list-item'; }
              if (cards.length === 0) { cards = select('.list-content > li, .sale-house, .house-item'); mode = 'fallback'; }

              return cards.map(card => {
                const t = (sel) => {
                  const el = card.querySelector(sel);
                  return el ? (el.textContent || '').trim().replace(/\\s+/g, ' ') : '';
                };
                const link = card.querySelector('a[href*="/prop/view/"], a[href*="/sale/"], a');
                const url = link ? (link.href || '') : '';
                const fullText = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                const linkText = link ? ((link.getAttribute('title') || link.textContent || '').trim().replace(/\\s+/g, ' ')) : '';
                const fallbackTitle = (linkText || fullText.split(' ').slice(0, 5).join(' ') || '').trim();
                const decorationText = pickDecorationNearArea(card);
                if (mode === 'property') {
                  return {
                    mode,
                    title: t('.property-content-title-name, .property-content-title a, .property-title, .title, h3 a') || fallbackTitle,
                    community: t('.property-content-info-comm-name, .property-content-info-comm a, .property-content-info a, .baseinfo a'),
                    region: t('.property-content-info-comm-address, .property-content-info-comm-address a, .address, .add'),
                    houseInfo: t('.property-content-info-text, .property-content-info, .baseinfo'),
                    timeText: t('.property-content-info-comm-address, .property-content-info-comm-name, .time'),
                    totalPriceText: t('.property-price-total, .price-det, .price'),
                    unitPriceText: t('.property-price-average, .unit-price, .unit'),
                    rawMetaText: t('.property-content-info-attribute, .property-content-detail, .house-tags, .tags-wrap, .tags'),
                    decorationText,
                    sourceUrl: url,
                    fullText,
                  };
                }
                return {
                  mode,
                  title: t('.title a, .title, h3 a, h2 a, a[title]') || fallbackTitle,
                  community: t('.baseinfo a, .list-info a, .house-title a, .title a'),
                  region: t('.baseinfo span:last-child, .baseinfo, .address, .house-address, .area'),
                  houseInfo: t('.baseinfo, .room, .details-item, .property-content-info'),
                  timeText: t('.update, .time, .baseinfo, .address, .tags'),
                  totalPriceText: t('.price .sum, .price b, .sum, .money strong, .price-det'),
                  unitPriceText: t('.price .unit, .unit, .money span, .unit-price'),
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
            # Strict mode: only keep records whose region can be confidently
            # inferred and exactly matches the requested district.
            if strict_region_mode and resolved_region != region:
                continue
            if not strict_region_mode and resolved_region and resolved_region != region:
                continue

            info = raw.get("houseInfo") or full_text
            parts = [p.strip() for p in info.replace("·", "|").split("|") if p.strip()]
            layout = next((p for p in parts if "室" in p), "")
            if layout and "卫" not in layout:
                m_layout_full = re.search(r"(\d+)\s*室\s*(\d+)\s*厅\s*(\d+)\s*卫", info)
                if not m_layout_full and full_text:
                    m_layout_full = re.search(r"(\d+)\s*室\s*(\d+)\s*厅\s*(\d+)\s*卫", full_text)
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
            unit_price_text = raw.get("unitPriceText") or ""
            if not unit_price_text and full_text:
                m_unit = _UNIT_PRICE_RE.search(full_text)
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
            if region in compact:
                return region
            if any(alias in compact for alias in aliases):
                return region
        return ""
