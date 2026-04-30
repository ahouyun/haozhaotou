"""Beike (lvliang.ke.com) second-hand listing collector.

Beike's Lvliang station follows the same DOM layout as the rest of its
country-wide network: a list at /ershoufang/{regionPinyin?}/pg{n}/, where
each card is a ``.sellListContent .clear`` block.

Region URL slugs differ by city. We map the 13 standard Lvliang regions
to the slugs Beike uses; if a slug is unknown we just skip the region
parameter (the city-wide list still works).

Be polite: max 8 pages per platform run, 1.5-3.5s between page loads.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List
from urllib.parse import urljoin

from .base import BaseCollector, BrowserContext, CollectorContext, Page
from .. import normalize


log = logging.getLogger("vault_pro.beike")

BASE_URL = "https://lvliang.ke.com"

REGION_SLUGS: Dict[str, str] = {
    "离石区": "lishiqu",
    "孝义市": "xiaoyishi",
    "汾阳市": "fenyangshi",
    "文水县": "wenshuixian",
    "交城县": "jiaochengxian",
    "兴县": "xingxian1",
    "临县": "linxian1",
    "柳林县": "liulinxian",
    "石楼县": "shilouxian",
    "岚县": "lanxian",
    "方山县": "fangshanxian",
    "中阳县": "zhongyangxian",
    "交口县": "jiaokouxian",
}


class BeikeCollector(BaseCollector):
    key = "beike"
    display_name = "贝壳·吕梁"
    max_pages = 8

    def _base_url(self) -> str:
        return self.resolve_base_url(BASE_URL)

    def _default_regions(self, ctx: CollectorContext) -> List[str]:
        if ctx.regions:
            return ctx.regions
        host = (self.runtime_host or "").lower()
        if "lvliang" in host or host.startswith("ll."):
            return list(REGION_SLUGS.keys())
        # Non-Lvliang cities likely use different district slugs.
        return ["全城"]

    async def _collect_one_platform(
        self,
        context: BrowserContext,
        page: Page,
        ctx: CollectorContext,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        # Build region queue: requested regions or all known regions.
        regions = self._default_regions(ctx)

        for region in regions:
            if len(results) >= max_items:
                break
            slug = REGION_SLUGS.get(region, "")
            picked = await self._collect_region(
                page, ctx, region=region, slug=slug, want=max_items - len(results)
            )
            results.extend(picked)

        return results

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
        max_pages = 1 if ctx.fixed_window else self.max_pages
        for page_no in range(1, max_pages + 1):
            if len(out) >= want:
                break
            url = self._build_url(slug, page_no)
            await ctx.report(
                min(0.95, 0.1 + page_no * 0.08),
                f"[{self.display_name}] {region} pg{page_no} ...",
            )
            ok = await self.safe_goto(page, url)
            if not ok:
                if page_no == 1:
                    reason = self.last_goto_error or "网络连接异常"
                    raise RuntimeError(f"{region} 列表页访问失败：{reason}")
                break

            # Beike usually shows a login wall after the 2nd-3rd anonymous
            # navigation. Detect it via a known anchor in logged-in pages.
            await self.maybe_wait_for_login(page, ctx, signal_text="sellListContent")
            if await self.is_risk_control_page(page):
                cleared = await self.wait_for_risk_control_clear(
                    page,
                    ctx,
                    max_wait_seconds=max(60, int(ctx.risk_wait_seconds or 180)),
                )
                if not cleared:
                    if page_no == 1:
                        reason = self.last_risk_control_reason or (
                            f"验证等待超时（{max(60, int(ctx.risk_wait_seconds or 180))}s）"
                        )
                        raise RuntimeError(
                            f"{region} {reason}，请完成验证或切换网络后重试"
                        )
                    break

            cards = await self._extract_cards(page, region)
            if not cards:
                # Either empty page or blocked. Stop pagination for this region.
                if page_no == 1:
                    raise RuntimeError(f"{region} 首屏未提取到数据（可能触发验证或页面结构变更）")
                break
            out.extend(cards)
            await self.throttle()

        return out[:want]

    def _build_url(self, slug: str, page_no: int) -> str:
        # Beike's "newest first" sort tag is rs sortByLastUpdate or co32
        # (newest published); we use co32 which is documented to mean
        # "publication time descending" across ke.com sites.
        suffix = f"pg{page_no}co32/" if page_no > 1 else "co32/"
        base_url = self._base_url()
        if slug:
            return urljoin(base_url + "/", f"ershoufang/{slug}/{suffix}")
        return urljoin(base_url + "/", f"ershoufang/{suffix}")

    async def _extract_cards(self, page: Page, region: str) -> List[Dict[str, Any]]:
        # Use a single page.evaluate so we don't pay per-element CDP cost.
        try:
            await page.wait_for_selector(".sellListContent .clear", timeout=8000)
        except Exception:  # noqa: BLE001
            return []

        raw_list: List[Dict[str, Any]] = await page.evaluate(
            """
            () => {
              const cards = Array.from(document.querySelectorAll('.sellListContent > li.clear'));
              return cards.map(card => {
                const q = sel => {
                  const el = card.querySelector(sel);
                  return el ? el.textContent.trim() : '';
                };
                const link = card.querySelector('.title a');
                return {
                  community:        q('.positionInfo a:first-child'),
                  positionDistrict: q('.positionInfo a:nth-child(2)'),
                  houseInfo:        q('.houseInfo'),       // "3室2厅 | 105㎡ | 南北 | 精装 | 中楼层(共15层) | 板楼"
                  totalPriceText:   q('.totalPrice'),       // "57万"
                  unitPriceText:    q('.unitPrice'),        // "5,429元/平"
                  followInfo:       q('.followInfo'),       // "10人关注 / 7天前发布"
                  title:            link ? link.textContent.trim() : '',
                  sourceUrl:        link ? link.href : '',
                };
              });
            }
            """
        )

        out: List[Dict[str, Any]] = []
        for raw in raw_list:
            follow_info = raw.get("followInfo") or ""
            if not self.within_since_days(follow_info, ctx.since_days):
                continue
            parts = [p.strip() for p in (raw.get("houseInfo") or "").split("|")]
            layout = parts[0] if len(parts) > 0 else ""
            area_text = parts[1] if len(parts) > 1 else ""
            orientation = parts[2] if len(parts) > 2 else ""
            decoration = next(
                (p for p in parts if any(k in p for k in ("精装", "简装", "毛坯", "毛胚", "装修"))),
                "",
            )
            floor_text = parts[4] if len(parts) > 4 else ""

            rec = normalize.finalise(
                {
                    "community": raw.get("community"),
                    "region": raw.get("positionDistrict") or region,
                    "floor": floor_text,
                    "totalPriceText": raw.get("totalPriceText"),
                    "unitPriceText": raw.get("unitPriceText"),
                    "areaText": area_text,
                    "layout": layout,
                    "decoration": decoration,
                    "orientation": orientation,
                    "houseInfo": raw.get("houseInfo"),
                    "sourceUrl": raw.get("sourceUrl"),
                },
                platform=self.key,
            )
            if normalize.is_complete(rec):
                out.append(rec)
        return out
