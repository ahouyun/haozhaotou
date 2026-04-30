"""Lianjia (lvliang.lianjia.com) collector.

Lvliang historically does NOT have a dedicated lianjia subdomain - the
prefecture-level cities of Shanxi province are generally only present on
ke.com (Beike). We still attempt the canonical subdomain so that if/when
Lianjia opens it the collector continues to work; otherwise we gracefully
return [] so the orchestrator can move on to the next platform.

The DOM layout is identical to Beike (same parent company), so we reuse
BeikeCollector's extraction logic.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List
from urllib.parse import urljoin

from .base import BrowserContext, CollectorContext, Page
from .beike import BeikeCollector


log = logging.getLogger("vault_pro.lianjia")

BASE_URL = "https://lvliang.lianjia.com"


class LianjiaCollector(BeikeCollector):
    key = "lianjia"
    display_name = "链家·吕梁"

    async def _collect_one_platform(
        self,
        context: BrowserContext,
        page: Page,
        ctx: CollectorContext,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        # Probe whether lvliang.lianjia.com actually serves a listing.
        ok = await self.safe_goto(page, self._base_url() + "/ershoufang/", timeout_ms=15000)
        if not ok:
            reason = self.last_goto_error or "网络连接异常"
            raise RuntimeError(f"链家站访问失败：{reason}")
        try:
            await page.wait_for_selector(".sellListContent, .ershoufang", timeout=5000)
        except Exception:  # noqa: BLE001
            await ctx.report(0.4, "[链家] 吕梁站疑无房源数据，跳过")
            return []
        return await super()._collect_one_platform(context, page, ctx, max_items)

    def _base_url(self) -> str:
        return self.resolve_base_url(BASE_URL)

    def _build_url(self, slug: str, page_no: int) -> str:
        suffix = f"pg{page_no}co32/" if page_no > 1 else "co32/"
        base_url = self._base_url()
        if slug:
            return urljoin(base_url + "/", f"ershoufang/{slug}/{suffix}")
        return urljoin(base_url + "/", f"ershoufang/{suffix}")
