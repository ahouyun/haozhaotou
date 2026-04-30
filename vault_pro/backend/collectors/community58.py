from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from .base import CollectorContext, ProgressCallback, async_playwright
from .tongcheng58 import Tongcheng58Collector


BASE_URL = "https://lvliang.58.com/xiaoqu/"
_PRICE_RE = re.compile(
    r"(\d[\d,]{2,})\s*元(?:/|／)?\s*(?:㎡|m²|m2|平米|平方米|平)",
    re.IGNORECASE,
)
_PRICE_FALLBACK_RE = re.compile(r"(?:均价|参考价)\s*[:：]?\s*(\d[\d,]{2,})")
_REGIONS = [
    "离石",
    "孝义",
    "汾阳",
    "文水",
    "临县",
    "交城",
    "柳林",
    "中阳",
    "方山县",
    "岚县",
    "交口县",
    "兴县",
    "石楼县",
    "吕梁周边",
]


class Community58PriceCollector(Tongcheng58Collector):
    key = "58_xiaoqu"
    display_name = "58 同城·小区均价"
    max_pages = 20

    async def collect_prices(
        self,
        *,
        start_url: str,
        max_pages: int,
        risk_wait_seconds: int,
        anti_detection_level: str,
        on_progress: Optional[ProgressCallback] = None,
    ) -> List[Dict[str, object]]:
        if async_playwright is None:
            raise RuntimeError(
                "Playwright 未安装。请运行 `pip install playwright` 与 `python -m playwright install chromium`。"
            )
        self.anti_detection_mode = True
        self.anti_detection_level = self._normalize_anti_detection_level(anti_detection_level)
        ctx = CollectorContext(
            target=1,
            risk_wait_seconds=max(60, int(risk_wait_seconds or 180)),
            on_progress=on_progress,
        )
        safe_url = str(start_url or "").strip() or BASE_URL
        page_cap = max(1, min(int(max_pages or 1), self.max_pages))

        async with async_playwright() as pw:
            browser_ctx = await self._launch_with_fallback(pw)
            try:
                page = await browser_ctx.new_page()
                await self._stealth(page)
                return await self._crawl_pages(
                    page=page,
                    ctx=ctx,
                    start_url=safe_url,
                    max_pages=page_cap,
                )
            finally:
                await browser_ctx.close()

    async def _crawl_pages(
        self,
        *,
        page,
        ctx: CollectorContext,
        start_url: str,
        max_pages: int,
    ) -> List[Dict[str, object]]:
        out: List[Dict[str, object]] = []
        seen_keys: Set[Tuple[str, str]] = set()
        visited_urls: Set[str] = set()
        current_url = start_url

        for page_no in range(1, max_pages + 1):
            await ctx.report(
                min(0.9, 0.08 + page_no / max_pages * 0.72),
                f"[{self.display_name}] 正在采集第 {page_no}/{max_pages} 页 ...",
            )
            ok = await self.safe_goto(page, current_url, timeout_ms=30_000)
            if not ok:
                raise RuntimeError(f"打开页面失败：{current_url}（{self.last_goto_error or '未知错误'}）")

            if await self.is_risk_control_page(page):
                cleared = await self.wait_for_risk_control_clear(
                    page,
                    ctx,
                    max_wait_seconds=max(60, int(ctx.risk_wait_seconds or 180)),
                )
                if not cleared:
                    reason = self.last_risk_control_reason or "验证码/风控等待超时"
                    raise RuntimeError(f"小区均价采集中断：{reason}")
                ok = await self.safe_goto(page, current_url, timeout_ms=30_000)
                if not ok:
                    raise RuntimeError(f"验证通过后页面重载失败：{current_url}")

            rows = await self._extract_price_rows(page)
            if not rows and page_no == 1:
                raise RuntimeError("未识别到小区均价列表（可能是页面结构变化或触发反爬）")
            if not rows:
                break

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            appended = 0
            for row in rows:
                key = (str(row.get("community") or ""), str(row.get("sourceUrl") or ""))
                if not key[0] or key in seen_keys:
                    continue
                seen_keys.add(key)
                out.append(
                    {
                        "community": row.get("community") or "",
                        "region": row.get("region") or "",
                        "unitPrice": int(row.get("unitPrice") or 0),
                        "sourceUrl": row.get("sourceUrl") or "",
                        "date": ts,
                        "platform": "58_xiaoqu",
                    }
                )
                appended += 1

            next_url = await self._extract_next_page_url(page)
            if appended == 0 or not next_url:
                break
            if next_url in visited_urls:
                break
            visited_urls.add(current_url)
            current_url = next_url
            await self.throttle()

        return out

    async def _extract_price_rows(self, page) -> List[Dict[str, object]]:
        raw_rows = await page.evaluate(
            """
            () => {
              const clean = (txt) => String(txt || '').replace(/\\s+/g, ' ').trim();
              const cardSelectors = [
                '.xiaoqu-card',
                '.xiaoqu-list-item',
                '.list-item',
                '.house-cell',
                '.property',
                '.list-wrap li',
                '.list li'
              ];
              let cards = [];
              for (const sel of cardSelectors) {
                cards = Array.from(document.querySelectorAll(sel));
                if (cards.length > 0) break;
              }
              if (cards.length === 0) {
                const anchors = Array.from(document.querySelectorAll('a[href*="/xiaoqu/"]'));
                cards = anchors.map(a => a.closest('li,div') || a).filter(Boolean);
              }
              return cards.map((card) => {
                const q = (sel) => card.querySelector(sel);
                const titleEl =
                  q('.title a, .title, .xiaoqu-name a, .xiaoqu-name, h3 a, h2 a, a[title]')
                  || q('a[href*="/xiaoqu/"]');
                const linkEl = q('a[href*="/xiaoqu/"]') || titleEl;
                const community = clean((titleEl && (titleEl.getAttribute('title') || titleEl.textContent)) || '');
                const sourceUrl = linkEl ? new URL(linkEl.getAttribute('href') || '', location.href).href : '';
                const priceText = clean([
                  q('.price'),
                  q('.unit-price'),
                  q('.xiaoqu-price'),
                  q('.price-num'),
                  q('.total-price'),
                ].map(el => (el ? el.textContent : '')).join(' '));
                const regionText = clean([
                  q('.address'),
                  q('.area'),
                  q('.region'),
                  q('.desc'),
                  q('.xiaoqu-address'),
                ].map(el => (el ? el.textContent : '')).join(' '));
                const fullText = clean(card.innerText || '');
                return { community, sourceUrl, priceText, regionText, fullText };
              });
            }
            """
        )
        out: List[Dict[str, object]] = []
        for row in raw_rows or []:
            community = str(row.get("community") or "").strip()
            if not community:
                continue
            price_text = " ".join([str(row.get("priceText") or ""), str(row.get("fullText") or "")])
            unit_price = self._parse_unit_price(price_text)
            if unit_price <= 0:
                continue
            out.append(
                {
                    "community": community,
                    "region": self._infer_region(str(row.get("regionText") or ""), str(row.get("fullText") or "")),
                    "unitPrice": unit_price,
                    "sourceUrl": str(row.get("sourceUrl") or "").strip(),
                }
            )
        return out

    def _parse_unit_price(self, text: str) -> int:
        raw = str(text or "")
        m = _PRICE_RE.search(raw)
        if m:
            return int(str(m.group(1)).replace(",", ""))
        m2 = _PRICE_FALLBACK_RE.search(raw)
        if m2:
            return int(str(m2.group(1)).replace(",", ""))
        return 0

    def _infer_region(self, *values: str) -> str:
        joined = " ".join(str(v or "") for v in values)
        compact = re.sub(r"\\s+", "", joined)
        for token in _REGIONS:
            if token in compact:
                return token
        return ""

    async def _extract_next_page_url(self, page) -> str:
        next_url = await page.evaluate(
            """
            () => {
              const links = Array.from(
                document.querySelectorAll('a[rel="next"], .next, .next-page, .pager a, .page a')
              );
              const clean = (txt) => String(txt || '').replace(/\\s+/g, '').toLowerCase();
              for (const link of links) {
                const label = clean(link.textContent || '') + clean(link.getAttribute('title') || '');
                if (!label || !(/下一页|下页|next|>|>>/.test(label))) continue;
                if (
                  link.matches('.disabled,[disabled],.off,.inactive')
                  || link.getAttribute('aria-disabled') === 'true'
                ) {
                  continue;
                }
                const href = link.getAttribute('href') || '';
                if (!href || href === '#' || href.startsWith('javascript:')) continue;
                return new URL(href, location.href).href;
              }
              return '';
            }
            """
        )
        return str(next_url or "").strip()

