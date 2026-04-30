"""Field normalisation, derivation and de-duplication helpers.

The frontend's data model is:
    { id, community, region, floor, totalPrice, unitPrice, area,
      layout, decoration, orientation, date, sourceUrl?, platform? }

Collectors return raw text fragments; this module standardises the values
so the review panel can display them and the IndexedDB can store them.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import html
import re
from typing import Any, Dict, Optional


_NUM_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")
_LAYOUT_RE = re.compile(r"(\d+)\s*室\s*(\d+)\s*厅(?:\s*(\d+)\s*卫)?")
_FLOOR_DESC_RE = re.compile(r"(低|中|高|底|顶|地下)(?:楼)?层")
_FLOOR_TOTAL_RE = re.compile(r"(?:共|总高|总)\s*(\d+)\s*层")
_FLOOR_NUMERIC_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
_AREA_WITH_UNIT_RE = re.compile(
    r"(\d[\d,]*(?:\.\d+)?)\s*(?:㎡|m²|m2|平米|平方米|平)\b",
    re.IGNORECASE,
)
_UNIT_PRICE_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*元")
_MIN_REASONABLE_AREA = 8.0
_MAX_REASONABLE_AREA = 1500.0
_DECO_FINE_TOKENS = (
    "精装",
    "精装修",
    "精致装修",
    "婚装",
    "拎包入住",
)
_DECO_LUXURY_TOKENS = (
    "豪装",
    "豪华装修",
    "高档装修",
    "高装",
)
_DECO_SIMPLE_TOKENS = (
    "简装",
    "简单装修",
    "简易装修",
    "普通装修",
    "普装",
    "中装",
    "中等装修",
    "基础装修",
)
_DECO_BLANK_TOKENS = (
    "毛坯",
    "毛坏",
    "毛胚",
    "清水",
    "清水房",
    "白坯",
    "未装修",
    "无装修",
)


def _decode_unicode_escape_sequences(text: str) -> str:
    """Decode \\uXXXX style escape sequences without touching other chars."""
    if not text or "\\u" not in text.lower():
        return text

    def _replace(m: re.Match[str]) -> str:
        raw = m.group(1)
        try:
            return chr(int(raw, 16))
        except Exception:  # noqa: BLE001
            return m.group(0)

    return re.sub(r"\\u([0-9a-fA-F]{4})", _replace, text)


def _normalize_decoration_text(text: str) -> str:
    txt = str(text or "")
    if not txt:
        return ""
    txt = html.unescape(txt)
    txt = _decode_unicode_escape_sequences(txt)
    # Keep separators as hints (for "装修: 精装" style snippets).
    txt = re.sub(r"\s+", "", txt)
    return txt


def _classify_decoration_token(token: str) -> str:
    t = _normalize_decoration_text(token)
    if not t:
        return ""
    if any(x in t for x in _DECO_LUXURY_TOKENS):
        return "豪装"
    if any(x in t for x in _DECO_FINE_TOKENS) or any(x in t for x in ("精修", "高端装修")):
        return "精装"
    if any(x in t for x in _DECO_SIMPLE_TOKENS) or any(x in t for x in ("带装修", "有装修")):
        return "简装"
    if any(x in t for x in _DECO_BLANK_TOKENS):
        return "毛坯"
    return ""


def _first_number(text: str) -> Optional[float]:
    if not text:
        return None
    m = _NUM_RE.search(text)
    if not m:
        return None
    token = m.group(0).replace(",", "")
    return float(token)


def parse_total_price(text: str) -> Optional[float]:
    """Parse a Chinese price string into '万' (10k CNY) units.

    Examples: '57万' -> 57.0,  '1,250,000元' -> 125.0
    """
    if not text:
        return None
    n = _first_number(text)
    if n is None:
        return None
    if "万" in text:
        return round(n, 1)
    if "元" in text:  # raw yuan
        return round(n / 10000.0, 1)
    return round(n, 1)


def parse_unit_price(text: str) -> Optional[int]:
    """Unit price in 元/㎡."""
    txt = text or ""
    marker = _UNIT_PRICE_RE.search(txt)
    if marker:
        n = float(marker.group(1).replace(",", ""))
    else:
        n = _first_number(txt)
    if n is None:
        return None
    return int(round(n))


def parse_area(text: str) -> Optional[float]:
    txt = (text or "").strip()
    if not txt:
        return None

    marker = _AREA_WITH_UNIT_RE.search(txt)
    if marker:
        area = round(float(marker.group(1).replace(",", "")), 1)
        if _MIN_REASONABLE_AREA <= area <= _MAX_REASONABLE_AREA:
            return area
        return None

    # Pure numeric areas are valid (e.g. already pre-cleaned "105.3").
    if re.fullmatch(r"\d+(?:\.\d+)?", txt):
        area = round(float(txt), 1)
        if _MIN_REASONABLE_AREA <= area <= _MAX_REASONABLE_AREA:
            return area
        return None

    # Do not fallback to "first number": strings like "4室2厅 ..." would
    # otherwise be parsed as area=4, which is clearly wrong.
    return None


def parse_layout(text: str) -> str:
    if not text:
        return ""
    m = _LAYOUT_RE.search(text)
    if m:
        if m.group(3):
            return f"{m.group(1)}室{m.group(2)}厅{m.group(3)}卫"
        return f"{m.group(1)}室{m.group(2)}厅"
    return text.strip().split()[0][:8]


def parse_decoration(text: str) -> str:
    if not text:
        return ""
    cleaned = _normalize_decoration_text(text)
    if not cleaned:
        return ""

    # Explicit label/value snippets in details or structured blocks.
    label_match = re.search(
        r"(?:装修|装潢|装饰|交付标准|交房标准|装修情况|装修程度)"
        r"[^精简毛坯清水豪普中未无带有]{0,8}"
        r"(精装修?|豪华装修|高档装修|豪装|精装|简装修?|普通装修|中装|普装|毛坯房?|毛胚房?|毛坏|清水房?|未装修|无装修|拎包入住|带装修|有装修)",
        cleaned,
    )
    if label_match:
        return _classify_decoration_token(label_match.group(1))

    # Delivery/fitment phrase variants.
    phrase_match = re.search(
        r"(精装修?|豪华装修|高档装修|豪装|精装|简装修?|普通装修|中装|普装|毛坯房?|毛胚房?|毛坏|清水房?|未装修|无装修|拎包入住|带装修|有装修)"
        r"(?:交付|交房|在售|房源|标准)?",
        cleaned,
    )
    if phrase_match:
        return _classify_decoration_token(phrase_match.group(1))

    if any(token in cleaned for token in _DECO_LUXURY_TOKENS):
        return "豪装"
    if any(token in cleaned for token in _DECO_FINE_TOKENS):
        return "精装"
    if any(token in cleaned for token in _DECO_SIMPLE_TOKENS):
        return "简装"
    if any(token in cleaned for token in _DECO_BLANK_TOKENS):
        return "毛坯"
    if "带装修" in cleaned or "有装修" in cleaned:
        return "简装"
    return ""


def parse_orientation(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"\s+", "", text)
    keep = "".join(ch for ch in cleaned if ch in "东南西北")
    if not keep:
        return cleaned[:4]
    if "南" in keep and "北" in keep:
        return "南北"
    if "东" in keep and "西" in keep and "南" not in keep and "北" not in keep:
        return "东西"
    return keep[:4]


def parse_floor(text: str) -> str:
    if not text:
        return "未知"
    cleaned = re.sub(r"\s+", "", text)
    m = _FLOOR_NUMERIC_RE.search(text)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    total_m = _FLOOR_TOTAL_RE.search(cleaned)
    total = total_m.group(1) if total_m else ""
    exact = ""
    for m2 in re.finditer(r"(\d+)层", cleaned):
        # Avoid treating "共15层/总高18层" as the current floor.
        prefix = cleaned[max(0, m2.start(1) - 2) : m2.start(1)]
        if prefix.endswith("共") or prefix.endswith("总") or prefix.endswith("高"):
            continue
        exact = m2.group(1)
        break
    band_m = _FLOOR_DESC_RE.search(cleaned)
    band = band_m.group(1) if band_m else ""
    if band in ("底", "地下"):
        band = "低"
    elif band == "顶":
        band = "高"
    if exact and total:
        return f"{exact}/{total}"
    if exact:
        return f"{exact}/未知"
    if band and total:
        return f"{band}层/{total}"
    if band:
        return f"{band}层/未知"
    return "未知"


def derive_unit_price(total_wan: Optional[float], area: Optional[float]) -> Optional[int]:
    if not total_wan or not area:
        return None
    return int(round(total_wan * 10000 / area))


def make_hash(community: str, area: Optional[float], total_price: Optional[float], floor: str) -> str:
    raw = f"{community.strip()}|{area or 0:.1f}|{total_price or 0:.1f}|{floor.strip()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


def make_id(platform: str, hash_key: str) -> str:
    prefix = {"anjuke": "AJ", "beike": "BK", "lianjia": "LJ", "58": "TC"}.get(platform, "XX")
    return f"{prefix}-{hash_key.upper()}"


def now_str() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M")


def finalise(raw: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """Take a raw record from a collector and return the canonical dict.

    Required raw inputs (best-effort, missing ones tolerated):
        community, region, floor, totalPrice (or totalPriceText),
        area (or areaText), layout, orientation, sourceUrl
    """
    community = (raw.get("community") or "").strip()
    region = (raw.get("region") or "").strip()
    floor = parse_floor(raw.get("floor") or raw.get("vpc") or "")
    layout = parse_layout(raw.get("layout") or "")
    decoration = parse_decoration(raw.get("decoration") or raw.get("houseInfo") or "")
    orientation = parse_orientation(raw.get("orientation") or "")

    total_price = raw.get("totalPrice")
    if total_price is None:
        total_price = parse_total_price(raw.get("totalPriceText") or "")

    area = raw.get("area")
    if area is None:
        area = parse_area(raw.get("areaText") or "")

    unit_price = raw.get("unitPrice")
    if unit_price is None:
        unit_price = parse_unit_price(raw.get("unitPriceText") or "")
    if not unit_price:
        unit_price = derive_unit_price(total_price, area)

    hash_key = make_hash(community, area, total_price, floor)
    return {
        "id": make_id(platform, hash_key),
        "community": community,
        "region": region,
        "floor": floor,
        "totalPrice": float(total_price) if total_price is not None else 0.0,
        "unitPrice": int(unit_price) if unit_price is not None else 0,
        "area": float(area) if area is not None else 0.0,
        "layout": layout,
        "decoration": decoration,
        "orientation": orientation,
        "date": now_str(),
        "sourceUrl": (raw.get("sourceUrl") or "").strip(),
        "platform": platform,
        "hashKey": hash_key,
    }


def is_complete(rec: Dict[str, Any]) -> bool:
    """Minimum field set for a record to be worth showing in review."""
    return bool(
        rec.get("community")
        and rec.get("region")
        and rec.get("totalPrice")
        and rec.get("area")
    )
