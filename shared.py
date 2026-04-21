"""
Shared utilities, constants, and helpers used by both property_scraper and broker_scraper.
"""
from __future__ import annotations

import asyncio
import json
import math
import random
import re
from typing import AsyncIterator, Optional
from urllib.parse import urlencode, quote

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# Bayut Algolia config (extracted from bayut.sa page bundle)
# ─────────────────────────────────────────────────────────────────────────────

BAYUT_ALGOLIA_APP_ID  = "LL8IZ711CS"
BAYUT_ALGOLIA_API_KEY = "5b970b39b22a4ff1b99e5167696eef3f"
BAYUT_ALGOLIA_INDEX   = "bayut-sa-production-ads-city-level-score-ar"
BAYUT_ALGOLIA_URL     = f"https://{BAYUT_ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{BAYUT_ALGOLIA_INDEX}/query"

# ─────────────────────────────────────────────────────────────────────────────
# Headers
# ─────────────────────────────────────────────────────────────────────────────

def _h(ref="https://www.google.com"):
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": ref,
    }

def _jh(ref=""):
    return {
        "Accept": "application/json, text/plain, */*",
        "Referer": ref,
    }

# ─────────────────────────────────────────────────────────────────────────────
# City coordinates & district data
# ─────────────────────────────────────────────────────────────────────────────

CITY_COORDS: dict[str, tuple[float, float]] = {
    "riyadh": (24.7136, 46.6753),
    "jeddah": (21.4858, 39.1925),
    "mecca": (21.3891, 39.8579),
    "medina": (24.5247, 39.5692),
    "dammam": (26.4207, 50.0888),
    "al khobar": (26.2172, 50.1971),
    "khobar": (26.2172, 50.1971),
    "dhahran": (26.2621, 50.0393),
    "abha": (18.2164, 42.5053),
    "tabuk": (28.3998, 36.5716),
    "buraidah": (26.3292, 43.9744),
    "khamis mushait": (18.3056, 42.7292),
    "al jubail": (27.0114, 49.6583),
    "jubail": (27.0114, 49.6583),
    "hail": (27.5114, 41.7208),
    "al taif": (21.2827, 40.4138),
    "taif": (21.2827, 40.4138),
    "yanbu": (24.0892, 38.0618),
    "al ahsa": (25.3754, 49.5882),
    "ahsa": (25.3754, 49.5882),
    "al qatif": (26.5093, 50.0036),
    "najran": (17.4924, 44.1277),
    "jazan": (16.8892, 42.5511),
    "al ula": (26.6159, 37.9212),
}

DISTRICTS: dict[str, list[str]] = {
    "riyadh": ["Al Malaz", "Al Olaya", "Al Nakheel", "Al Wurud", "Al Rawdah",
               "Al Sulaimaniyah", "Al Qirawan", "Al Yarmouk", "Al Shifa", "Al Aqiq",
               "Al Hamra", "Al Murabba", "Al Izdihar", "Al Naseem", "Al Aziziyah",
               "Al Malqa", "Al Rabwah", "Hittin", "Al Yasmeen", "Al Sahafah"],
    "jeddah": ["Al Rawdah", "Al Hamra", "Al Andalus", "Al Nuzha", "Al Zahra",
               "Al Salamah", "Al Khalidiyah", "Al Rehab", "Al Marwah", "Al Naeem",
               "Al Shati", "Al Corniche", "Al Balad", "Al Faisaliyah"],
    "dammam": ["Al Faisaliyah", "Al Shula", "Al Noor", "Al Badiyah", "Al Hamra",
               "Al Anoud", "Al Mazruiyah", "Al Jalawiyah", "Al Muhammadiyah"],
    "mecca": ["Al Aziziyah", "Al Rusaifah", "Ajyad", "Al Massa", "Al Zaher", "Al Adl"],
    "medina": ["Al Aziziyah", "Al Ranuna", "Quba", "Sakan", "Al Haram", "Al Aqoul"],
    "khobar": ["Al Corniche", "Al Thuqbah", "Al Aqrabiyah", "Al Khobar Al Shamaliyah"],
    "abha": ["Al Manhal", "Al Mahalah", "Al Namas", "Al Sad"],
    "tabuk": ["Al Rawdah", "Al Nuzha", "Al Safa", "Al Marwah"],
}

PROPERTY_IMAGES: dict[str, list[str]] = {
    "apartment": [
        "https://images.unsplash.com/photo-1560448204-e02f11c3d0e2?w=600&q=80",
        "https://images.unsplash.com/photo-1502672260266-1c1ef2d93688?w=600&q=80",
        "https://images.unsplash.com/photo-1522708323590-d24dbb6b0267?w=600&q=80",
        "https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?w=600&q=80",
        "https://images.unsplash.com/photo-1484154218962-a197022b5858?w=600&q=80",
        "https://images.unsplash.com/photo-1493809842364-78817add7ffb?w=600&q=80",
        "https://images.unsplash.com/photo-1554995207-c18c203602cb?w=600&q=80",
        "https://images.unsplash.com/photo-1536376072261-38c75010e6c9?w=600&q=80",
    ],
    "villa": [
        "https://images.unsplash.com/photo-1613977257363-707ba9348227?w=600&q=80",
        "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=600&q=80",
        "https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=600&q=80",
        "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w=600&q=80",
        "https://images.unsplash.com/photo-1568605114967-8130f3a36994?w=600&q=80",
        "https://images.unsplash.com/photo-1583608205776-bfd35f0d9f83?w=600&q=80",
        "https://images.unsplash.com/photo-1449844908441-8829872d2607?w=600&q=80",
    ],
    "house": [
        "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w=600&q=80",
        "https://images.unsplash.com/photo-1575517111839-3a3843ee7f5d?w=600&q=80",
        "https://images.unsplash.com/photo-1523217582562-09d0def993a6?w=600&q=80",
        "https://images.unsplash.com/photo-1598228723793-52759bba239c?w=600&q=80",
    ],
    "land": [
        "https://images.unsplash.com/photo-1500382017468-9049fed747ef?w=600&q=80",
        "https://images.unsplash.com/photo-1602941525421-8f8b81d3edbb?w=600&q=80",
        "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=600&q=80",
    ],
    "office": [
        "https://images.unsplash.com/photo-1497366216548-37526070297c?w=600&q=80",
        "https://images.unsplash.com/photo-1497366811353-6870744d04b2?w=600&q=80",
        "https://images.unsplash.com/photo-1604328698692-f76ea9498e76?w=600&q=80",
    ],
    "commercial": [
        "https://images.unsplash.com/photo-1486325212027-8081e485255e?w=600&q=80",
        "https://images.unsplash.com/photo-1441986300917-64674bd600d8?w=600&q=80",
        "https://images.unsplash.com/photo-1580587771525-78b9dba3b914?w=600&q=80",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Location helpers
# ─────────────────────────────────────────────────────────────────────────────

def _city_from_location(location: str) -> str:
    if "," in location:
        return location.split(",")[-1].strip()
    return location


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _get_coords(location: str, offset: bool = True) -> tuple[float, float]:
    city_part = _city_from_location(location).strip().lower()
    key       = location.strip().lower()
    base = CITY_COORDS.get(city_part) or CITY_COORDS.get(key)
    if not base:
        for k, v in CITY_COORDS.items():
            if k in key or key in k:
                base = v
                break
    if not base:
        base = (24.7136, 46.6753)
    if offset:
        lat = base[0] + random.uniform(-0.08, 0.08)
        lng = base[1] + random.uniform(-0.08, 0.08)
        return round(lat, 6), round(lng, 6)
    return base

# ─────────────────────────────────────────────────────────────────────────────
# Value helpers
# ─────────────────────────────────────────────────────────────────────────────

def _int(v) -> int:
    if not v: return 0
    try: return int(float(re.sub(r"[^\d.]", "", str(v))))
    except: return 0

def _str(v, fb="N/A") -> str:
    s = str(v).strip() if v is not None else ""
    return s or fb

def _clean_phone(raw) -> str:
    if raw is None:
        return ""
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith("00966"):
        digits = digits[5:]
    elif digits.startswith("966") and len(digits) > 9:
        digits = digits[3:]
    if digits.startswith("0") and len(digits) > 1:
        digits = digits[1:]
    return digits if 8 <= len(digits) <= 10 else ""

# ─────────────────────────────────────────────────────────────────────────────
# SSE helper
# ─────────────────────────────────────────────────────────────────────────────

def _sse(p: dict) -> str:
    return f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
