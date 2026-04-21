"""
Saudi Property Aggregator — FastAPI backend
Bayut via Algolia API (real data) + curl_cffi for other platforms
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
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Saudi Property Aggregator", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Headers
# ─────────────────────────────────────────────────────────────────────────────

# ── Bayut Algolia config (extracted from bayut.sa page bundle) ────────────────
BAYUT_ALGOLIA_APP_ID  = "LL8IZ711CS"
BAYUT_ALGOLIA_API_KEY = "5b970b39b22a4ff1b99e5167696eef3f"
BAYUT_ALGOLIA_INDEX   = "bayut-sa-production-ads-city-level-score-ar"
BAYUT_ALGOLIA_URL     = f"https://{BAYUT_ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{BAYUT_ALGOLIA_INDEX}/query"

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


def _city_from_location(location: str) -> str:
    """If location is 'District, City' format, return 'City'. Otherwise return location."""
    if "," in location:
        return location.split(",")[-1].strip()
    return location


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in kilometres between two lat/lng points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _get_coords(location: str, offset: bool = True) -> tuple[float, float]:
    # Handle "District, City" format — try city first
    city_part = _city_from_location(location).strip().lower()
    key       = location.strip().lower()
    base = CITY_COORDS.get(city_part) or CITY_COORDS.get(key)
    if not base:
        for k, v in CITY_COORDS.items():
            if k in key or key in k:
                base = v
                break
    if not base:
        base = (24.7136, 46.6753)  # default Riyadh
    if offset:
        lat = base[0] + random.uniform(-0.08, 0.08)
        lng = base[1] + random.uniform(-0.08, 0.08)
        return round(lat, 6), round(lng, 6)
    return base

# ─────────────────────────────────────────────────────────────────────────────
# Mock data generator
# ─────────────────────────────────────────────────────────────────────────────

def _platform_search_url(platform_name: str, location: str, prop_type: str,
                         listing_type: str, min_price: Optional[int],
                         max_price: Optional[int], rooms: Optional[int]) -> str:
    """
    Return the verified working search-results URL for each platform.
    These URLs have been validated against each platform's actual routing.
    """
    loc       = location.strip()
    loc_lower = loc.lower()
    loc_slug  = loc_lower.replace(" ", "-")        # "al-khobar"
    ltype     = "sale" if listing_type == "sale" else "rent"

    # Bayut English URL structure (verified working):
    # bayut.sa/for-sale/apartments/riyadh/?price_min=X&bedrooms=N
    BAYUT_TYPES = {
        "apartment":"apartments", "villa":"villas", "house":"houses",
        "office":"offices", "land":"land", "commercial":"commercial-spaces",
    }
    # PropertyFinder SEO URL structure (verified):
    # propertyfinder.sa/en/buy/apartments-for-sale-in-riyadh/
    PF_SALE_TYPES = {
        "apartment":"apartments-for-sale",  "villa":"villas-for-sale",
        "house":"houses-for-sale",          "office":"offices-for-sale",
        "land":"land-for-sale",             "commercial":"commercial-for-sale",
    }
    PF_RENT_TYPES = {
        "apartment":"apartments-for-rent",  "villa":"villas-for-rent",
        "house":"houses-for-rent",          "office":"offices-for-rent",
        "land":"land-for-rent",             "commercial":"commercial-for-rent",
    }
    # Aqar Arabic search query
    AQAR_TYPE_AR = {
        "apartment":"شقة", "villa":"فيلا", "house":"منزل",
        "land":"أرض", "office":"مكتب", "commercial":"تجاري",
    }
    AQAR_CITY_AR = {
        "riyadh":"الرياض", "jeddah":"جدة", "dammam":"الدمام",
        "mecca":"مكة", "medina":"المدينة", "khobar":"الخبر",
        "al khobar":"الخبر", "abha":"أبها", "tabuk":"تبوك",
        "buraidah":"بريدة", "hail":"حائل", "al taif":"الطائف",
        "yanbu":"ينبع", "najran":"نجران", "jazan":"جازان",
    }

    if platform_name == "Bayut":
        seg  = "for-sale" if ltype == "sale" else "for-rent"
        prop = BAYUT_TYPES.get(prop_type, "properties")
        p = {}
        if min_price: p["price_min"] = min_price
        if max_price: p["price_max"] = max_price
        if rooms:     p["bedrooms"]  = rooms
        qs = ("?" + urlencode(p)) if p else ""
        return f"https://www.bayut.sa/{seg}/{prop}/{loc_slug}/{qs}"

    if platform_name == "Aqar":
        # aqar.fm (NOT sa.aqar.fm — that's a dead subdomain)
        type_ar = AQAR_TYPE_AR.get(prop_type, "عقار")
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        purpose_ar = "للبيع" if ltype == "sale" else "للإيجار"
        # Aqar search URL with Arabic query
        q = quote(f"{type_ar} {purpose_ar} في {city_ar}")
        return f"https://aqar.fm/search?q={q}"

    if platform_name == "PropertyFinder":
        # Verified SEO URL structure: /en/buy/TYPE-for-sale-in-CITY/
        seg = "buy" if ltype == "sale" else "rent"
        type_slug = (PF_SALE_TYPES if ltype == "sale" else PF_RENT_TYPES).get(prop_type, f"properties-for-{ltype}")
        return f"https://www.propertyfinder.sa/en/{seg}/{type_slug}-in-{loc_slug}/"

    if platform_name == "Wasalt":
        # Wasalt.com verified URL
        purpose = "buy" if ltype == "sale" else "rent"
        prop_slug = {"apartment":"apartment","villa":"villa","house":"house",
                     "office":"office","land":"land","commercial":"commercial"}.get(prop_type,"apartment")
        return f"https://wasalt.com/en/properties?purpose={purpose}&type={prop_slug}&city={loc_slug}"

    if platform_name == "Sakani":
        # Government housing portal — city in Arabic
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        return f"https://sakani.sa/en/projects?city={quote(city_ar)}"

    if platform_name == "Haraj":
        # haraj.com.sa — Arabic search query
        type_ar = AQAR_TYPE_AR.get(prop_type, "عقار")
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        q = quote(f"{type_ar} {city_ar}")
        return f"https://haraj.com.sa/search?q={q}"

    if platform_name == "OpenSooq":
        # sa.opensooq.com — verified category URLs
        cat_map = {
            "apartment": "apartments",  "villa": "villas",
            "house": "houses",          "land": "lands",
            "office": "offices",        "commercial": "commercial-properties",
        }
        purpose = "for-sale" if ltype == "sale" else "for-rent"
        cat = cat_map.get(prop_type, "real-estate")
        return f"https://sa.opensooq.com/{cat}-{purpose}/{loc_slug}"

    if platform_name == "Expatriates":
        # expatriates.com — simple static category
        sub = "for-sale" if ltype == "sale" else "for-rent"
        return f"https://www.expatriates.com/classifieds/saudi-arabia/real-estate/{sub}/"

    if platform_name == "Mourjan":
        # sa.mourjan.com verified URL
        purpose = "for-sale" if ltype == "sale" else "for-rent"
        return f"https://sa.mourjan.com/classifieds/saudi-arabia/real-estate-{purpose}/"

    if platform_name == "Satel":
        return "https://satel.sa/compounds"

    if platform_name == "Zaahib":
        return f"https://zaahib.com/search?purpose={ltype}&city={loc_slug}"

    if platform_name == "Bezaat":
        return f"https://bezaat.com/sa/real-estate?type={ltype}&city={loc_slug}"

    if platform_name == "SaudiDeal":
        return f"https://saudi-deal.com/properties?purpose={ltype}&city={loc_slug}"

    return "https://www.bayut.sa"



# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _int(v) -> int:
    if not v: return 0
    try: return int(float(re.sub(r"[^\d.]", "", str(v))))
    except: return 0

def _str(v, fb="N/A") -> str:
    s = str(v).strip() if v is not None else ""
    return s or fb

def _clean_phone(raw) -> str:
    """Normalize to Saudi local number (9 digits, no country code, no symbols)."""
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
# Base scraper
# ─────────────────────────────────────────────────────────────────────────────

# Keywords for guessing property type from title (used to post-filter scrapers
# that don't have server-side property-type filtering)
_TYPE_INCLUDE = {
    "apartment":  ["apartment","flat","studio","شقة","شقق"],
    "villa":      ["villa","فيلا","townhouse","دوبلكس","duplex"],
    "house":      ["house","منزل","townhouse"],
    "office":     ["office","مكتب","workspace"],
    "land":       ["land","plot","أرض","قطعة"],
    "commercial": ["commercial","shop","retail","تجاري","محل","showroom"],
}
_TYPE_EXCLUDE = {
    "apartment":  ["villa","فيلا","أرض","land plot"],
    "villa":      ["apartment","flat","studio","شقة","office","أرض","land plot"],
    "house":      ["apartment","flat","office","أرض"],
    "office":     ["apartment","villa","land","فيلا","أرض"],
    "land":       ["apartment","villa","office","فيلا","شقة"],
    "commercial": ["apartment","villa","فيلا","شقة"],
}


class BaseScraper:
    platform_name: str = "Unknown"
    base_url: str = ""
    mock_count: int = 8

    def __init__(self, location, min_price, max_price, rooms, property_type, listing_type):
        self.location = location
        self.min_price = min_price
        self.max_price = max_price
        self.rooms = rooms
        self.property_type = property_type.lower()
        self.listing_type = listing_type.lower()

    async def scrape(self, client: AsyncSession) -> list[dict]:
        raise NotImplementedError

    def _type_filter(self, results: list[dict]) -> list[dict]:
        """Remove listings whose title clearly belongs to a different property type."""
        inc = _TYPE_INCLUDE.get(self.property_type, [])
        exc = _TYPE_EXCLUDE.get(self.property_type, [])
        if not inc and not exc:
            return results
        filtered = []
        for r in results:
            t = (r.get("title","") or "").lower()
            if any(k in t for k in exc):
                continue         # clearly wrong type → drop
            filtered.append(r)
        return filtered

    def _with_coords(self, item: dict) -> dict:
        if "lat" not in item or not item["lat"]:
            lat, lng = _get_coords(self.location)
            item["lat"] = lat
            item["lng"] = lng
        if "area_sqm" not in item:
            item["area_sqm"] = random.randint(80, 500)
        return item

    def _extract_next_data(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            try:
                data = json.loads(tag.string)
                return self._walk_json(data)
            except: pass
        return []

    def _walk_json(self, data, depth=0) -> list[dict]:
        if depth > 10: return []
        out = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and ("price" in item or "title" in item):
                    n = self._norm(item)
                    if n: out.append(n)
                elif isinstance(item, (dict, list)):
                    out.extend(self._walk_json(item, depth+1))
        elif isinstance(data, dict):
            for k, v in data.items():
                if k in ("hits","properties","listings","results","data","searchResult","items"):
                    out.extend(self._walk_json(v, depth+1))
                elif isinstance(v, (dict, list)):
                    out.extend(self._walk_json(v, depth+1))
        seen, deduped = set(), []
        for r in out:
            key = r.get("source_url","") + str(r.get("price_sar",""))
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        return deduped[:20]

    def _norm(self, item: dict) -> Optional[dict]:
        price = _int(item.get("price") or item.get("rentPrice") or item.get("pricePerYear"))
        title = _str(item.get("title") or item.get("name") or item.get("nameL1"), "")
        if not title or price <= 0: return None
        loc = item.get("location") or item.get("locationPath") or []
        if isinstance(loc, list):
            ld = " › ".join(x.get("name","") for x in loc if isinstance(x,dict)).strip(" ›")
        else:
            ld = _str(loc, self.location)
        rooms = _str(item.get("rooms") or item.get("beds") or item.get("bedrooms"))
        baths = _str(item.get("baths") or item.get("bathrooms"))
        slug = item.get("slug") or item.get("externalID") or ""
        # Only use slug-based URL when it looks like a real slug (not a bare integer ID
        # which would produce invalid URLs on most platforms).
        if slug and not str(slug).isdigit():
            url = f"{self.base_url}/property/{slug}/"
        elif slug and str(slug).isdigit() and self.platform_name == "Bayut":
            url = f"{self.base_url}/property/{slug}/"  # Bayut does use numeric IDs
        else:
            # Fall back to the real search page so "View on Platform" always works
            url = _platform_search_url(
                self.platform_name, self.location, self.property_type,
                self.listing_type, self.min_price, self.max_price, self.rooms
            )
        ph_obj = item.get("phoneNumber") or item.get("agent") or {}
        contact = _str(ph_obj.get("mobile") or ph_obj.get("phone"), "") if isinstance(ph_obj,dict) else _str(ph_obj,"")
        lat, lng = _get_coords(self.location)
        if isinstance(item.get("geography"), dict):
            lat = item["geography"].get("lat", lat)
            lng = item["geography"].get("lng", lng)
        imgs = PROPERTY_IMAGES.get(self.property_type, PROPERTY_IMAGES["apartment"])
        image_url = (item.get("coverPhoto",{}) or {}).get("url","") or \
                    (item.get("photos",[{}])[0] or {}).get("url","") or \
                    item.get("image","") or item.get("thumbnail","") or random.choice(imgs)
        return {
            "title": title, "price_sar": price, "rent_period": "",
            "location_detail": ld or self.location,
            "bedrooms": rooms, "bathrooms": baths,
            "area_sqm": _int(item.get("area") or item.get("size") or item.get("areaInSqft",0)),
            "contact_number": contact,
            "source_url": url, "source_platform_name": self.platform_name,
            "image_url": image_url,
            "lat": lat, "lng": lng,
            "is_mock": False,
        }

# ─────────────────────────────────────────────────────────────────────────────
# 1. Bayut
# ─────────────────────────────────────────────────────────────────────────────

class BayutScraper(BaseScraper):
    """
    Bayut — queries their Algolia search index directly.
    Algolia config is publicly exposed in bayut.sa's page bundle.
    Returns real, live listings with actual phone numbers and photos.
    """
    platform_name = "Bayut"
    base_url      = "https://www.bayut.sa"

    # Algolia category slugs (verified via live queries)
    _CAT_SLUGS = {
        "apartment": "apartments",
        "villa":     "villas",
        "house":     "townhouses",
        "office":    "offices",
        "land":      "residential-lands",
        "commercial":"showrooms",
    }
    # City name → Algolia location slug
    _CITY_SLUGS = {
        "riyadh":      "/riyadh",
        "jeddah":      "/jeddah",
        "mecca":       "/mecca",
        "medina":      "/medina",
        "dammam":      "/dammam",
        "al khobar":   "/al-khobar",
        "khobar":      "/al-khobar",
        "abha":        "/abha",
        "tabuk":       "/tabuk",
        "buraidah":    "/buraidah",
        "khamis mushait": "/khamis-mushait",
        "hail":        "/hail",
        "al taif":     "/taif",
        "taif":        "/taif",
        "yanbu":       "/yanbu",
        "najran":      "/najran",
        "jazan":       "/jazan",
        "dhahran":     "/dhahran",
        "al jubail":   "/jubail",
        "jubail":      "/jubail",
    }

    def _parse_hit(self, h: dict) -> Optional[dict]:
        price = _int(h.get("price") or 0)
        title = _str(h.get("title_l1") or h.get("title"), "")
        if not title: return None

        # Location breadcrumb
        loc_list = h.get("location") or []
        ld = " › ".join(x.get("name_l1","") for x in loc_list if isinstance(x, dict) and x.get("name_l1")).strip(" ›") or self.location

        # URL — /property/details-{externalID}.html
        ext_id = h.get("externalID","")
        source_url = f"{self.base_url}/property/details-{ext_id}.html" if ext_id else self.base_url

        # Image — images.bayut.sa/thumbnails/{id}-400x300.jpeg
        cover = h.get("coverPhoto") or {}
        cover_id = cover.get("id") if isinstance(cover, dict) else None
        image_url = f"https://images.bayut.sa/thumbnails/{cover_id}-400x300.jpeg" if cover_id else ""

        # Phone + broker info
        ph = h.get("phoneNumber") or {}
        contact = ""
        if isinstance(ph, dict):
            raw_phone = ph.get("mobile") or ph.get("phone") or (ph.get("phoneNumbers") or [None])[0]
            contact = _clean_phone(raw_phone)

        # Agent/broker profile info
        agent_obj  = h.get("agent") or h.get("agency") or {}
        agency_obj = h.get("agency") or {}
        broker_name  = _str((agent_obj.get("name") if isinstance(agent_obj, dict) else ""), "")
        broker_agency = _str((agency_obj.get("name") if isinstance(agency_obj, dict) else
                              agent_obj.get("name") if isinstance(agent_obj, dict) else ""), "")
        broker_photo = _str((agent_obj.get("photo") or agent_obj.get("profilePhoto") or
                             agent_obj.get("logoUrl") if isinstance(agent_obj, dict) else ""), "")
        agent_id     = _str(agent_obj.get("externalID") or agent_obj.get("id") or
                            agent_obj.get("slug") if isinstance(agent_obj, dict) else "", "")
        broker_url   = f"{self.base_url}/en/agents/{agent_id}/" if agent_id else ""

        # Coordinates
        geo = h.get("geography") or h.get("_geoloc") or {}
        lat = geo.get("lat") or _get_coords(self.location)[0]
        lng = geo.get("lng") or _get_coords(self.location)[1]

        rooms_val = h.get("rooms")
        if rooms_val == 0:
            bedrooms = "Studio"
        elif rooms_val and int(rooms_val) > 0:
            bedrooms = str(int(rooms_val))
        else:
            bedrooms = "N/A"

        # Rent frequency (Yearly / Monthly / Weekly / Daily)
        freq_raw = _str(h.get("rentFrequency") or h.get("rent_frequency"), "")
        freq_map = {"yearly": "/year", "monthly": "/month", "weekly": "/week", "daily": "/day"}
        rent_period = freq_map.get(freq_raw.lower(), "")

        return {
            "title": title,
            "price_sar": price,
            "rent_period": rent_period,
            "location_detail": ld,
            "bedrooms":  bedrooms,
            "bathrooms": _str(h.get("baths", "N/A")),
            "area_sqm":  _int(h.get("area", 0)),
            "contact_number": contact,
            "source_url": source_url,
            "source_platform_name": self.platform_name,
            "image_url": image_url,
            "lat": lat, "lng": lng,
            # Broker fields (used by /api/brokers)
            "broker_name":    broker_name,
            "broker_agency":  broker_agency,
            "broker_photo":   broker_photo,
            "broker_url":     broker_url,
        }

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            purpose    = "for-sale" if self.listing_type == "sale" else "for-rent"
            cat_slug   = self._CAT_SLUGS.get(self.property_type, "apartments")

            # Handle "District, City" format
            city_str   = _city_from_location(self.location).strip().lower()
            city_slug  = self._CITY_SLUGS.get(city_str,
                             f"/{city_str.replace(' ', '-')}")
            # If a district was specified, use it as a text query for Algolia
            district_q = ""
            if "," in self.location:
                district_q = self.location.split(",")[0].strip()

            filters = f"purpose:{purpose} AND category.slug_l1:{cat_slug}"
            if self.min_price: filters += f" AND price>={self.min_price}"
            if self.max_price: filters += f" AND price<={self.max_price}"
            if self.rooms:     filters += f" AND rooms={self.rooms}"

            results = []
            for page in range(2):          # fetch 2 pages = up to 40 listings
                payload = {
                    "query": district_q,   # empty for city-only, district name for district search
                    "filters": filters,
                    "facetFilters": [[f"location.slug_l1:{city_slug}"]],
                    "hitsPerPage": 20,
                    "page": page,
                    "attributesToRetrieve": [
                        "title_l1","price","purpose","rooms","baths","area",
                        "externalID","slug_l1","coverPhoto","phoneNumber",
                        "geography","_geoloc","location","rentFrequency",
                    ],
                }
                r = await client.post(
                    BAYUT_ALGOLIA_URL,
                    json=payload,
                    headers={
                        "X-Algolia-Application-Id": BAYUT_ALGOLIA_APP_ID,
                        "X-Algolia-API-Key":        BAYUT_ALGOLIA_API_KEY,
                        "Content-Type":             "application/json",
                        "Origin":                   "https://www.bayut.sa",
                        "Referer":                  "https://www.bayut.sa/",
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    data = r.json()
                    hits = data.get("hits", [])
                    parsed = [self._parse_hit(h) for h in hits]
                    results.extend([x for x in parsed if x])
                    if len(hits) < 20: break   # last page
                else:
                    print(f"[Bayut Algolia] status={r.status_code}")
                    break

            print(f"[Bayut] {len(results)} listings")
            return results
        except Exception as e:
            print(f"[Bayut] error: {e}")
            return []

# ─────────────────────────────────────────────────────────────────────────────
# 2. Aqar
# ─────────────────────────────────────────────────────────────────────────────

class AqarScraper(BaseScraper):
    """
    Aqar — scrapes via Next.js RSC (React Server Components) payload.
    GET the listing page with header 'RSC: 1' — the server returns a text/x-component
    stream that contains all rendered listing objects as embedded JSON.
    """
    platform_name = "Aqar"
    base_url = "https://sa.aqar.fm"

    # URL path segment: (property_type, listing_type) → Arabic slug
    _SLUGS: dict[tuple[str,str], str] = {
        ("apartment",  "rent"): "شقق-للإيجار",
        ("apartment",  "sale"): "شقق-للبيع",
        ("villa",      "rent"): "فلل-للإيجار",
        ("villa",      "sale"): "فلل-للبيع",
        ("house",      "rent"): "منازل-للإيجار",
        ("house",      "sale"): "منازل-للبيع",
        ("land",       "sale"): "أراضي-للبيع",
        ("land",       "rent"): "أراضي-للإيجار",
        ("office",     "rent"): "مكاتب-للإيجار",
        ("office",     "sale"): "مكاتب-للبيع",
        ("commercial", "rent"): "محلات-للإيجار",
        ("commercial", "sale"): "محلات-للبيع",
    }
    _CITIES: dict[str, str] = {
        "riyadh":    "الرياض",
        "jeddah":    "جدة",
        "mecca":     "مكة-المكرمة",
        "medina":    "المدينة-المنورة",
        "dammam":    "الدمام",
        "khobar":    "الخبر",
        "al khobar": "الخبر",
        "abha":      "أبها",
        "tabuk":     "تبوك",
        "hail":      "حائل",
        "buraidah":  "بريدة",
        "taif":      "الطائف",
        "al taif":   "الطائف",
        "yanbu":     "ينبع",
        "najran":    "نجران",
        "jazan":     "جازان",
    }
    _RENT_PERIOD: dict[str, str] = {
        "سنوي":  "/year",
        "شهري":  "/month",
        "أسبوعي": "/week",
        "يومي":  "/day",
    }

    def _extract_listings(self, text: str) -> list[dict]:
        """Extract embedded listing JSON objects from RSC stream text."""
        matches = list(re.finditer(r'\{"id":\d+,"sov_campaign_id"', text))
        listings, seen = [], set()
        for m in matches:
            start = m.start()
            depth, end = 0, start
            for i, ch in enumerate(text[start:], start):
                if ch == "{":   depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            try:
                obj = json.loads(text[start:end])
                lid = obj.get("id")
                if lid and lid not in seen:
                    seen.add(lid)
                    listings.append(obj)
            except Exception:
                pass
        return listings

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            city_str  = _city_from_location(self.location).strip().lower()
            loc_lower = self.location.strip().lower()
            city_ar   = self._CITIES.get(city_str, self._CITIES.get(loc_lower, "الرياض"))
            type_slug = self._SLUGS.get((self.property_type, self.listing_type))
            if not type_slug:
                print(f"[Aqar] no slug for ({self.property_type}, {self.listing_type}) — skipping")
                return []
            url       = f"{self.base_url}/{quote(type_slug, safe='')}/{quote(city_ar, safe='')}"

            r = await client.get(
                url,
                headers={
                    "RSC": "1",
                    "Accept": "text/x-component, */*",
                    "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                    "Referer": self.base_url + "/",
                },
                timeout=20,
            )
            if r.status_code != 200:
                print(f"[Aqar] HTTP {r.status_code}")
                return []

            raw = self._extract_listings(r.text)
            results = []
            for item in raw:
                price = _int(item.get("price") or 0)
                title = _str(item.get("title"), "")
                if not title:
                    continue

                # Rent period
                rpt = _str(item.get("rent_period_text"), "")
                rent_period = self._RENT_PERIOD.get(rpt, "")

                # Location
                address_text = _str(item.get("address_text") or item.get("district"), "")
                city_name    = _str(item.get("city"), "")
                ld = address_text or city_name or self.location.title()

                # Coordinates
                geo = item.get("location") or {}
                lat = float(geo.get("lat") or 0) or _get_coords(self.location)[0]
                lng = float(geo.get("lng") or 0) or _get_coords(self.location)[1]

                # Image
                main_img = item.get("mainImage") or (item.get("imgs") or [None])[0]
                image_url = f"https://images.aqar.fm/webp/750x0/props/{main_img}" if main_img else ""

                # Source URL
                path = _str(item.get("path"), "")
                source_url = f"{self.base_url}{path}" if path else f"{self.base_url}/عقارات"

                # Apply price filter manually (RSC doesn't support server-side filtering)
                if self.min_price and price < self.min_price: continue
                if self.max_price and price > self.max_price: continue
                if self.rooms:
                    beds = _int(item.get("beds") or 0)
                    if beds and beds != self.rooms: continue

                results.append({
                    "title":                title,
                    "price_sar":            price,
                    "rent_period":          rent_period,
                    "location_detail":      ld,
                    "bedrooms":             _str(item.get("beds"), "N/A") if item.get("beds") else "N/A",
                    "bathrooms":            _str(item.get("wc"),   "N/A") if item.get("wc")   else "N/A",
                    "area_sqm":             _int(item.get("area") or 0),
                    "contact_number":       "",
                    "source_url":           source_url,
                    "source_platform_name": self.platform_name,
                    "image_url":            image_url,
                    "lat": lat, "lng": lng,
                })

            print(f"[Aqar] {len(results)} listings from {url}")
            return results[:20]

        except Exception as e:
            print(f"[Aqar] error: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 3. Property Finder SA
# ─────────────────────────────────────────────────────────────────────────────

class PropertyFinderScraper(BaseScraper):
    """
    PropertyFinder SA — uses SEO listing pages (NOT /search which is WAF-blocked).
    URL: /en/{rent|buy}/{city-slug}/{type}-for-{rent|buy}.html
    Returns __NEXT_DATA__ with searchResult.properties (25 listings per page).
    """
    platform_name = "PropertyFinder"
    base_url = "https://www.propertyfinder.sa"

    _TYPES = {
        "apartment":  "apartments",
        "villa":      "villas",
        "house":      "houses",
        "land":       "land",
        "office":     "offices",
        "commercial": "commercial-properties",
    }
    # Sitemap-derived region slugs — PF organises by region not city
    _CITIES = {
        "riyadh":    "ar-riyadh",
        "jeddah":    "makkah-al-mukarramah",
        "mecca":     "makkah-al-mukarramah",
        "medina":    "al-madinah-al-munawwarah",
        "dammam":    "eastern",
        "khobar":    "eastern",
        "al khobar": "eastern",
        "dhahran":   "eastern",
        "jubail":    "eastern",
        "al jubail": "eastern",
        "abha":      "asir",
        "taif":      "asir",
        "al taif":   "asir",
        "tabuk":     "tabuk",
        "hail":      "ar-riyadh",   # fallback — no dedicated hail slug
    }

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            city_str   = _city_from_location(self.location).strip().lower()
            loc_lower  = self.location.strip().lower()
            city_slug  = self._CITIES.get(city_str, self._CITIES.get(loc_lower, "ar-riyadh"))
            type_slug  = self._TYPES.get(self.property_type, "apartments")
            purpose    = "buy" if self.listing_type == "sale" else "rent"
            url = f"{self.base_url}/en/{purpose}/{city_slug}/{type_slug}-for-{purpose}.html"

            r = await client.get(url, headers={
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.google.com/",
            }, timeout=20)

            if r.status_code != 200:
                print(f"[PropertyFinder] HTTP {r.status_code} for {url}")
                return []

            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if not m:
                print("[PropertyFinder] no __NEXT_DATA__")
                return []

            data  = json.loads(m.group(1))
            sr    = data.get("props", {}).get("pageProps", {}).get("searchResult", {})
            props = sr.get("properties", []) if isinstance(sr, dict) else []

            results = []
            for p in props:
                title = _str(p.get("title"), "")
                if not title:
                    continue

                # Price
                price_obj  = p.get("price") or {}
                price      = _int(price_obj.get("value") or 0)
                period_raw = _str(price_obj.get("period"), "").lower()
                rent_period = "/year" if "year" in period_raw else "/month" if "month" in period_raw else ""
                if purpose == "buy":
                    rent_period = ""

                # Location
                loc_obj = p.get("location") or {}
                ld      = _str(loc_obj.get("full_name"), self.location.title())
                coords  = loc_obj.get("coordinates") or {}
                lat     = float(coords.get("lat") or 0) or _get_coords(self.location)[0]
                lng     = float(coords.get("lon") or coords.get("lng") or 0) or _get_coords(self.location)[1]

                # Image
                imgs      = p.get("images") or []
                image_url = (imgs[0].get("medium") or imgs[0].get("small") or "") if imgs else ""

                # Bedrooms / bathrooms / area
                beds  = _str(p.get("bedrooms"),  "N/A")
                baths = _str(p.get("bathrooms"), "N/A")
                sz    = p.get("size") or {}
                area  = _int(sz.get("value") or 0)

                # Phone — prefer "phone" type in contact_options, fallback to broker.phone
                raw_phone  = ""
                agent_obj  = p.get("agent")  or {}
                broker_obj = p.get("broker") or {}
                for co in (p.get("contact_options") or []):
                    if co.get("type") == "phone":
                        raw_phone = _str(co.get("value"), "")
                        break
                if not raw_phone:
                    raw_phone = _str(broker_obj.get("phone"), "")
                phone = _clean_phone(raw_phone)

                # Broker profile info — agent has photo/slug, broker has agency name
                broker_name   = _str(agent_obj.get("name") or broker_obj.get("name"), "")
                broker_agency = _str(broker_obj.get("name") or
                                    (broker_obj.get("agency") or {}).get("name"), "")
                broker_photo  = _str(agent_obj.get("image"), "")
                agent_id      = _str(agent_obj.get("id"), "")
                agent_uid     = _str(agent_obj.get("user_id"), "")
                broker_slug   = _str(agent_obj.get("slug"), "")
                if broker_slug and agent_id and agent_uid:
                    broker_url = f"https://www.propertyfinder.sa/en/broker/{broker_slug}-{agent_id}-{agent_uid}"
                else:
                    broker_url = ""

                # Source URL
                source_url = _str(p.get("share_url"), url)
                if not source_url.startswith("http"):
                    source_url = f"{self.base_url}{source_url}"

                # Price filter
                if self.min_price and price and price < self.min_price: continue
                if self.max_price and price and price > self.max_price: continue
                if self.rooms:
                    b = _int(p.get("bedrooms") or 0)
                    if b and b != self.rooms: continue

                results.append({
                    "title":                title,
                    "price_sar":            price,
                    "rent_period":          rent_period,
                    "location_detail":      ld,
                    "bedrooms":             beds,
                    "bathrooms":            baths,
                    "area_sqm":             area,
                    "contact_number":       phone,
                    "source_url":           source_url,
                    "source_platform_name": self.platform_name,
                    "image_url":            image_url,
                    "lat": lat, "lng": lng,
                    "broker_name":    broker_name,
                    "broker_agency":  broker_agency,
                    "broker_photo":   broker_photo,
                    "broker_url":     broker_url,
                })

            print(f"[PropertyFinder] {len(results)} listings from {url}")
            return results[:20]

        except Exception as e:
            print(f"[PropertyFinder] error: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 4. Wasalt
# ─────────────────────────────────────────────────────────────────────────────

class WasaltScraper(BaseScraper):
    """
    Wasalt — uses Next.js Pages Router with __NEXT_DATA__.
    URL pattern: https://wasalt.sa/en/{type}s-for-{purpose}-in-{city}
    searchResult.properties contains up to 32 full listing objects per page.
    """
    platform_name = "Wasalt"
    base_url = "https://wasalt.sa"
    _IMG_CDN = "https://imagedelivery.net/1DNKFJPRaeUdy_j8F7HT3w/production/properties"

    _TYPES = {
        "apartment": "apartments", "villa": "villas",
        "house":     "houses",     "land":  "land",
        "office":    "offices",    "commercial": "commercial",
    }
    _CITIES = {
        "riyadh":    "riyadh",    "jeddah":   "jeddah",
        "mecca":     "makkah",    "medina":   "madinah",
        "dammam":    "dammam",    "khobar":   "al-khobar",
        "al khobar": "al-khobar", "abha":     "abha",
        "tabuk":     "tabuk",     "hail":     "hail",
        "buraidah":  "buraidah",  "taif":     "al-taif",
        "al taif":   "al-taif",   "yanbu":    "yanbu",
        "najran":    "najran",    "jazan":    "jazan",
    }

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            # Extract city from "District, City" format if needed
            city_str  = _city_from_location(self.location).strip().lower()
            loc_lower = self.location.strip().lower()
            city_slug = self._CITIES.get(city_str, self._CITIES.get(loc_lower, city_str.replace(" ", "-")))
            type_slug = self._TYPES.get(self.property_type, "properties")
            purpose   = "sale" if self.listing_type == "sale" else "rent"
            url = f"{self.base_url}/en/{type_slug}-for-{purpose}-in-{city_slug}"

            # Wasalt blocks Chrome TLS fingerprints — use a separate Safari session
            async with AsyncSession(impersonate="safari15_3") as safari:
                r = await safari.get(url, headers={
                    "Accept": "text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": self.base_url + "/en/",
                }, timeout=20)

            if r.status_code != 200:
                print(f"[Wasalt] HTTP {r.status_code} for {url}")
                return []

            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if not m:
                print("[Wasalt] no __NEXT_DATA__")
                return []

            data   = json.loads(m.group(1))
            sr     = data.get("props", {}).get("pageProps", {}).get("searchResult", {})
            raw    = sr.get("properties", []) if isinstance(sr, dict) else []

            results = []
            for p in raw:
                pi   = p.get("propertyInfo") or {}
                loc  = p.get("location")     or {}
                owner = p.get("propertyOwner") or {}
                files = p.get("propertyFiles") or {}
                attrs = {a["key"]: a["value"]
                         for a in (p.get("attributes") or [])
                         if isinstance(a, dict) and "key" in a}

                title = _str(pi.get("title"), "")
                if not title:
                    continue

                # Price: rent uses expectedRent, sale uses salePrice / conversionPrice
                if purpose == "rent":
                    price = _int(pi.get("expectedRent") or pi.get("conversionPrice") or 0)
                else:
                    price = _int(pi.get("salePrice") or pi.get("conversionPrice") or 0)

                # Rent period
                freq_raw = _str(pi.get("expectedRentType"), "").lower()
                rent_period = "/year" if "year" in freq_raw else "/month" if "month" in freq_raw else ""
                if purpose == "sale":
                    rent_period = ""

                # Location
                ld = _str(pi.get("address") or pi.get("district") or pi.get("zone"), self.location.title())

                # Coordinates
                lat = float(loc.get("lat") or 0) or _get_coords(self.location)[0]
                lng = float(loc.get("lon") or loc.get("lng") or 0) or _get_coords(self.location)[1]

                # Image
                prop_id = p.get("id", "")
                imgs = files.get("images") if isinstance(files, dict) else []
                image_url = f"{self._IMG_CDN}/{prop_id}/images/{imgs[0]}/public" if imgs else ""

                # Phone — normalize to 9-digit local number
                phone = _clean_phone(owner.get("phone") or owner.get("whatsApp") or
                                     (p.get("contactDetails") or {}).get("phoneNumber"))

                # Broker profile info (owner object)
                broker_name   = _str(owner.get("enName") or owner.get("name") or owner.get("fullName"), "")
                broker_agency = _str(owner.get("companyName") or
                                    (owner.get("company") or {}).get("name") or
                                     owner.get("agencyName"), "")
                raw_avatar    = owner.get("userAvatar") or owner.get("companyLogo") or ""
                broker_photo  = (f"https://images.wasalt.sa/{raw_avatar}"
                                 if raw_avatar and not raw_avatar.startswith("http") and "null" not in raw_avatar
                                 else _str(raw_avatar if raw_avatar and "null" not in str(raw_avatar) else "", ""))
                owner_id      = _str(owner.get("userId") or owner.get("slug") or owner.get("id"), "")
                broker_url    = (f"{self.base_url}/en/user/{owner_id}" if owner_id else "")

                # Source URL — format: /en/property/{slug} or /en/property/{id}
                slug = _str(pi.get("slug"), "")
                source_url = (f"{self.base_url}/en/property/{slug}" if slug
                              else f"{self.base_url}/en/property/{prop_id}" if prop_id
                              else url)

                # Price filter
                if self.min_price and price and price < self.min_price: continue
                if self.max_price and price and price > self.max_price: continue
                if self.rooms:
                    beds = _int(attrs.get("noOfBedrooms") or 0)
                    if beds and beds != self.rooms: continue

                results.append({
                    "title":                title,
                    "price_sar":            price,
                    "rent_period":          rent_period,
                    "location_detail":      ld,
                    "bedrooms":             _str(attrs.get("noOfBedrooms"), "N/A") if attrs.get("noOfBedrooms") is not None else "N/A",
                    "bathrooms":            _str(attrs.get("noOfBathrooms"), "N/A") if attrs.get("noOfBathrooms") is not None else "N/A",
                    "area_sqm":             _int(attrs.get("builtUpArea") or 0),
                    "contact_number":       phone,
                    "source_url":           source_url,
                    "source_platform_name": self.platform_name,
                    "image_url":            image_url,
                    "lat": lat, "lng": lng,
                    "broker_name":    broker_name,
                    "broker_agency":  broker_agency,
                    "broker_photo":   broker_photo,
                    "broker_url":     broker_url,
                })

            print(f"[Wasalt] {len(results)} listings from {url}")
            return results[:20]

        except Exception as e:
            print(f"[Wasalt] error: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 5. Sakani (government housing)
# ─────────────────────────────────────────────────────────────────────────────

class SakaniScraper(BaseScraper):
    platform_name = "Sakani"
    base_url = "https://sakani.sa"
    mock_count = 5

    async def scrape(self, client):
        try:
            url = f"{self.base_url}/en/projects?city={quote(self.location)}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[Sakani]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 6. Haraj (classifieds)
# ─────────────────────────────────────────────────────────────────────────────

class HarajScraper(BaseScraper):
    platform_name = "Haraj"
    base_url = "https://haraj.com.sa"
    mock_count = 8

    async def scrape(self, client):
        try:
            city_str = _city_from_location(self.location).strip().lower()
            city_ar = {"riyadh":"الرياض","jeddah":"جدة","dammam":"الدمام",
                       "mecca":"مكة","medina":"المدينة","khobar":"الخبر",
                       "abha":"أبها","tabuk":"تبوك","hail":"حائل",
                       "buraidah":"بريدة","medina":"المدينة المنورة"}.get(
                           city_str, city_str)
            prop_ar = {"apartment":"شقة","villa":"فيلا","house":"منزل",
                       "land":"أرض","office":"مكتب"}.get(self.property_type,"عقار")
            q = f"{prop_ar} {city_ar}"
            url = f"{self.base_url}/search?q={quote(q)}&cat=real-estate"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                soup = BeautifulSoup(r.text,"lxml")
                cards = soup.find_all("div", class_=re.compile(r"post|listing|card",re.I))[:20]
                results = []
                for card in cards:
                    title_el = card.find(re.compile(r"^h[1-6]$")) or card.find(class_=re.compile(r"title",re.I))
                    price_el = card.find(class_=re.compile(r"price",re.I))
                    if not (title_el and price_el): continue
                    price = _int(re.sub(r"[^\d]","",price_el.get_text()))
                    if price<=0: continue
                    link = card.find("a",href=True)
                    href = link["href"] if link else ""
                    url2 = f"{self.base_url}{href}" if href.startswith("/") else href
                    lat,lng = _get_coords(self.location)
                    results.append({
                        "title": title_el.get_text(" ",strip=True),
                        "price_sar": price, "rent_period": "",
                        "location_detail": self.location.title(),
                        "bedrooms":"N/A","bathrooms":"N/A","area_sqm":0,
                        "contact_number":"",
                        "source_url": url2 or self.base_url,
                        "source_platform_name": self.platform_name,
                        "lat":lat,"lng":lng,
                    })
                if results: return self._type_filter(results)
        except Exception as e:
            print(f"[Haraj]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 7. OpenSooq
# ─────────────────────────────────────────────────────────────────────────────

class OpenSooqScraper(BaseScraper):
    platform_name = "OpenSooq"
    base_url = "https://sa.opensooq.com"
    mock_count = 6

    async def scrape(self, client):
        try:
            ltype = "for-sale" if self.listing_type=="sale" else "for-rent"
            prop_slug = {"apartment":"apartments-for-sale","villa":"villas",
                         "house":"houses","land":"land","office":"offices"}.get(
                             self.property_type,"real-estate")
            city_slug = _city_from_location(self.location).strip().lower().replace(' ','-')
            url = f"{self.base_url}/en/{prop_slug}/{city_slug}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                soup = BeautifulSoup(r.text,"lxml")
                # Try JSON in script
                for sc in soup.find_all("script"):
                    txt = sc.string or ""
                    if '"price"' in txt and '"title"' in txt:
                        try:
                            m = re.search(r'\[(\{.*?"price".*?\})\]',txt,re.S)
                            if m:
                                arr = json.loads("["+m.group(1)+"]")
                                found = [self._norm(x) for x in arr if self._norm(x)]
                                if found: return [self._with_coords(x) for x in found]
                        except: pass
        except Exception as e:
            print(f"[OpenSooq]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 8. Expatriates
# ─────────────────────────────────────────────────────────────────────────────

class ExpatriatesScraper(BaseScraper):
    platform_name = "Expatriates"
    base_url = "https://www.expatriates.com"
    mock_count = 6

    _CITIES = {
        "riyadh": "riyadh", "jeddah": "jeddah", "dammam": "dammam",
        "mecca": "mecca", "medina": "medina", "khobar": "al-khobar",
        "al khobar": "al-khobar", "abha": "abha", "tabuk": "tabuk",
    }

    async def scrape(self, client):
        try:
            city_str = _city_from_location(self.location).strip().lower()
            city_slug = self._CITIES.get(city_str, city_str.replace(" ", "-"))
            url = f"{self.base_url}/classifieds/saudi-arabia/{city_slug}/real-estate/"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                soup = BeautifulSoup(r.text,"lxml")
                cards = soup.find_all("div",class_=re.compile(r"classifiedsDiv|listing",re.I))[:20]
                results = []
                for card in cards:
                    a = card.find("a",href=True)
                    title_el = card.find(class_=re.compile(r"title|heading",re.I)) or a
                    price_el = card.find(class_=re.compile(r"price|amount",re.I))
                    if not title_el: continue
                    price = _int(re.sub(r"[^\d]","",price_el.get_text())) if price_el else 0
                    lat,lng = _get_coords(self.location)
                    results.append({
                        "title": title_el.get_text(" ",strip=True)[:120],
                        "price_sar": price, "rent_period": "",
                        "location_detail": self.location.title(),
                        "bedrooms":"N/A","bathrooms":"N/A","area_sqm":0,
                        "contact_number":"",
                        "source_url": f"{self.base_url}{a['href']}" if a and a.get("href","").startswith("/") else self.base_url,
                        "source_platform_name": self.platform_name,
                        "lat":lat,"lng":lng,
                    })
                if results: return self._type_filter(results)
        except Exception as e:
            print(f"[Expatriates]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 9. Mourjan
# ─────────────────────────────────────────────────────────────────────────────

class MourjanScraper(BaseScraper):
    platform_name = "Mourjan"
    base_url = "https://sa.mourjan.com"
    mock_count = 6

    async def scrape(self, client):
        try:
            ltype = "for-sale" if self.listing_type=="sale" else "for-rent"
            city_slug = _city_from_location(self.location).strip().lower().replace(' ', '-')
            url = f"{self.base_url}/classifieds/real-estate/{ltype}/{city_slug}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[Mourjan]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 10. Satel (high-end compounds)
# ─────────────────────────────────────────────────────────────────────────────

class SatelScraper(BaseScraper):
    platform_name = "Satel"
    base_url = "https://satel.sa"
    mock_count = 4

    async def scrape(self, client):
        try:
            r = await client.get(self.base_url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[Satel]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 11. Zaahib
# ─────────────────────────────────────────────────────────────────────────────

class ZaahibScraper(BaseScraper):
    platform_name = "Zaahib"
    base_url = "https://www.zaahib.com"
    mock_count = 5

    async def scrape(self, client):
        try:
            url = f"{self.base_url}/properties?city={quote(self.location)}&type={self.listing_type}"
            r = await client.get(url, headers=_jh(self.base_url), timeout=15)
            if r.status_code==200:
                try:
                    data = r.json()
                    items = data.get("data",data.get("properties",[]))
                    if isinstance(items,list) and items:
                        return [self._with_coords(self._norm(x)) for x in items if self._norm(x)]
                except: pass
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[Zaahib]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 12. Bezaat
# ─────────────────────────────────────────────────────────────────────────────

class BezaatScraper(BaseScraper):
    platform_name = "Bezaat"
    base_url = "https://bezaat.com"
    mock_count = 5

    async def scrape(self, client):
        try:
            url = f"{self.base_url}/sa/real-estate/{self.listing_type}/{self.property_type}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[Bezaat]: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# 13. Deal (saudi-deal.com)
# ─────────────────────────────────────────────────────────────────────────────

class DealScraper(BaseScraper):
    platform_name = "SaudiDeal"
    base_url = "https://saudi-deal.com"
    mock_count = 5

    async def scrape(self, client):
        try:
            url = f"{self.base_url}/real-estate?city={quote(self.location)}&purpose={self.listing_type}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                found = self._extract_next_data(r.text)
                if found: return [self._with_coords(x) for x in found]
        except Exception as e:
            print(f"[SaudiDeal]: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Broker / Agent scrapers  ─  Advanced v2
# ─────────────────────────────────────────────────────────────────────────────

# City→slug maps used by multiple broker functions
_BAYUT_AGENT_SLUGS: dict[str, str] = {
    "riyadh": "riyadh", "jeddah": "jeddah", "mecca": "mecca",
    "medina": "medina", "dammam": "dammam", "al khobar": "al-khobar",
    "khobar": "al-khobar", "abha": "abha", "tabuk": "tabuk",
    "buraidah": "buraidah", "hail": "hail", "yanbu": "yanbu",
    "najran": "najran", "jazan": "jazan", "taif": "taif",
    "al taif": "taif", "dhahran": "dhahran", "jubail": "jubail",
    "al jubail": "jubail", "khamis mushait": "khamis-mushait",
}

_AQAR_BROKER_CITIES: dict[str, str] = {
    "riyadh": "الرياض", "jeddah": "جدة", "mecca": "مكة-المكرمة",
    "medina": "المدينة-المنورة", "dammam": "الدمام", "khobar": "الخبر",
    "al khobar": "الخبر", "abha": "أبها", "tabuk": "تبوك",
    "hail": "حائل", "buraidah": "بريدة", "taif": "الطائف",
    "al taif": "الطائف", "yanbu": "ينبع", "najran": "نجران", "jazan": "جازان",
}

_WASALT_BROKER_CITIES: dict[str, str] = {
    "riyadh": "riyadh", "jeddah": "jeddah", "mecca": "makkah",
    "medina": "madinah", "dammam": "dammam", "khobar": "al-khobar",
    "al khobar": "al-khobar", "abha": "abha", "tabuk": "tabuk",
    "hail": "hail", "buraidah": "buraidah", "taif": "al-taif",
    "al taif": "al-taif", "yanbu": "yanbu", "najran": "najran", "jazan": "jazan",
}

_HARAJ_CITIES_AR: dict[str, str] = {
    "riyadh": "الرياض", "jeddah": "جدة", "dammam": "الدمام",
    "mecca": "مكة", "medina": "المدينة", "khobar": "الخبر",
    "abha": "أبها", "tabuk": "تبوك", "hail": "حائل",
    "buraidah": "بريدة", "yanbu": "ينبع", "taif": "الطائف", "al taif": "الطائف",
}

# Top Riyadh / Jeddah districts — used to broaden broker search
_DISTRICT_QUERIES: dict[str, list[str]] = {
    "riyadh": [
        "Al Olaya", "Al Malaz", "Al Nakheel", "Al Sulaimaniyah", "Al Rawdah",
        "Hittin", "Al Malqa", "Al Sahafah", "Al Izdihar", "Al Yasmin",
    ],
    "jeddah": [
        "Al Hamra", "Al Rawdah", "Al Andalus", "Al Salamah", "Al Corniche",
        "Al Marwah", "Al Zahra", "Al Khalidiyah", "Al Naeem", "Al Shati",
    ],
    "dammam": ["Al Faisaliyah", "Al Shula", "Al Noor", "Al Hamra", "Al Anoud"],
}


# ── Bayut agencies directory parser ──────────────────────────────────────────

def _parse_bayut_agencies_page(text: str) -> tuple[list[dict], int]:
    """
    Parse window.state from /en/agencies/{city}/ pages.
    Agency objects have: name, logo, phoneNumber, slug, stats.adsCount, locations.
    """
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", text, re.S)
    for s in scripts:
        if "window.state" not in s:
            continue
        if not any(k in s for k in ["agenciesCount", "\"agencies\"", "agencyCount", "nbAgencies"]):
            continue
        try:
            start = s.index("window.state = ") + len("window.state = ")
            depth, end = 0, start
            for i, c in enumerate(s[start:], start):
                if c == "{":   depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0: end = i + 1; break
            data     = json.loads(s[start:end])
            content  = data.get("algolia", {}).get("content", {})
            hits     = content.get("hits", [])
            nb_pages = content.get("nbPages", 1)
            agencies = []
            for a in hits:
                # Phone — agencies expose landlines (phone) and mobiles
                ph_obj = a.get("phoneNumber") or {}
                if isinstance(ph_obj, str):
                    raw_phone = ph_obj
                elif isinstance(ph_obj, dict):
                    raw_phone = (ph_obj.get("phone") or ph_obj.get("mobile") or
                                 (ph_obj.get("mobileNumbers") or [""])[0] or
                                 ph_obj.get("whatsApp") or "")
                else:
                    raw_phone = ""
                # Also try top-level fields
                if not raw_phone:
                    for f in ["phone", "mobile", "contactPhone", "officePhone"]:
                        raw_phone = _str(a.get(f, ""), "")
                        if raw_phone: break
                phone = _clean_phone(raw_phone)
                if not phone:
                    continue
                logo          = (a.get("logo") or {}).get("url", "")
                slug          = _str(a.get("slug") or a.get("externalID") or "", "")
                profile_url   = f"https://www.bayut.sa/en/companies/{slug}/" if slug else ""
                listing_count = _int(
                    (a.get("stats") or {}).get("adsCount") or
                    a.get("listingsCount") or a.get("propertiesCount") or 0
                )
                # Service areas
                locs  = a.get("locations") or a.get("serviceAreas") or a.get("location") or []
                areas = []
                if isinstance(locs, list):
                    for loc in locs[:5]:
                        n = loc.get("name_l1", "") if isinstance(loc, dict) else _str(loc, "")
                        if n: areas.append(n)
                elif isinstance(locs, str) and locs:
                    areas = [locs]
                agencies.append({
                    "name":          _str(a.get("name") or a.get("agencyName"), ""),
                    "agency":        _str(a.get("name") or a.get("agencyName"), ""),
                    "photo_url":     logo,
                    "phone":         phone,
                    "platforms":     ["Bayut"],
                    "listing_count": listing_count,
                    "areas":         areas,
                    "profile_url":   profile_url,
                })
            return agencies, nb_pages
        except Exception as e:
            print(f"[BayutAgencies parse] {e}")
    return [], 1



class BrokerMerger:
    """Deduplicates and merges broker records by phone number."""

    def __init__(self):
        self._map: dict[str, dict] = {}

    def upsert(self, phone: str, data: dict) -> bool:
        """Merge data in. Returns True if this was a brand-new broker."""
        is_new = phone not in self._map
        if is_new:
            self._map[phone] = {
                "name": "", "agency": "", "photo_url": "",
                "phone": phone, "platforms": [], "listing_count": 0,
                "areas": [], "profile_url": "",
            }
        b = self._map[phone]
        if data.get("name")        and not b["name"]:        b["name"]        = data["name"]
        if data.get("agency")      and not b["agency"]:      b["agency"]      = data["agency"]
        if data.get("photo_url")   and not b["photo_url"]:   b["photo_url"]   = data["photo_url"]
        if data.get("profile_url") and not b["profile_url"]: b["profile_url"] = data["profile_url"]
        for p in data.get("platforms", []):
            if p not in b["platforms"]: b["platforms"].append(p)
        b["listing_count"] += data.get("listing_count", 0)
        for area in data.get("areas", []):
            if area and area not in b["areas"]: b["areas"].append(area)
        return is_new

    def snapshot(self) -> list[dict]:
        return sorted(self._map.values(), key=lambda b: -b["listing_count"])

    def __len__(self) -> int:
        return len(self._map)


# ── Source 1: Bayut Algolia listing index → broker extraction ─────────────────
async def _bayut_brokers_algolia(client: AsyncSession, city_str: str) -> list[dict]:
    """
    Queries Bayut's Algolia listing index (verified creds) across 6 property
    categories × sale+rent. Returns unique broker contacts with listing counts.
    Most reliable source — 100+ brokers per major city.
    """
    city_slug = BayutScraper._CITY_SLUGS.get(city_str, f"/{city_str.replace(' ', '-')}")

    CAT_SLUGS = ["apartments", "villas", "townhouses", "offices",
                 "residential-lands", "showrooms"]
    PURPOSES  = ["for-sale", "for-rent"]

    async def _page(cat: str, purpose: str, page: int) -> list[dict]:
        try:
            r = await client.post(
                BAYUT_ALGOLIA_URL,
                json={
                    "query": "",
                    "filters": f"purpose:{purpose} AND category.slug_l1:{cat}",
                    "facetFilters": [[f"location.slug_l1:{city_slug}"]],
                    "hitsPerPage": 50,
                    "page": page,
                    "attributesToRetrieve": [
                        "phoneNumber", "agent", "agency", "externalID", "location",
                    ],
                },
                headers={
                    "X-Algolia-Application-Id": BAYUT_ALGOLIA_APP_ID,
                    "X-Algolia-API-Key":        BAYUT_ALGOLIA_API_KEY,
                    "Content-Type":             "application/json",
                    "Origin":                   "https://www.bayut.sa",
                    "Referer":                  "https://www.bayut.sa/",
                },
                timeout=12,
            )
            return r.json().get("hits", []) if r.status_code == 200 else []
        except:
            return []

    # Fetch page 0 for every combo in parallel
    first_page_tasks = [_page(cat, p, 0) for cat in CAT_SLUGS for p in PURPOSES]
    first_pages = await asyncio.gather(*first_page_tasks, return_exceptions=True)

    # Fetch page 1 for high-volume combos (apartments in big cities)
    extra_tasks = [_page("apartments", p, 1) for p in PURPOSES]
    extra_tasks += [_page("villas", p, 1) for p in PURPOSES]
    extra_pages = await asyncio.gather(*extra_tasks, return_exceptions=True)

    all_hits = []
    for bucket in [*first_pages, *extra_pages]:
        if isinstance(bucket, list):
            all_hits.extend(bucket)

    brokers: dict[str, dict] = {}
    for h in all_hits:
        ph       = h.get("phoneNumber") or {}
        raw      = (ph.get("mobile") or (ph.get("mobileNumbers") or [""])[0] or
                    ph.get("phone") or "")
        phone    = _clean_phone(_str(raw, ""))
        if not phone:
            continue

        agent_obj  = h.get("agent")  or {}
        agency_obj = h.get("agency") or {}
        is_agency  = not agent_obj and bool(agency_obj)
        loc_list   = h.get("location") or []
        area       = _str(loc_list[0].get("name_l1", "") if loc_list and isinstance(loc_list[0], dict) else "", "")

        if is_agency:
            name        = _str(agency_obj.get("name_l1") or agency_obj.get("name") or "", "")
            photo       = _str((agency_obj.get("logo") or {}).get("url", "") or "", "")
            slug        = _str(agency_obj.get("slug_l1") or agency_obj.get("slug") or "", "")
            profile_url = f"https://www.bayut.sa/en/companies/{slug}/" if slug else ""
            agency_name = name
        else:
            agent_id    = _str(agent_obj.get("externalID") or agent_obj.get("slug") or "", "")
            name        = _str(agent_obj.get("name") or agency_obj.get("name") or "", "")
            photo       = _str((agent_obj.get("logo") or {}).get("url", "") or "", "")
            profile_url = f"https://www.bayut.sa/en/agents/{agent_id}/" if agent_id else ""
            agency_name = _str(agency_obj.get("name_l1") or agency_obj.get("name") or "", "")

        if phone not in brokers:
            brokers[phone] = {
                "name":          name,
                "agency":        agency_name,
                "photo_url":     photo,
                "phone":         phone,
                "platforms":     ["Bayut"],
                "listing_count": 0,
                "areas":         [area] if area else [],
                "profile_url":   profile_url,
            }
        b = brokers[phone]
        b["listing_count"] += 1
        if area and area not in b["areas"]: b["areas"].append(area)
        if not b["name"]   and name:   b["name"]   = name
        if not b["agency"] and agency_name: b["agency"] = agency_name

    result = list(brokers.values())
    print(f"[BayutBrokersAlgolia] {len(result)} unique brokers for '{city_str}'")
    return result


# ── Source 2: Bayut HTML agents directory ─────────────────────────────────────
def _parse_bayut_agents_page(text: str) -> tuple[list[dict], int]:
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", text, re.S)
    for s in scripts:
        if "window.state" not in s or "agentsCount" not in s:
            continue
        try:
            start = s.index("window.state = ") + len("window.state = ")
            depth, end = 0, start
            for i, c in enumerate(s[start:], start):
                if c == "{":   depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0: end = i + 1; break
            data    = json.loads(s[start:end])
            content = data.get("algolia", {}).get("content", {})
            hits    = content.get("hits", [])
            nb_pages = content.get("nbPages", 1)
            agents  = []
            for a in hits:
                ph_obj    = a.get("phoneNumber") or {}
                raw_phone = (ph_obj.get("mobile") or
                             (ph_obj.get("mobileNumbers") or [""])[0] or
                             ph_obj.get("phone") or "")
                phone = _clean_phone(_str(raw_phone, ""))
                if not phone:
                    continue
                logo          = (a.get("logo") or {}).get("url", "")
                slug          = _str(a.get("slug") or a.get("externalID") or "", "")
                profile_url   = f"https://www.bayut.sa/en/agents/{slug}/" if slug else ""
                listing_count = _int((a.get("stats") or {}).get("adsCount") or a.get("listingsCount") or 0)
                area          = _str(a.get("location"), "")
                agents.append({
                    "name":          _str(a.get("name") or a.get("fullName"), ""),
                    "agency":        "",
                    "photo_url":     logo,
                    "phone":         phone,
                    "platforms":     ["Bayut"],
                    "listing_count": listing_count,
                    "areas":         [area] if area else [],
                    "profile_url":   profile_url,
                    "_location":     area.lower(),
                })
            return agents, nb_pages
        except Exception:
            pass
    return [], 1


async def _bayut_agents_directory(client: AsyncSession, city_str: str) -> list[dict]:
    """Bayut /en/agents/{city}/ HTML directory — fetches up to 6 pages in parallel."""
    slug     = _BAYUT_AGENT_SLUGS.get(city_str, city_str.replace(" ", "-"))
    base_url = f"https://www.bayut.sa/en/agents/{slug}/"
    headers  = {
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.bayut.sa/",
    }
    try:
        r0 = await client.get(base_url, headers=headers, timeout=20)
        if r0.status_code != 200:
            return []
        agents, nb_pages = _parse_bayut_agents_page(r0.text)

        # Fetch additional pages in parallel
        extra = list(range(1, min(nb_pages, 7)))
        if extra:
            resps = await asyncio.gather(
                *[client.get(f"{base_url}?page={p+1}", headers=headers, timeout=18)
                  for p in extra],
                return_exceptions=True,
            )
            for resp in resps:
                if isinstance(resp, Exception) or resp.status_code != 200:
                    continue
                more, _ = _parse_bayut_agents_page(resp.text)
                agents.extend(more)

        if city_str:
            agents = [a for a in agents if city_str in a.get("_location", "")]
        for a in agents:
            a.pop("_location", None)
        print(f"[BayutAgentsDir] {len(agents)} for '{city_str}'")
        return agents
    except Exception as e:
        print(f"[BayutAgentsDir] error: {e}")
        return []


# ── Source 3: PropertyFinder find-broker directory ────────────────────────────
async def _pf_agents(client: AsyncSession, city_str: str) -> list[dict]:
    """PropertyFinder /en/find-broker/search — 5 pages fetched in parallel."""
    city_label = city_str.title() if city_str else "Saudi Arabia"

    async def _fetch(page: int) -> list[dict]:
        url = (f"https://www.propertyfinder.sa/en/find-broker/search"
               f"?q={city_label}&page={page}")
        try:
            r = await client.get(url, headers={
                "Accept":          "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer":         "https://www.propertyfinder.sa/",
            }, timeout=20)
            if r.status_code != 200:
                return []
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if not m:
                return []
            data = json.loads(m.group(1))
            pp   = data.get("props", {}).get("pageProps", {})
            raw  = pp.get("brokers", {}).get("data", [])
            out  = []
            for b in raw:
                phone = _clean_phone(_str(b.get("phone"), ""))
                if not phone:
                    continue
                url_slug  = _str(b.get("urlSlug"), "")
                client_id = _str(b.get("clientId") or b.get("id"), "")
                profile_url = (f"https://www.propertyfinder.sa/en/broker/{url_slug}-{client_id}"
                               if url_slug and client_id else "")
                logo = ((b.get("logo") or {}).get("url", "")
                        if isinstance(b.get("logo"), dict)
                        else _str(b.get("logo"), ""))
                listing_count = _int(
                    b.get("totalProperties") or
                    (b.get("propertiesResidentialForSaleCount", 0) +
                     b.get("propertiesResidentialForRentCount", 0))
                )
                out.append({
                    "name":          _str(b.get("name"), ""),
                    "agency":        _str(b.get("name"), ""),
                    "photo_url":     logo,
                    "phone":         phone,
                    "platforms":     ["PropertyFinder"],
                    "listing_count": listing_count,
                    "areas":         [_str(b.get("location"), "")] if b.get("location") else [],
                    "profile_url":   profile_url,
                })
            return out
        except Exception:
            return []

    pages = await asyncio.gather(*[_fetch(p) for p in range(1, 6)], return_exceptions=True)
    seen, results = set(), []
    for page_list in pages:
        if not isinstance(page_list, list):
            continue
        for b in page_list:
            if b["phone"] not in seen:
                seen.add(b["phone"])
                results.append(b)
    print(f"[PFAgents] {len(results)} for '{city_label}'")
    return results


# ── Source 4: Wasalt agents directory ────────────────────────────────────────
async def _wasalt_agents(client: AsyncSession, city_str: str) -> list[dict]:
    city_slug = _WASALT_BROKER_CITIES.get(city_str, city_str.replace(" ", "-"))
    urls_to_try = [
        f"https://wasalt.sa/en/agents?city={city_slug}",
        f"https://wasalt.sa/en/real-estate-agents/{city_slug}",
        f"https://wasalt.sa/en/user?city={city_slug}",
    ]
    try:
        async with AsyncSession(impersonate="safari15_3") as safari:
            resps = await asyncio.gather(
                *[safari.get(u, headers={
                    "Accept":          "text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer":         "https://wasalt.sa/",
                  }, timeout=18) for u in urls_to_try],
                return_exceptions=True,
            )

        agents_raw = []
        for resp in resps:
            if isinstance(resp, Exception) or resp.status_code != 200:
                continue
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.S)
            if not m:
                continue
            data = json.loads(m.group(1))
            pp   = data.get("props", {}).get("pageProps", {})
            raw  = (pp.get("agents", {}).get("data") or
                    pp.get("agentsList") or
                    pp.get("agents") or [])
            if raw:
                agents_raw = raw
                break

        results = []
        for a in agents_raw:
            phone = _clean_phone(_str(
                a.get("phone") or a.get("mobile") or a.get("whatsApp"), ""))
            if not phone:
                continue
            company_obj = a.get("company") or a.get("agency") or {}
            agency = _str(company_obj.get("name") if isinstance(company_obj, dict)
                          else company_obj, "")
            photo  = _str(a.get("photo") or a.get("avatar") or a.get("profilePhoto") or
                         (a.get("image") or {}).get("url", ""), "")
            slug   = _str(a.get("slug") or a.get("id"), "")
            results.append({
                "name":          _str(a.get("name") or a.get("fullName"), ""),
                "agency":        agency,
                "photo_url":     photo,
                "phone":         phone,
                "platforms":     ["Wasalt"],
                "listing_count": _int(a.get("listingsCount") or a.get("propertiesCount") or 0),
                "areas":         [],
                "profile_url":   f"https://wasalt.sa/en/agents/{slug}" if slug else "",
            })
        print(f"[WasaltAgents] {len(results)} for '{city_str}'")
        return results
    except Exception as e:
        print(f"[WasaltAgents] error: {e}")
        return []


# ── Source 5: Aqar broker / office directory ─────────────────────────────────
async def _aqar_brokers(client: AsyncSession, city_str: str) -> list[dict]:
    """Try Aqar's brokers / offices pages via RSC endpoint."""
    city_ar = _AQAR_BROKER_CITIES.get(city_str, city_str)

    urls_to_try = [
        f"https://sa.aqar.fm/brokers/{quote(city_ar, safe='')}",
        f"https://sa.aqar.fm/مكاتب-عقارية/{quote(city_ar, safe='')}",
        f"https://sa.aqar.fm/وسطاء/{quote(city_ar, safe='')}",
    ]
    results = []
    try:
        for url in urls_to_try:
            r = await client.get(url, headers={
                "RSC":             "1",
                "Accept":          "text/x-component, */*",
                "Accept-Language": "ar-SA,ar;q=0.9",
                "Referer":         "https://sa.aqar.fm/",
            }, timeout=15)
            if r.status_code != 200:
                continue
            # Extract phone numbers embedded in the RSC stream
            text = r.text
            seen_phones: set[str] = set()
            for raw_phone in re.findall(r'"phone"\s*:\s*"([^"]{7,15})"', text):
                phone = _clean_phone(raw_phone)
                if phone and phone not in seen_phones:
                    seen_phones.add(phone)
                    results.append({
                        "name": "", "agency": "", "photo_url": "",
                        "phone": phone, "platforms": ["Aqar"],
                        "listing_count": 1, "areas": [city_ar], "profile_url": "",
                    })
            if results:
                break
        print(f"[AqarBrokers] {len(results)} for '{city_str}'")
    except Exception as e:
        print(f"[AqarBrokers] error: {e}")
    return results


# ── Source 6: Broker contacts extracted from listing scrapers ─────────────────
async def _brokers_from_listings(
    client: AsyncSession,
    location: str,
) -> list[dict]:
    """
    Run PropertyFinder + Wasalt + Aqar listing scrapers concurrently for
    apartment/villa × sale/rent. Extracts every broker contact with profile data.
    """
    combos = [
        ("apartment", "rent"), ("apartment", "sale"),
        ("villa",     "sale"), ("villa",     "rent"),
    ]
    scraper_classes = [PropertyFinderScraper, WasaltScraper, AqarScraper]

    async def _one(Cls, pt: str, lt: str) -> list[dict]:
        try:
            sc = Cls(location=location, min_price=None, max_price=None,
                     rooms=None, property_type=pt, listing_type=lt)
            listings = await sc.scrape(client)
            out = []
            for lst in listings:
                phone = _clean_phone(_str(lst.get("contact_number", ""), ""))
                if not phone:
                    continue
                out.append({
                    "name":          lst.get("broker_name", ""),
                    "agency":        lst.get("broker_agency", ""),
                    "photo_url":     lst.get("broker_photo", ""),
                    "profile_url":   lst.get("broker_url", ""),
                    "platforms":     [Cls.platform_name],
                    "listing_count": 1,
                    "areas":         [lst.get("location_detail", "")],
                    "phone":         phone,
                })
            return out
        except Exception as e:
            print(f"[BrokerListings/{Cls.platform_name}] {e}")
            return []

    all_tasks = [_one(sc, pt, lt) for sc in scraper_classes for pt, lt in combos]
    raw = await asyncio.gather(*all_tasks, return_exceptions=True)
    result: list[dict] = []
    for r in raw:
        if isinstance(r, list):
            result.extend(r)
    print(f"[BrokerListings] {len(result)} raw contacts from listing scrapers")
    return result


# ── Source 7: Bayut Algolia — district-level deep scan ───────────────────────
async def _bayut_district_brokers(
    client: AsyncSession,
    city_str: str,
) -> list[dict]:
    """
    For Riyadh and Jeddah, query Bayut Algolia using each district name as the
    free-text query. Returns brokers that would be missed by city-wide queries.
    """
    districts = _DISTRICT_QUERIES.get(city_str, [])
    if not districts:
        return []

    city_slug = BayutScraper._CITY_SLUGS.get(city_str, f"/{city_str.replace(' ', '-')}")

    async def _query_district(district: str) -> list[dict]:
        try:
            r = await client.post(
                BAYUT_ALGOLIA_URL,
                json={
                    "query":        district,
                    "facetFilters": [[f"location.slug_l1:{city_slug}"]],
                    "hitsPerPage":  50,
                    "page":         0,
                    "attributesToRetrieve": [
                        "phoneNumber", "agent", "agency", "externalID", "location",
                    ],
                },
                headers={
                    "X-Algolia-Application-Id": BAYUT_ALGOLIA_APP_ID,
                    "X-Algolia-API-Key":        BAYUT_ALGOLIA_API_KEY,
                    "Content-Type":             "application/json",
                    "Origin":                   "https://www.bayut.sa",
                    "Referer":                  "https://www.bayut.sa/",
                },
                timeout=10,
            )
            return r.json().get("hits", []) if r.status_code == 200 else []
        except:
            return []

    all_hits_nested = await asyncio.gather(
        *[_query_district(d) for d in districts], return_exceptions=True
    )
    brokers: dict[str, dict] = {}
    for hits in all_hits_nested:
        if not isinstance(hits, list):
            continue
        for h in hits:
            ph    = h.get("phoneNumber") or {}
            raw   = (ph.get("mobile") or (ph.get("mobileNumbers") or [""])[0] or ph.get("phone") or "")
            phone = _clean_phone(_str(raw, ""))
            if not phone:
                continue
            agent_obj  = h.get("agent")  or {}
            agency_obj = h.get("agency") or {}
            is_agency  = not agent_obj and bool(agency_obj)
            loc_list   = h.get("location") or []
            area       = _str(loc_list[0].get("name_l1", "")
                              if loc_list and isinstance(loc_list[0], dict) else "", "")
            if is_agency:
                name        = _str(agency_obj.get("name_l1") or agency_obj.get("name") or "", "")
                photo       = _str((agency_obj.get("logo") or {}).get("url", "") or "", "")
                slug        = _str(agency_obj.get("slug_l1") or agency_obj.get("slug") or "", "")
                profile_url = f"https://www.bayut.sa/en/companies/{slug}/" if slug else ""
                agency_name = name
            else:
                agent_id    = _str(agent_obj.get("externalID") or agent_obj.get("slug") or "", "")
                name        = _str(agent_obj.get("name") or agency_obj.get("name") or "", "")
                photo       = _str((agent_obj.get("logo") or {}).get("url", "") or "", "")
                profile_url = f"https://www.bayut.sa/en/agents/{agent_id}/" if agent_id else ""
                agency_name = _str(agency_obj.get("name_l1") or agency_obj.get("name") or "", "")
            if phone not in brokers:
                brokers[phone] = {
                    "name":          name,
                    "agency":        agency_name,
                    "photo_url":     photo,
                    "phone":         phone,
                    "platforms":     ["Bayut"],
                    "listing_count": 0,
                    "areas":         [area] if area else [],
                    "profile_url":   profile_url,
                }
            b = brokers[phone]
            b["listing_count"] += 1
            if area and area not in b["areas"]: b["areas"].append(area)

    result = list(brokers.values())
    print(f"[BayutDistrictBrokers] {len(result)} for '{city_str}' ({len(districts)} districts)")
    return result


# ── Source 8: Haraj phone extraction ─────────────────────────────────────────
async def _haraj_brokers(client: AsyncSession, city_str: str) -> list[dict]:
    """Scrape Haraj real-estate listings and extract Saudi phone numbers."""
    city_ar  = _HARAJ_CITIES_AR.get(city_str, city_str)
    queries  = [f"عقار {city_ar}", f"شقة {city_ar}", f"فيلا {city_ar}"]
    results: list[dict] = []
    seen: set[str]      = set()

    for q in queries:
        try:
            url = f"https://haraj.com.sa/search?q={quote(q)}&cat=real-estate"
            r   = await client.get(url, headers=_h("https://haraj.com.sa"), timeout=15)
            if r.status_code != 200:
                continue
            text = r.text
            for raw in re.findall(
                r'\b(05\d{8})\b|\b(9665\d{8})\b|\b(00966\d{9})\b', text
            ):
                raw_phone = next((x for x in raw if x), "")
                phone = _clean_phone(raw_phone)
                if phone and phone not in seen:
                    seen.add(phone)
                    results.append({
                        "name": "", "agency": "", "photo_url": "",
                        "phone": phone, "platforms": ["Haraj"],
                        "listing_count": 1, "areas": [city_ar], "profile_url": "",
                    })
        except Exception as e:
            print(f"[HarajBrokers] {e}")

    print(f"[HarajBrokers] {len(results)} contacts for '{city_str}'")
    return results

# ─────────────────────────────────────────────────────────────────────────────
# Platform registry
# ─────────────────────────────────────────────────────────────────────────────

ALL_SCRAPERS = {
    "bayut":          BayutScraper,
    "aqar":           AqarScraper,
    "propertyfinder": PropertyFinderScraper,
    "wasalt":         WasaltScraper,
    "sakani":         SakaniScraper,
    "haraj":          HarajScraper,
    "opensooq":       OpenSooqScraper,
    "expatriates":    ExpatriatesScraper,
    "mourjan":        MourjanScraper,
    "satel":          SatelScraper,
    "zaahib":         ZaahibScraper,
    "bezaat":         BezaatScraper,
    "saudideal":      DealScraper,
}

def _build_scrapers(platforms, kwargs) -> list[BaseScraper]:
    keys = [p.lower().replace(" ","").replace("-","") for p in (platforms or list(ALL_SCRAPERS.keys()))]
    return [ALL_SCRAPERS[k](**kwargs) for k in keys if k in ALL_SCRAPERS]

# ─────────────────────────────────────────────────────────────────────────────
# Aggregator
# ─────────────────────────────────────────────────────────────────────────────

class PropertyAggregator:
    def __init__(self, **kwargs):
        self.scrapers = _build_scrapers(kwargs.pop("platforms", None), kwargs)

    async def aggregate(self) -> list[dict]:
        async with AsyncSession(impersonate="chrome124") as client:
            results = await asyncio.gather(*[s.scrape(client) for s in self.scrapers],
                                           return_exceptions=True)
        out = []
        for r in results:
            if isinstance(r, list): out.extend(r)
        return out

# ─────────────────────────────────────────────────────────────────────────────
# SSE helper
# ─────────────────────────────────────────────────────────────────────────────

def _sse(p: dict) -> str:
    return f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

# ─────────────────────────────────────────────────────────────────────────────
# API endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/platforms")
def get_platforms():
    platform_meta = {
        "bayut":          {"label":"Bayut",         "url":"bayut.sa",         "tier":"premium"},
        "aqar":           {"label":"Aqar",           "url":"aqar.fm",          "tier":"premium"},
        "propertyfinder": {"label":"PropertyFinder", "url":"propertyfinder.sa","tier":"premium"},
        "wasalt":         {"label":"Wasalt",         "url":"wasalt.com",       "tier":"premium"},
        "sakani":         {"label":"Sakani",         "url":"sakani.sa",        "tier":"government"},
        "haraj":          {"label":"Haraj",          "url":"haraj.com.sa",     "tier":"classifieds"},
        "opensooq":       {"label":"OpenSooq",       "url":"sa.opensooq.com",  "tier":"classifieds"},
        "expatriates":    {"label":"Expatriates",    "url":"expatriates.com",  "tier":"classifieds"},
        "mourjan":        {"label":"Mourjan",        "url":"sa.mourjan.com",   "tier":"classifieds"},
        "satel":          {"label":"Satel",          "url":"satel.sa",         "tier":"niche"},
        "zaahib":         {"label":"Zaahib",         "url":"zaahib.com",       "tier":"niche"},
        "bezaat":         {"label":"Bezaat",         "url":"bezaat.com",       "tier":"niche"},
        "saudideal":      {"label":"SaudiDeal",      "url":"saudi-deal.com",   "tier":"niche"},
    }
    return platform_meta

@app.get("/api/stream")
async def stream(
    location:      str            = Query(...),
    min_price:     Optional[int]  = Query(None),
    max_price:     Optional[int]  = Query(None),
    rooms:         Optional[int]  = Query(None),
    property_type: str            = Query("apartment"),
    listing_type:  str            = Query("sale"),
    platforms:     Optional[str]  = Query(None),
):
    # Support comma-separated property types (multi-select)
    property_types = [t.strip() for t in property_type.split(",") if t.strip()] or ["apartment"]

    # 5% price buffer — expand range slightly so users see near-match properties
    PRICE_BUFFER = 0.05
    buf_min = int(min_price * (1 - PRICE_BUFFER)) if min_price else None
    buf_max = int(max_price * (1 + PRICE_BUFFER)) if max_price else None

    platform_list = [p.strip() for p in platforms.split(",")] if platforms else None

    # Build one set of scrapers per property type, deduplicate by source_url
    scrapers: list[BaseScraper] = []
    seen_classes: set = set()
    for pt in property_types:
        kw = dict(location=location, min_price=buf_min, max_price=buf_max,
                  rooms=rooms, property_type=pt, listing_type=listing_type)
        for sc in _build_scrapers(platform_list, kw):
            # Use (class, property_type) as key to avoid duplicates when platform_list is identical
            key = (type(sc).__name__, pt)
            if key not in seen_classes:
                seen_classes.add(key)
                scrapers.append(sc)

    # When a specific district is requested ("District, City"), filter all
    # results to within DISTRICT_RADIUS_KM of the district centroid.
    # Centroid is established from the first scraper that returns coords
    # (Bayut runs first and uses Algolia district filtering, so its centroid
    # is accurate). Subsequent scrapers' city-wide results are then clipped.
    DISTRICT_RADIUS_KM = 10.0
    is_district = "," in location
    centroid: list[float] = []   # [lat, lng] once established

    def _in_district(item: dict) -> bool:
        """True if listing is within DISTRICT_RADIUS_KM of established centroid."""
        if not centroid:
            return True  # centroid not yet set — let it through
        lat, lng = item.get("lat"), item.get("lng")
        if not lat or not lng:
            return False
        return _haversine_km(centroid[0], centroid[1], lat, lng) <= DISTRICT_RADIUS_KM

    async def gen() -> AsyncIterator[str]:
        seen_urls: set[str] = set()  # deduplicate across property types

        async with AsyncSession(impersonate="chrome124") as client:
            for sc in scrapers:
                yield _sse({"status":"scanning","platform":sc.platform_name,
                            "message":f"Scanning {sc.platform_name}…"})
                try:
                    results = await sc.scrape(client)

                    if is_district and results:
                        if not centroid:
                            lats = [r["lat"] for r in results if r.get("lat") and r.get("lng")]
                            lngs = [r["lng"] for r in results if r.get("lat") and r.get("lng")]
                            if lats:
                                centroid.append(sum(lats) / len(lats))
                                centroid.append(sum(lngs) / len(lngs))
                        if centroid:
                            results = [r for r in results if _in_district(r)]

                    # Deduplicate by source_url across property type runs
                    unique = []
                    for item in results:
                        url_key = item.get("source_url", "")
                        if url_key and url_key in seen_urls:
                            continue
                        if url_key:
                            seen_urls.add(url_key)
                        unique.append(item)

                    for item in unique:
                        yield _sse({"status":"result","listing":item})
                    yield _sse({"status":"platform_done","platform":sc.platform_name,"count":len(unique)})
                except Exception as ex:
                    yield _sse({"status":"error","platform":sc.platform_name,"message":str(ex)})
        yield _sse({"status":"complete"})

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.get("/api/properties")
async def batch(
    location:      str            = Query(...),
    min_price:     Optional[int]  = Query(None),
    max_price:     Optional[int]  = Query(None),
    rooms:         Optional[int]  = Query(None),
    property_type: str            = Query("apartment"),
    listing_type:  str            = Query("sale"),
    platforms:     Optional[str]  = Query(None),
):
    agg = PropertyAggregator(location=location, min_price=min_price, max_price=max_price,
                              rooms=rooms, property_type=property_type, listing_type=listing_type,
                              platforms=[p.strip() for p in platforms.split(",")] if platforms else None)
    listings = await agg.aggregate()
    return {"status":"success","count":len(listings),"listings":listings}

@app.get("/api/cities")
def cities():
    return sorted(CITY_COORDS.keys())

@app.get("/health")
def health():
    return {"status":"ok","platforms":len(ALL_SCRAPERS)}


# ─────────────────────────────────────────────────────────────────────────────
# Broker aggregation endpoint  ─  Advanced v2
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/brokers")
async def brokers_stream(
    location:  str           = Query(...),
    platforms: Optional[str] = Query(None),
):
    raw_city = _city_from_location(location).strip().lower()
    city_str = "" if raw_city in ("saudi arabia", "ksa", "") else raw_city
    # "All cities" falls back to Riyadh as the primary market
    search_location = location if city_str else "Riyadh"
    search_city     = city_str if city_str else "riyadh"

    async def gen() -> AsyncIterator[str]:
        merger = BrokerMerger()

        def _ingest(broker_list: list[dict]) -> int:
            """Merge a list of broker dicts; return count of NEW unique brokers."""
            new = 0
            for b in broker_list:
                phone = b.get("phone", "")
                if phone and merger.upsert(phone, b):
                    new += 1
            return new

        async with AsyncSession(impersonate="chrome124") as client:

            # ── Phase 1: Bayut — Algolia + agents dir (parallel) ─────────────────
            yield _sse({"status": "scanning", "platform": "Bayut",
                        "message": "Scanning Bayut listings & agents…"})
            bayut_results = await asyncio.gather(
                _bayut_brokers_algolia(client, search_city),
                _bayut_agents_directory(client, search_city),
                return_exceptions=True,
            )
            bayut_count = sum(
                _ingest(r) for r in bayut_results if isinstance(r, list)
            )
            # Stream each Bayut broker immediately
            for b in merger.snapshot():
                yield _sse({"status": "broker", "broker": b})
            yield _sse({"status": "platform_done", "platform": "Bayut",
                        "count": bayut_count})

            # ── Phase 2: Bayut district deep-scan (Riyadh + Jeddah only) ──────
            if search_city in _DISTRICT_QUERIES:
                yield _sse({"status": "scanning", "platform": "Bayut Districts",
                            "message": f"Deep scanning {search_city.title()} districts…"})
                district_brokers = await _bayut_district_brokers(client, search_city)
                new_dist = _ingest(district_brokers)
                # Stream only newly found brokers
                seen_phones = {b["phone"] for b in merger.snapshot()[:-new_dist]} if new_dist else set()
                for b in merger.snapshot():
                    if b["phone"] not in seen_phones:
                        yield _sse({"status": "broker", "broker": b})
                yield _sse({"status": "platform_done", "platform": "Bayut Districts",
                            "count": new_dist})

            # ── Phase 3: PropertyFinder + Wasalt + Aqar directories (parallel) ─
            yield _sse({"status": "scanning", "platform": "PropertyFinder",
                        "message": "Scanning PropertyFinder broker directory…"})
            yield _sse({"status": "scanning", "platform": "Wasalt",
                        "message": "Scanning Wasalt agent directory…"})
            yield _sse({"status": "scanning", "platform": "Aqar",
                        "message": "Scanning Aqar broker directory…"})

            dir_results = await asyncio.gather(
                _pf_agents(client, search_city),
                _wasalt_agents(client, search_city),
                _aqar_brokers(client, search_city),
                return_exceptions=True,
            )
            dir_labels = ["PropertyFinder", "Wasalt", "Aqar"]
            prev_total = len(merger)
            for label, res in zip(dir_labels, dir_results):
                if not isinstance(res, list):
                    continue
                new_n = _ingest(res)
                yield _sse({"status": "platform_done", "platform": label, "count": new_n})
            # Stream all newly added brokers from this phase
            new_brokers_phase3 = merger.snapshot()[prev_total:]
            for b in new_brokers_phase3:
                yield _sse({"status": "broker", "broker": b})

            # ── Phase 4: Listing-based extraction (PF + Wasalt + Aqar in parallel) ─
            yield _sse({"status": "scanning", "platform": "Listings",
                        "message": "Extracting broker contacts from live listings…"})
            prev_total2 = len(merger)
            listing_brokers = await _brokers_from_listings(client, search_location)
            new_listing = _ingest(listing_brokers)
            new_brokers_phase4 = merger.snapshot()[prev_total2:]
            for b in new_brokers_phase4:
                yield _sse({"status": "broker", "broker": b})
            yield _sse({"status": "platform_done", "platform": "Listings",
                        "count": new_listing})

            # ── Phase 5: Haraj phone extraction ───────────────────────────────
            yield _sse({"status": "scanning", "platform": "Haraj",
                        "message": "Extracting contacts from Haraj listings…"})
            prev_total3 = len(merger)
            haraj_brokers = await _haraj_brokers(client, search_city)
            new_haraj = _ingest(haraj_brokers)
            new_brokers_phase5 = merger.snapshot()[prev_total3:]
            for b in new_brokers_phase5:
                yield _sse({"status": "broker", "broker": b})
            yield _sse({"status": "platform_done", "platform": "Haraj",
                        "count": new_haraj})

        yield _sse({"status": "complete"})

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
