"""
Property listing scrapers and API endpoints.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import AsyncIterator, Optional
from urllib.parse import quote, urlencode

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from shared import (
    BAYUT_ALGOLIA_APP_ID, BAYUT_ALGOLIA_API_KEY, BAYUT_ALGOLIA_URL,
    PROPERTY_IMAGES,
    _h, _jh, _city_from_location, _haversine_km, _get_coords,
    _int, _str, _clean_phone, _sse,
)

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Platform search URL builder
# ─────────────────────────────────────────────────────────────────────────────

def _platform_search_url(platform_name: str, location: str, prop_type: str,
                         listing_type: str, min_price: Optional[int],
                         max_price: Optional[int], rooms: Optional[int]) -> str:
    loc       = location.strip()
    loc_lower = loc.lower()
    loc_slug  = loc_lower.replace(" ", "-")
    ltype     = "sale" if listing_type == "sale" else "rent"

    BAYUT_TYPES = {
        "apartment":"apartments", "villa":"villas", "house":"houses",
        "office":"offices", "land":"land", "commercial":"commercial-spaces",
    }
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
        type_ar = AQAR_TYPE_AR.get(prop_type, "عقار")
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        purpose_ar = "للبيع" if ltype == "sale" else "للإيجار"
        q = quote(f"{type_ar} {purpose_ar} في {city_ar}")
        return f"https://aqar.fm/search?q={q}"

    if platform_name == "PropertyFinder":
        seg = "buy" if ltype == "sale" else "rent"
        type_slug = (PF_SALE_TYPES if ltype == "sale" else PF_RENT_TYPES).get(prop_type, f"properties-for-{ltype}")
        return f"https://www.propertyfinder.sa/en/{seg}/{type_slug}-in-{loc_slug}/"

    if platform_name == "Wasalt":
        purpose = "buy" if ltype == "sale" else "rent"
        prop_slug = {"apartment":"apartment","villa":"villa","house":"house",
                     "office":"office","land":"land","commercial":"commercial"}.get(prop_type,"apartment")
        return f"https://wasalt.com/en/properties?purpose={purpose}&type={prop_slug}&city={loc_slug}"

    if platform_name == "Sakani":
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        return f"https://sakani.sa/en/projects?city={quote(city_ar)}"

    if platform_name == "Haraj":
        type_ar = AQAR_TYPE_AR.get(prop_type, "عقار")
        city_ar = AQAR_CITY_AR.get(loc_lower, loc)
        q = quote(f"{type_ar} {city_ar}")
        return f"https://haraj.com.sa/search?q={q}"

    if platform_name == "OpenSooq":
        cat_map = {
            "apartment": "apartments",  "villa": "villas",
            "house": "houses",          "land": "lands",
            "office": "offices",        "commercial": "commercial-properties",
        }
        purpose = "for-sale" if ltype == "sale" else "for-rent"
        cat = cat_map.get(prop_type, "real-estate")
        return f"https://sa.opensooq.com/{cat}-{purpose}/{loc_slug}"

    if platform_name == "Expatriates":
        sub = "for-sale" if ltype == "sale" else "for-rent"
        return f"https://www.expatriates.com/classifieds/saudi-arabia/real-estate/{sub}/"

    if platform_name == "Mourjan":
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
# Base scraper
# ─────────────────────────────────────────────────────────────────────────────

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
        inc = _TYPE_INCLUDE.get(self.property_type, [])
        exc = _TYPE_EXCLUDE.get(self.property_type, [])
        if not inc and not exc:
            return results
        filtered = []
        for r in results:
            t = (r.get("title","") or "").lower()
            if any(k in t for k in exc):
                continue
            filtered.append(r)
        return filtered

    def _with_coords(self, item: dict) -> dict:
        if "lat" not in item or not item["lat"]:
            import random
            lat, lng = _get_coords(self.location)
            item["lat"] = lat
            item["lng"] = lng
        if "area_sqm" not in item:
            import random
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
        if slug and not str(slug).isdigit():
            url = f"{self.base_url}/property/{slug}/"
        elif slug and str(slug).isdigit() and self.platform_name == "Bayut":
            url = f"{self.base_url}/property/{slug}/"
        else:
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
        import random
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
    platform_name = "Bayut"
    base_url      = "https://www.bayut.sa"

    _CAT_SLUGS = {
        "apartment": "apartments",
        "villa":     "villas",
        "house":     "townhouses",
        "office":    "offices",
        "land":      "residential-lands",
        "commercial":"showrooms",
    }
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

        loc_list = h.get("location") or []
        ld = " › ".join(x.get("name_l1","") for x in loc_list if isinstance(x, dict) and x.get("name_l1")).strip(" ›") or self.location

        ext_id = h.get("externalID","")
        source_url = f"{self.base_url}/property/details-{ext_id}.html" if ext_id else self.base_url

        cover = h.get("coverPhoto") or {}
        cover_id = cover.get("id") if isinstance(cover, dict) else None
        image_url = f"https://images.bayut.sa/thumbnails/{cover_id}-400x300.jpeg" if cover_id else ""

        ph = h.get("phoneNumber") or {}
        contact = ""
        if isinstance(ph, dict):
            raw_phone = ph.get("mobile") or ph.get("phone") or (ph.get("phoneNumbers") or [None])[0]
            contact = _clean_phone(raw_phone)

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
            "broker_name":    broker_name,
            "broker_agency":  broker_agency,
            "broker_photo":   broker_photo,
            "broker_url":     broker_url,
        }

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            purpose    = "for-sale" if self.listing_type == "sale" else "for-rent"
            cat_slug   = self._CAT_SLUGS.get(self.property_type, "apartments")

            city_str   = _city_from_location(self.location).strip().lower()
            city_slug  = self._CITY_SLUGS.get(city_str,
                             f"/{city_str.replace(' ', '-')}")
            district_q = ""
            if "," in self.location:
                district_q = self.location.split(",")[0].strip()

            filters = f"purpose:{purpose} AND category.slug_l1:{cat_slug}"
            if self.min_price: filters += f" AND price>={self.min_price}"
            if self.max_price: filters += f" AND price<={self.max_price}"
            if self.rooms:     filters += f" AND rooms={self.rooms}"

            results = []
            for page in range(2):
                payload = {
                    "query": district_q,
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
                    if len(hits) < 20: break
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
    platform_name = "Aqar"
    base_url = "https://sa.aqar.fm"

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

    async def _fetch_rsc(self, client: AsyncSession, url: str) -> str:
        """
        Fetch Aqar RSC stream. Tries direct request first, then FlareSolverr
        at localhost:8191 if Cloudflare blocks the direct request.
        """
        # Direct attempt
        try:
            r = await client.get(url, headers={
                "RSC": "1",
                "Accept": "text/x-component, */*",
                "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                "Referer": self.base_url + "/",
            }, timeout=20)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass

        # FlareSolverr fallback (handles Cloudflare JS challenge + returns cookies)
        try:
            async with AsyncSession() as fs:
                fs_resp = await fs.post(
                    "http://localhost:8191/v1",
                    json={"cmd": "request.get", "url": url, "maxTimeout": 60000},
                    timeout=70,
                )
                if fs_resp.status_code != 200:
                    return ""
                sol = fs_resp.json().get("solution", {})
                html = sol.get("response", "")
                if not html:
                    return ""
                cookies = {c["name"]: c["value"] for c in sol.get("cookies", [])}
                ua = sol.get("userAgent", "")
                # Use the solved cookies to fetch RSC data
                r2 = await client.get(url, headers={
                    "RSC": "1",
                    "Accept": "text/x-component, */*",
                    "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                    "Referer": self.base_url + "/",
                    "User-Agent": ua,
                }, cookies=cookies, timeout=20)
                if r2.status_code == 200:
                    return r2.text
                # FlareSolverr HTML might itself contain RSC data embedded
                return html
        except Exception:
            pass

        return ""

    async def scrape(self, client: AsyncSession) -> list[dict]:
        try:
            city_str  = _city_from_location(self.location).strip().lower()
            loc_lower = self.location.strip().lower()
            city_ar   = self._CITIES.get(city_str, self._CITIES.get(loc_lower, "الرياض"))
            type_slug = self._SLUGS.get((self.property_type, self.listing_type))
            if not type_slug:
                print(f"[Aqar] no slug for ({self.property_type}, {self.listing_type}) — skipping")
                return []
            url = f"{self.base_url}/{quote(type_slug, safe='')}/{quote(city_ar, safe='')}"

            text = await self._fetch_rsc(client, url)
            if not text:
                return []

            raw = self._extract_listings(text)
            results = []
            for item in raw:
                price = _int(item.get("price") or 0)
                title = _str(item.get("title"), "")
                if not title:
                    continue

                rpt = _str(item.get("rent_period_text"), "")
                rent_period = self._RENT_PERIOD.get(rpt, "")

                address_text = _str(item.get("address_text") or item.get("district"), "")
                city_name    = _str(item.get("city"), "")
                ld = address_text or city_name or self.location.title()

                geo = item.get("location") or {}
                lat = float(geo.get("lat") or 0) or _get_coords(self.location)[0]
                lng = float(geo.get("lng") or 0) or _get_coords(self.location)[1]

                main_img = item.get("mainImage") or (item.get("imgs") or [None])[0]
                image_url = f"https://images.aqar.fm/webp/750x0/props/{main_img}" if main_img else ""

                path = _str(item.get("path"), "")
                source_url = f"{self.base_url}{path}" if path else f"{self.base_url}/عقارات"

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
# 3. PropertyFinder SA
# ─────────────────────────────────────────────────────────────────────────────

class PropertyFinderScraper(BaseScraper):
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
        "hail":      "ar-riyadh",
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

                price_obj  = p.get("price") or {}
                price      = _int(price_obj.get("value") or 0)
                period_raw = _str(price_obj.get("period"), "").lower()
                rent_period = "/year" if "year" in period_raw else "/month" if "month" in period_raw else ""
                if purpose == "buy":
                    rent_period = ""

                loc_obj = p.get("location") or {}
                ld      = _str(loc_obj.get("full_name"), self.location.title())
                coords  = loc_obj.get("coordinates") or {}
                lat     = float(coords.get("lat") or 0) or _get_coords(self.location)[0]
                lng     = float(coords.get("lon") or coords.get("lng") or 0) or _get_coords(self.location)[1]

                imgs      = p.get("images") or []
                image_url = (imgs[0].get("medium") or imgs[0].get("small") or "") if imgs else ""

                beds  = _str(p.get("bedrooms"),  "N/A")
                baths = _str(p.get("bathrooms"), "N/A")
                sz    = p.get("size") or {}
                area  = _int(sz.get("value") or 0)

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

                broker_name   = _str(agent_obj.get("name") or broker_obj.get("name"), "")
                broker_agency = _str(broker_obj.get("name") or
                                    (broker_obj.get("agency") or {}).get("name"), "")
                broker_photo  = _str(agent_obj.get("image"), "")
                broker_url = ""

                source_url = _str(p.get("share_url"), url)
                if not source_url.startswith("http"):
                    source_url = f"{self.base_url}{source_url}"

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
            city_str  = _city_from_location(self.location).strip().lower()
            loc_lower = self.location.strip().lower()
            city_slug = self._CITIES.get(city_str, self._CITIES.get(loc_lower, city_str.replace(" ", "-")))
            type_slug = self._TYPES.get(self.property_type, "properties")
            purpose   = "sale" if self.listing_type == "sale" else "rent"
            url = f"{self.base_url}/en/{type_slug}-for-{purpose}-in-{city_slug}"

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
                pi    = p.get("propertyInfo") or {}
                loc   = p.get("location")     or {}
                _own  = p.get("propertyOwner") or {}
                owner = _own if isinstance(_own, dict) else (_own[0] if isinstance(_own, list) and _own else {})
                files = p.get("propertyFiles") or {}
                attrs = {a["key"]: a["value"]
                         for a in (p.get("attributes") or [])
                         if isinstance(a, dict) and "key" in a}

                title = _str(pi.get("title"), "")
                if not title:
                    continue

                if purpose == "rent":
                    price = _int(pi.get("expectedRent") or pi.get("conversionPrice") or 0)
                else:
                    price = _int(pi.get("salePrice") or pi.get("conversionPrice") or 0)

                freq_raw = _str(pi.get("expectedRentType"), "").lower()
                rent_period = "/year" if "year" in freq_raw else "/month" if "month" in freq_raw else ""
                if purpose == "sale":
                    rent_period = ""

                ld = _str(pi.get("address") or pi.get("district") or pi.get("zone"), self.location.title())

                lat = float(loc.get("lat") or 0) or _get_coords(self.location)[0]
                lng = float(loc.get("lon") or loc.get("lng") or 0) or _get_coords(self.location)[1]

                prop_id = p.get("id", "")
                imgs = files.get("images") if isinstance(files, dict) else []
                image_url = f"{self._IMG_CDN}/{prop_id}/images/{imgs[0]}/public" if imgs else ""

                phone = _clean_phone(owner.get("phone") or owner.get("whatsApp") or
                                     (p.get("contactDetails") or {}).get("phoneNumber"))

                broker_name   = _str(owner.get("enName") or owner.get("name") or owner.get("fullName"), "")
                broker_agency = _str(owner.get("companyName") or
                                    (owner.get("company") or {}).get("name") or
                                     owner.get("agencyName"), "")
                raw_avatar    = owner.get("userAvatar") or owner.get("companyLogo") or ""
                broker_photo  = (f"https://images.wasalt.sa/{raw_avatar}"
                                 if raw_avatar and not raw_avatar.startswith("http") and "null" not in raw_avatar
                                 else _str(raw_avatar if raw_avatar and "null" not in str(raw_avatar) else "", ""))
                owner_slug    = _str(owner.get("slug"), "")
                broker_url    = (f"https://wasalt.sa/en/agents/{owner_slug}" if owner_slug else "")

                slug = _str(pi.get("slug"), "")
                source_url = (f"{self.base_url}/en/property/{slug}" if slug
                              else f"{self.base_url}/en/property/{prop_id}" if prop_id
                              else url)

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
# 5. Sakani
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
# 6. Haraj
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
            prop_slug = {"apartment":"apartments-for-sale","villa":"villas",
                         "house":"houses","land":"land","office":"offices"}.get(
                             self.property_type,"real-estate")
            city_slug = _city_from_location(self.location).strip().lower().replace(' ','-')
            url = f"{self.base_url}/en/{prop_slug}/{city_slug}"
            r = await client.get(url, headers=_h(self.base_url), timeout=15)
            if r.status_code==200:
                soup = BeautifulSoup(r.text,"lxml")
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
# 10. Satel
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
# 13. SaudiDeal
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
# API routes
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/platforms")
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


@router.get("/api/stream")
async def stream(
    location:      str            = Query(...),
    min_price:     Optional[int]  = Query(None),
    max_price:     Optional[int]  = Query(None),
    rooms:         Optional[int]  = Query(None),
    property_type: str            = Query("apartment"),
    listing_type:  str            = Query("sale"),
    platforms:     Optional[str]  = Query(None),
):
    property_types = [t.strip() for t in property_type.split(",") if t.strip()] or ["apartment"]

    PRICE_BUFFER = 0.05
    buf_min = int(min_price * (1 - PRICE_BUFFER)) if min_price else None
    buf_max = int(max_price * (1 + PRICE_BUFFER)) if max_price else None

    platform_list = [p.strip() for p in platforms.split(",")] if platforms else None

    scrapers: list[BaseScraper] = []
    seen_classes: set = set()
    for pt in property_types:
        kw = dict(location=location, min_price=buf_min, max_price=buf_max,
                  rooms=rooms, property_type=pt, listing_type=listing_type)
        for sc in _build_scrapers(platform_list, kw):
            key = (type(sc).__name__, pt)
            if key not in seen_classes:
                seen_classes.add(key)
                scrapers.append(sc)

    DISTRICT_RADIUS_KM = 10.0
    is_district = "," in location
    centroid: list[float] = []

    def _in_district(item: dict) -> bool:
        if not centroid:
            return True
        lat, lng = item.get("lat"), item.get("lng")
        if not lat or not lng:
            return False
        return _haversine_km(centroid[0], centroid[1], lat, lng) <= DISTRICT_RADIUS_KM

    async def gen() -> AsyncIterator[str]:
        seen_urls: set[str] = set()

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


@router.get("/api/properties")
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


@router.get("/api/cities")
def cities():
    from shared import CITY_COORDS
    return sorted(CITY_COORDS.keys())


@router.get("/health")
def health():
    return {"status":"ok","platforms":len(ALL_SCRAPERS)}
