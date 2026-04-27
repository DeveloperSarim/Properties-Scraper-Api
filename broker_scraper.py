"""
Broker / agent scraper and /api/brokers SSE endpoint.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import AsyncIterator, Optional
from urllib.parse import quote

from curl_cffi.requests import AsyncSession
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from shared import (
    BAYUT_ALGOLIA_APP_ID, BAYUT_ALGOLIA_API_KEY, BAYUT_ALGOLIA_URL,
    _h, _city_from_location,
    _int, _str, _clean_phone, _sse,
)
from property_scraper import (
    BayutScraper, PropertyFinderScraper, WasaltScraper, AqarScraper,
)

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# City slug maps
# ─────────────────────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────────────────────
# Bayut agencies page parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_bayut_agencies_page(text: str) -> tuple[list[dict], int]:
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
                ph_obj = a.get("phoneNumber") or {}
                if isinstance(ph_obj, str):
                    raw_phone = ph_obj
                elif isinstance(ph_obj, dict):
                    raw_phone = (ph_obj.get("phone") or ph_obj.get("mobile") or
                                 (ph_obj.get("mobileNumbers") or [""])[0] or
                                 ph_obj.get("whatsApp") or "")
                else:
                    raw_phone = ""
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

# ─────────────────────────────────────────────────────────────────────────────
# BrokerMerger
# ─────────────────────────────────────────────────────────────────────────────

class BrokerMerger:
    def __init__(self):
        self._map: dict[str, dict] = {}

    def upsert(self, phone: str, data: dict) -> bool:
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

    def override_listing_count(self, phone: str, count: int, **fields) -> None:
        """Set listing_count from an authoritative source (companies directory)."""
        if phone not in self._map:
            self._map[phone] = {
                "name": fields.get("name", ""), "agency": fields.get("agency", ""),
                "photo_url": fields.get("photo_url", ""), "phone": phone,
                "platforms": ["Bayut"], "listing_count": count,
                "areas": fields.get("areas", []), "profile_url": fields.get("profile_url", ""),
            }
        else:
            b = self._map[phone]
            b["listing_count"] = count
            for f in ("name", "agency", "photo_url", "profile_url"):
                if fields.get(f) and not b[f]:
                    b[f] = fields[f]
            for p in ["Bayut"]:
                if p not in b["platforms"]:
                    b["platforms"].append(p)

    def __len__(self) -> int:
        return len(self._map)

# ─────────────────────────────────────────────────────────────────────────────
# Source 1: Bayut Algolia listing index → broker extraction
# ─────────────────────────────────────────────────────────────────────────────

async def _bayut_brokers_algolia(client: AsyncSession, city_str: str) -> list[dict]:
    # city_str="" means "all Saudi Arabia" — no city filter applied
    city_slug = BayutScraper._CITY_SLUGS.get(city_str, f"/{city_str.replace(' ', '-')}") if city_str else None

    CAT_SLUGS = ["apartments", "villas", "townhouses", "offices",
                 "residential-lands", "showrooms",
                 "residential-buildings", "compounds", "warehouses"]
    PURPOSES  = ["for-sale", "for-rent"]

    async def _page(cat: str, purpose: str, page: int) -> list[dict]:
        try:
            payload: dict = {
                "query": "",
                "filters": f"purpose:{purpose} AND category.slug_l1:{cat}",
                "hitsPerPage": 50,
                "page": page,
                "attributesToRetrieve": [
                    "phoneNumber", "agent", "agency", "externalID", "location",
                ],
            }
            if city_slug:
                payload["facetFilters"] = [[f"location.slug_l1:{city_slug}"]]
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
                timeout=12,
            )
            return r.json().get("hits", []) if r.status_code == 200 else []
        except:
            return []

    first_pages = await asyncio.gather(
        *[_page(cat, p, 0) for cat in CAT_SLUGS for p in PURPOSES],
        return_exceptions=True,
    )
    extra_pages = await asyncio.gather(
        *[_page(cat, p, 1) for cat in CAT_SLUGS for p in PURPOSES],
        return_exceptions=True,
    )

    all_hits = []
    for bucket in [*first_pages, *extra_pages]:
        if isinstance(bucket, list):
            all_hits.extend(bucket)

    brokers: dict[str, dict] = {}
    seen_listing_ids: set[str] = set()
    for h in all_hits:
        ph       = h.get("phoneNumber") or {}
        raw      = (ph.get("mobile") or (ph.get("mobileNumbers") or [""])[0] or
                    ph.get("phone") or "")
        phone    = _clean_phone(_str(raw, ""))
        if not phone:
            continue

        listing_id = _str(h.get("externalID") or "", "")
        is_dup     = bool(listing_id and listing_id in seen_listing_ids)
        if listing_id:
            seen_listing_ids.add(listing_id)

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
                "_agency_slug":  slug if is_agency else "",
            }
        b = brokers[phone]
        if not is_dup:
            b["listing_count"] += 1
        if area and area not in b["areas"]: b["areas"].append(area)
        if not b["name"]   and name:        b["name"]   = name
        if not b["agency"] and agency_name: b["agency"] = agency_name

    result = [{k: v for k, v in b.items() if k != "_agency_slug"} for b in brokers.values()]
    print(f"[BayutBrokersAlgolia] {len(result)} unique brokers for '{city_str}'")
    return result

# ─────────────────────────────────────────────────────────────────────────────
# Source 2: Bayut HTML agents directory
# ─────────────────────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────────────────────
# Source 2b: Bayut HTML companies directory (authoritative listing counts)
# ─────────────────────────────────────────────────────────────────────────────

async def _bayut_companies_directory(client: AsyncSession, city_str: str) -> list[dict]:
    """
    Fetch Bayut /en/companies/{city}/ HTML pages.
    Uses stats.adsCount from window.state — the real listing count, not a sample.
    """
    city_slug = _BAYUT_AGENT_SLUGS.get(city_str, city_str.replace(" ", "-")) if city_str else "riyadh"
    base_url  = f"https://www.bayut.sa/en/companies/{city_slug}/"
    headers   = {
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.bayut.sa/",
    }
    try:
        r0 = await client.get(base_url, headers=headers, timeout=20)
        if r0.status_code != 200:
            print(f"[BayutCompaniesDir] HTTP {r0.status_code} for {base_url}")
            return []
        agencies, nb_pages = _parse_bayut_agencies_page(r0.text)

        # Fetch all remaining pages in parallel
        extra = list(range(1, nb_pages))
        if extra:
            resps = await asyncio.gather(
                *[client.get(f"{base_url}?page={p+1}", headers=headers, timeout=18)
                  for p in extra],
                return_exceptions=True,
            )
            for resp in resps:
                if isinstance(resp, Exception) or resp.status_code != 200:
                    continue
                more, _ = _parse_bayut_agencies_page(resp.text)
                agencies.extend(more)

        print(f"[BayutCompaniesDir] {len(agencies)} agencies for '{city_str}' ({nb_pages} pages)")
        return agencies
    except Exception as e:
        print(f"[BayutCompaniesDir] error: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# Source 3: PropertyFinder find-broker directory
# ─────────────────────────────────────────────────────────────────────────────

async def _pf_agents(client: AsyncSession, city_str: str) -> list[dict]:
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
            raw = (pp.get("brokers", {}).get("data") or
                   (pp.get("brokers") if isinstance(pp.get("brokers"), list) else None) or
                   pp.get("agents", {}).get("data") or
                   (pp.get("agents") if isinstance(pp.get("agents"), list) else None) or
                   pp.get("initialData", {}).get("brokers", {}).get("data") or
                   pp.get("searchResult", {}).get("brokers") or
                   [])
            if not raw and page == 1:
                print(f"[PFAgents] pageProps keys: {list(pp.keys())}")
            elif raw and page == 1:
                print(f"[PFAgents] {len(raw)} brokers, sample keys: {list(raw[0].keys())[:10]}")
            out  = []
            for b in raw:
                phone = _clean_phone(_str(b.get("phone"), ""))
                if not phone:
                    continue
                url_slug  = _str(b.get("urlSlug") or b.get("slug"), "")
                client_id = _str(b.get("clientId") or b.get("id"), "")
                if url_slug and client_id and not url_slug.endswith(str(client_id)):
                    profile_url = f"https://www.propertyfinder.sa/en/broker/{url_slug}-{client_id}"
                elif url_slug:
                    profile_url = f"https://www.propertyfinder.sa/en/broker/{url_slug}"
                else:
                    profile_url = ""
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
            return out, data.get("props", {}).get("pageProps", {}).get("searchResult", {}).get("meta", {}).get("lastPage", 5)
        except Exception:
            return [], 1

    # Fetch page 1 first to get pagination info
    first_page_res, total_pages = await _fetch(1)
    results = list(first_page_res)
    seen = {b["phone"] for b in results if b.get("phone")}
    
    if total_pages > 1:
        pages = await asyncio.gather(*[_fetch(p) for p in range(2, total_pages + 1)], return_exceptions=True)
        for page_tuple in pages:
            if not isinstance(page_tuple, tuple):
                continue
            page_list, _ = page_tuple
            for b in page_list:
                if b["phone"] not in seen:
                    seen.add(b["phone"])
                    results.append(b)
    print(f"[PFAgents] {len(results)} for '{city_label}'")
    return results

# ─────────────────────────────────────────────────────────────────────────────
# Source 4: Wasalt agents directory
# ─────────────────────────────────────────────────────────────────────────────

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
                    (pp.get("agents") if isinstance(pp.get("agents"), list) else None) or
                    pp.get("agentsList") or
                    pp.get("brokers", {}).get("data") or
                    pp.get("initialData", {}).get("agents", {}).get("data") or
                    [])
            print(f"[WasaltAgents] URL={resp.url} raw={len(raw)} ppKeys={list(pp.keys())[:8]}")
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

# ─────────────────────────────────────────────────────────────────────────────
# Source 5: Aqar broker / office directory
# ─────────────────────────────────────────────────────────────────────────────

async def _aqar_fetch(client: AsyncSession, url: str) -> str:
    """Fetch Aqar RSC page directly or via FlareSolverr fallback."""
    try:
        r = await client.get(url, headers={
            "RSC": "1", "Accept": "text/x-component, */*",
            "Accept-Language": "ar-SA,ar;q=0.9", "Referer": "https://sa.aqar.fm/",
        }, timeout=15)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
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
            cookies = {c["name"]: c["value"] for c in sol.get("cookies", [])}
            ua = sol.get("userAgent", "")
            r2 = await client.get(url, headers={
                "RSC": "1", "Accept": "text/x-component, */*",
                "Accept-Language": "ar-SA,ar;q=0.9", "Referer": "https://sa.aqar.fm/",
                "User-Agent": ua,
            }, cookies=cookies, timeout=15)
            if r2.status_code == 200:
                return r2.text
            return sol.get("response", "")
    except Exception:
        pass
    return ""


async def _aqar_brokers(client: AsyncSession, city_str: str) -> list[dict]:
    city_ar = _AQAR_BROKER_CITIES.get(city_str, city_str)
    urls_to_try = [
        f"https://sa.aqar.fm/brokers/{quote(city_ar, safe='')}",
        f"https://sa.aqar.fm/مكاتب-عقارية/{quote(city_ar, safe='')}",
        f"https://sa.aqar.fm/وسطاء/{quote(city_ar, safe='')}",
    ]
    results = []
    try:
        for url in urls_to_try:
            text = await _aqar_fetch(client, url)
            if not text:
                continue
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

# ─────────────────────────────────────────────────────────────────────────────
# Source 6: Broker contacts extracted from listing scrapers
# ─────────────────────────────────────────────────────────────────────────────

async def _brokers_from_listings(client: AsyncSession, location: str) -> list[dict]:
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

# ─────────────────────────────────────────────────────────────────────────────
# Source 7: Bayut Algolia — district-level deep scan
# ─────────────────────────────────────────────────────────────────────────────

async def _bayut_district_brokers(client: AsyncSession, city_str: str, target_district: str = "") -> list[dict]:
    districts = [target_district] if target_district else _DISTRICT_QUERIES.get(city_str, [])
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
            if area and area not in b["areas"]: b["areas"].append(area)

    result = list(brokers.values())
    print(f"[BayutDistrictBrokers] {len(result)} for '{city_str}' ({len(districts)} districts)")
    return result

# ─────────────────────────────────────────────────────────────────────────────
# Source 8: Haraj phone extraction
# ─────────────────────────────────────────────────────────────────────────────

async def _haraj_brokers(client: AsyncSession, city_str: str) -> list[dict]:
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
# /api/brokers SSE endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/brokers")
async def brokers_stream(
    location:  str           = Query(...),
    platforms: Optional[str] = Query(None),
):
    raw_city = _city_from_location(location).strip().lower()
    city_str = "" if raw_city in ("saudi arabia", "ksa", "") else raw_city
    search_location = location if city_str else "Riyadh"
    search_city     = city_str if city_str else "riyadh"
    search_district = location.split(",")[0].strip() if "," in location else ""

    async def gen() -> AsyncIterator[str]:
        merger = BrokerMerger()

        def _ingest(broker_list: list[dict]) -> int:
            new = 0
            for b in broker_list:
                phone = b.get("phone", "")
                name  = (b.get("name") or b.get("agency") or "").strip()
                if phone and name and merger.upsert(phone, b):
                    new += 1
            return new

        streamed: set[str] = set()

        def _stream_new(snap: list[dict]):
            for b in snap:
                if b["phone"] not in streamed:
                    streamed.add(b["phone"])
                    yield _sse({"status": "broker", "broker": b})

        async with AsyncSession(impersonate="chrome124") as client:

            # Phase 1: Bayut — Algolia + agents dir + companies dir (parallel)
            if not search_district:
                yield _sse({"status": "scanning", "platform": "Bayut",
                            "message": "Scanning Bayut listings, agents & companies…"})
                bayut_results = await asyncio.gather(
                    _bayut_brokers_algolia(client, city_str),        # city_str="" = all KSA
                    _bayut_agents_directory(client, search_city),
                    _bayut_companies_directory(client, search_city), # authoritative counts
                    return_exceptions=True,
                )
                bayut_count = sum(
                    _ingest(r) for r in bayut_results[:2] if isinstance(r, list)
                )
                if isinstance(bayut_results[2], list):
                    for agency in bayut_results[2]:
                        phone = agency.get("phone", "")
                        if phone:
                            merger.override_listing_count(
                                phone,
                                agency["listing_count"],
                                name=agency.get("name", ""),
                                agency=agency.get("agency", ""),
                                photo_url=agency.get("photo_url", ""),
                                profile_url=agency.get("profile_url", ""),
                                areas=agency.get("areas", []),
                            )
                for _ev in _stream_new(merger.snapshot()):
                    yield _ev
                yield _sse({"status": "platform_done", "platform": "Bayut",
                            "count": len(merger)})

            # Phase 2: Bayut district deep-scan (Targeted district or top districts)
            if search_district or search_city in _DISTRICT_QUERIES:
                msg_target = search_district if search_district else f"{search_city.title()} districts"
                yield _sse({"status": "scanning", "platform": "Bayut Districts",
                            "message": f"Deep scanning {msg_target}…"})
                district_brokers = await _bayut_district_brokers(client, search_city, search_district)
                _ingest(district_brokers)
                for _ev in _stream_new(merger.snapshot()):
                    yield _ev
                yield _sse({"status": "platform_done", "platform": "Bayut Districts",
                            "count": len(streamed)})

            # Phase 3: PropertyFinder + Wasalt + Aqar directories (parallel)
            if not search_district:
                yield _sse({"status": "scanning", "platform": "PropertyFinder",
                            "message": "Scanning PropertyFinder & Wasalt broker directories…"})
                dir_results = await asyncio.gather(
                    _pf_agents(client, search_city),
                    _wasalt_agents(client, search_city),
                    _aqar_brokers(client, search_city),
                    return_exceptions=True,
                )
                dir_labels = ["PropertyFinder", "Wasalt", "Aqar"]
                for label, res in zip(dir_labels, dir_results):
                    if isinstance(res, Exception):
                        continue
                    if not isinstance(res, list):
                        continue
                    new_n = _ingest(res)
                    yield _sse({"status": "platform_done", "platform": label, "count": new_n})
                for _ev in _stream_new(merger.snapshot()):
                    yield _ev

            # Phase 4: Listing-based extraction (PF + Wasalt + Aqar in parallel)
            yield _sse({"status": "scanning", "platform": "Listings",
                        "message": "Extracting broker contacts from live listings…"})
            listing_brokers = await _brokers_from_listings(client, search_location)
            new_listing = _ingest(listing_brokers)
            for _ev in _stream_new(merger.snapshot()):
                yield _ev
            yield _sse({"status": "platform_done", "platform": "Listings",
                        "count": new_listing})

            # Phase 5: Haraj phone extraction
            if not search_district:
                yield _sse({"status": "scanning", "platform": "Haraj",
                            "message": "Extracting contacts from Haraj listings…"})
                haraj_brokers = await _haraj_brokers(client, search_city)
                new_haraj = _ingest(haraj_brokers)
                for _ev in _stream_new(merger.snapshot()):
                    yield _ev
                yield _sse({"status": "platform_done", "platform": "Haraj",
                            "count": new_haraj})

        yield _sse({"status": "complete"})

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
