from flask import Flask, request, jsonify, Response
import os, re, json, io, time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ---------- Helper functions & DCAT converter ----------

_EXT_MEDIA = {
    "zip": "application/zip",
    "csv": "text/csv",
    "json": "application/json",
    "geojson": "application/geo+json",
    "xml": "application/xml",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "parquet": "application/octet-stream",
    "html": "text/html",
    "htm": "text/html",
    "txt": "text/plain",
    "tgz": "application/gzip",
    "gz": "application/gzip",
    "tar": "application/x-tar"
}
_STOPWORDS = set(["of","the","and","in","on","for","a","an","with","by","to","from","dataset","data","2009"])

def _guess_media_type_from_url(u):
    if not u:
        return "text/html"
    m = re.search(r"\.([a-zA-Z0-9]+)(?:$|\?)", u)
    if m:
        ext = m.group(1).lower()
        return _EXT_MEDIA.get(ext, "application/octet-stream")
    return "text/html"

def _uppercase_ext_from_url(u):
    m = re.search(r"\.([a-zA-Z0-9]+)(?:$|\?)", u)
    return m.group(1).upper() if m else "HTML"

def _extract_year_from_string(s):
    if not s:
        return None
    m = re.search(r"(20\d{2})", s)
    if m:
        return m.group(1)
    m2 = re.search(r"\b(\d{2})\b", s)
    if m2:
        yy = int(m2.group(1))
        return f"20{yy:02d}" if yy <= 25 else f"19{yy:02d}"
    return None

def _parse_date_from_metadata(dt_string):
    if not dt_string:
        return None
    s = dt_string.strip()
    patterns = [
        "%d/%m/%Y %H:%M:%S","%d/%m/%y %H:%M:%S",
        "%d/%m/%Y","%d/%m/%y",
        "%Y-%m-%d","%Y/%m/%d",
        "%d-%m-%Y","%d-%m-%y",
        "%Y"
    ]
    for fmt in patterns:
        try:
            parsed = datetime.strptime(s, fmt)
            return parsed.strftime("%Y-%m-%d") if "%d" in fmt else parsed.strftime("%Y")
        except Exception:
            pass
    return _extract_year_from_string(s)

def _make_keywords(title, summary, metadata_list):
    toks = []
    if title:
        toks += re.findall(r"[A-Za-z0-9]+", title.lower())
    if summary:
        toks += re.findall(r"[A-Za-z0-9]+", summary.lower())
    if metadata_list:
        md = metadata_list[0]
        for key in ("Sector","Geographical coverage","Dataset type"):
            if key in md and md[key]:
                toks += re.findall(r"[A-Za-z0-9]+", str(md[key]).lower())
    kws = []
    for t in toks:
        if t in _STOPWORDS or len(t) <= 2:
            continue
        if t.isdigit() and len(t) != 4:
            continue
        if t not in kws:
            kws.append(t)
    return [w.replace("_"," ").strip() for w in kws][:25]

# ---- distributions extraction helpers ----
def _extract_distributions_from_soup(soup, base_url, exts=None):
    if soup is None:
        return []
    if exts is None:
        exts = ["csv","zip","json","geojson","xlsx","xls","parquet","xml","tgz","tar","gz"]
    ext_pattern = re.compile(r"\.(" + "|".join([re.escape(e) for e in exts]) + r")(?:$|\?|#)", re.I)
    found = {}
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        m = ext_pattern.search(full)
        if m:
            ext = m.group(1).lower()
            found[full] = {
                "dcat:accessURL": full,
                "dcat:mediaType": _EXT_MEDIA.get(ext, _guess_media_type_from_url(full)),
                "dct:format": ext.upper(),
                **({"dct:title": a.get_text(strip=True)} if a.get_text(strip=True) else {})
            }

    # data-* attributes and scripts
    for tag in soup.find_all(True):
        for attr in ("data-download","data-href","data-url","data-resource","data-link"):
            if tag.has_attr(attr):
                href = tag[attr]
                if href:
                    full = urljoin(base_url, href)
                    m = ext_pattern.search(full)
                    if m:
                        ext = m.group(1).lower()
                        found[full] = {
                            "dcat:accessURL": full,
                            "dcat:mediaType": _EXT_MEDIA.get(ext, _guess_media_type_from_url(full)),
                            "dct:format": ext.upper()
                        }

    for script in soup.find_all("script"):
        text = script.string or ""
        for m in ext_pattern.finditer(text):
            start = max(0, m.start()-100)
            snippet = text[start:m.end()+100]
            url_like = re.search(r"https?://[^\s'\"<>]+", snippet)
            if url_like:
                full = url_like.group(0)
                found.setdefault(full, {
                    "dcat:accessURL": full,
                    "dcat:mediaType": _guess_media_type_from_url(full),
                    "dct:format": _uppercase_ext_from_url(full)
                })

    if not found:
        found[base_url] = {
            "dcat:accessURL": base_url,
            "dcat:mediaType":"text/html",
            "dct:format":"HTML"
        }
    return [found[k] for k in sorted(found.keys())]

def _extract_aikosh_specific(soup, base_url):
    dists = []
    if soup is None:
        return dists
    for section in soup.find_all(["section","div"], class_=re.compile(r"(resource|download|files|dataset)", re.I)):
        for a in section.find_all("a", href=True):
            full = urljoin(base_url, a["href"].strip())
            dists.append({
                "dcat:accessURL": full,
                "dcat:mediaType": _guess_media_type_from_url(full),
                "dct:format": _uppercase_ext_from_url(full),
                **({"dct:title": a.get_text(strip=True)} if a.get_text(strip=True) else {})
            })
    for el in soup.find_all(True, class_=re.compile(r"dataset-metadata|download|resource", re.I)):
        for attr in ("href","data-download","data-url","data-href"):
            if el.has_attr(attr):
                full = urljoin(base_url, el[attr])
                dists.append({
                    "dcat:accessURL": full,
                    "dcat:mediaType": _guess_media_type_from_url(full),
                    "dct:format": _uppercase_ext_from_url(full)
                })
    seen = set()
    out = []
    for d in dists:
        u = d.get("dcat:accessURL")
        if u and u not in seen:
            seen.add(u)
            out.append(d)
    return out

# stronger kaggle explorer
def _explore_kaggle_for_files(soup, base_url):
    urls = set()
    if soup is None:
        return []
    for script in soup.find_all("script"):
        text = script.string or script.get_text(" ") or ""
        for m in re.finditer(r"https?://[^\s'\"<>]+\.(csv|zip|json|xlsx|parquet|geojson)(?:\?[^'\"\\s<>]*)?", text, re.I):
            urls.add(m.group(0))
        for key in ("downloadUrl","fileUrl","url","path","rawUrl","archiveUrl"):
            for m in re.finditer(r'"' + re.escape(key) + r'"\s*:\s*"([^"]+)"', text, re.I):
                candidate = m.group(1)
                if candidate and candidate.startswith("http") and re.search(r"\.(csv|zip|json|xlsx|parquet|geojson)(?:\?|$)", candidate, re.I):
                    urls.add(candidate)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        if (re.search(r"(kaggleusercontent|storage.googleapis|s3.amazonaws|azureedge)" , full, re.I) and
                re.search(r"\.(csv|zip|json|xlsx|parquet|geojson)(?:\?|$)", full, re.I)):
            urls.add(full)
    for tag in soup.find_all("script"):
        if tag.get("type") and "application/json" in tag.get("type"):
            text = tag.string or ""
            for m in re.finditer(r"https?://[^\s'\"<>]+\.(csv|zip|json|xlsx|parquet|geojson)(?:\?[^'\"\\s<>]*)?", text, re.I):
                urls.add(m.group(0))
    return [urljoin(base_url, u) for u in sorted(urls)]

def _extract_kaggle_specific(soup, base_url):
    dists = []
    if soup is None:
        return dists
    for div in soup.find_all(True, class_=re.compile(r"(file|resource|download|data-file|dataset-file|files)", re.I)):
        for a in div.find_all("a", href=True):
            full = urljoin(base_url, a["href"].strip())
            dists.append({
                "dcat:accessURL": full,
                "dcat:mediaType": _guess_media_type_from_url(full),
                "dct:format": _uppercase_ext_from_url(full),
                **({"dct:title": a.get_text(strip=True)} if a.get_text(strip=True) else {})
            })
    discovered = _explore_kaggle_for_files(soup, base_url)
    for u in discovered:
        dists.append({
            "dcat:accessURL": u,
            "dcat:mediaType": _guess_media_type_from_url(u),
            "dct:format": _uppercase_ext_from_url(u)
        })
    for script in soup.find_all("script"):
        txt = script.string or ""
        if txt and "kaggle" in txt.lower():
            for m in re.finditer(r"https?://[^\s'\"<>]+\.(csv|zip|json|xlsx|parquet|geojson)", txt, re.I):
                u = m.group(0)
                dists.append({
                    "dcat:accessURL": u,
                    "dcat:mediaType": _guess_media_type_from_url(u),
                    "dct:format": _uppercase_ext_from_url(u)
                })
    seen = set()
    out = []
    for d in dists:
        u = d.get("dcat:accessURL")
        if u and u not in seen:
            seen.add(u)
            out.append(d)
    return out

# simple Turtle serializer
def dcat_to_turtle(dcat_dict):
    if not dcat_dict or "dcat:Dataset" not in dcat_dict:
        return "# No DCAT data available"
    ds = dcat_dict["dcat:Dataset"]
    lines = []
    prefixes = {
        "dcat": "<https://www.w3.org/ns/dcat#>",
        "dct": "<http://purl.org/dc/terms/>",
        "foaf": "<http://xmlns.com/foaf/0.1/>"
    }
    for p, uri in prefixes.items():
        lines.append(f"@prefix {p}: {uri} .")
    lines.append("")
    subj = "_:dataset"
    lines.append(f"{subj}")
    if ds.get("dct:title"):
        lines.append(f"    dct:title \"{ds.get('dct:title').replace('\"','\\\\\"')}\" ;")
    if ds.get("dct:description"):
        desc = ds.get("dct:description").replace('\"','\\\\\"').replace('\n',' ')
        lines.append(f"    dct:description \"{desc}\" ;")
    pub = ds.get("dct:publisher", {}).get("foaf:name") if ds.get("dct:publisher") else None
    if pub:
        lines.append(f"    dct:publisher [ foaf:name \"{pub}\" ] ;")
    if ds.get("dct:issued"):
        lines.append(f"    dct:issued \"{ds.get('dct:issued')}\" ;")
    if ds.get("dct:modified"):
        lines.append(f"    dct:modified \"{ds.get('dct:modified')}\" ;")
    if ds.get("dct:license"):
        lines.append(f"    dct:license <{ds.get('dct:license')}> ;")
    if ds.get("dcat:keyword"):
        kws = ds.get("dcat:keyword")
        for k in kws:
            lines.append(f"    dcat:keyword \"{k}\" ;")
    dists = ds.get("dcat:distribution", [])
    if dists:
        for d in dists:
            access = d.get("dcat:accessURL")
            media = d.get("dcat:mediaType")
            fmt = d.get("dct:format")
            title = d.get("dct:title")
            lines.append(f"    dcat:distribution [")
            if title:
                t = title.replace('\"','\\\\\"')
                lines.append(f"        dct:title \"{t}\" ;")
            if access:
                lines.append(f"        dcat:accessURL <{access}> ;")
            if media:
                lines.append(f"        dcat:mediaType \"{media}\" ;")
            if fmt:
                lines.append(f"        dct:format \"{fmt}\" ")
            lines.append(f"    ] ;")
    if lines and lines[-1].endswith(' ;'):
        lines[-1] = lines[-1][:-2] + ' .'
    return "\n".join(lines)

# main DCAT converter
def convert_to_dcat_dynamic(result_json, url, soup=None, site_hint=None, output_path=None):
    if not site_hint:
        low = (url or "").lower()
        if "aikosh" in low or "indiaai" in low:
            site_hint="aikosh"
        elif "kaggle" in low:
            site_hint="kaggle"
        else:
            hostname = (urlparse(url).hostname or "").lower()
            if "kaggle" in hostname:
                site_hint="kaggle"
            elif "indiaai" in hostname or "aikosh" in hostname:
                site_hint="aikosh"
            else:
                site_hint="unknown"

    title = result_json.get("Title") or result_json.get("title") or "dataset"
    desc = (
        result_json.get("About Dataset") or
        result_json.get("Summary") or
        result_json.get("Description") or ""
    )
    metadata_list = result_json.get("DATASET METADATA", []) or []

    publisher_name = None
    if metadata_list:
        md = metadata_list[0]
        if site_hint == "aikosh":
            publisher_name = (
                md.get("Source organisation") or
                md.get("Author") or
                md.get("Uploaded by")
            )
        elif site_hint == "kaggle":
            publisher_name = (
                md.get("Author") or
                md.get("Owner") or
                md.get("Uploaded by")
            )
    if not publisher_name:
        publisher_name = result_json.get("Source") or urlparse(url).hostname or "Unknown"

    issued = None
    modified = None
    if metadata_list:
        md = metadata_list[0]

        # --- Aikosh special handling: prefer "Date & Time" (or Date) as issued ---
        if site_hint == "aikosh":
            for k in ("Date & Time", "Date", "Published", "Published on", "Uploaded on", "Updated"):
                if md.get(k):
                    maybe = _parse_date_from_metadata(str(md.get(k)))
                    if maybe:
                        # use parsed full date as issued (not modified)
                        issued = maybe if len(maybe) > 4 else f"{maybe}-01-01"
                        break

            # also allow explicit "Year" fields to populate issued if above not found
            if not issued:
                for k in ("Year range", "Year", "Issued", "Created", "Published"):
                    if md.get(k):
                        y = _extract_year_from_string(str(md.get(k)))
                        if y:
                            issued = f"{y}-01-01"
                            break

            # ensure we do not supply a modified date for Aikosh (user requested)
            modified = None

        else:
            # --- original/default handling for non-Aikosh pages ---
            for k in ("Date & Time", "Date", "Last Updated", "Last modified", "Updated", "Published", "Published on"):
                if md.get(k):
                    maybe = _parse_date_from_metadata(str(md.get(k)))
                    if maybe:
                        modified = maybe
                        break
            for k in ("Year range", "Year", "Issued", "Published", "Created"):
                if md.get(k):
                    y = _extract_year_from_string(str(md.get(k)))
                    if y:
                        issued = f"{y}-01-01"
                        break

    # Fallback: if we somehow ended with only modified for aikosh, promote it to issued
    if site_hint == "aikosh" and not issued and modified:
        issued = modified
        modified = None

    license_val = None
    if metadata_list:
        license_val = metadata_list[0].get("License") or metadata_list[0].get("license")
    if license_val and "open government license" in str(license_val).lower():
        license_val = "Open Government License, India (https://www.data.gov.in/Godl)"

    distributions = []
    if soup is not None:
        if site_hint == "aikosh":
            distributions += _extract_aikosh_specific(soup, url)
        elif site_hint == "kaggle":
            distributions += _extract_kaggle_specific(soup, url)
        distributions += _extract_distributions_from_soup(soup, url)
    else:
        distributions = [{
            "dcat:accessURL": url,
            "dcat:mediaType": _guess_media_type_from_url(url),
            "dct:format": _uppercase_ext_from_url(url)
        }]

    seen = set()
    merged = []
    for d in distributions:
        a = d.get("dcat:accessURL")
        if a and a not in seen:
            seen.add(a)
            merged.append(d)

    has_csv = any(
        (d.get("dcat:mediaType") == "text/csv" or str(d.get("dct:format")).upper() == "CSV")
        for d in merged
    )
    if site_hint == "kaggle" and not has_csv:
        for i, d in enumerate(merged):
            access = d.get("dcat:accessURL", "")
            if "kaggle.com" in access and d.get("dcat:mediaType") == "text/html":
                merged[i]["dcat:mediaType"] = "text/csv"
                merged[i]["dct:format"] = "CSV"
                break

    keywords = _make_keywords(title, result_json.get("Summary"), metadata_list)

    dcat = {
        "@context":"https://www.w3.org/ns/dcat.jsonld",
        "dcat:Dataset": {
            "dct:title": title,
            "dct:description": desc,
            "dct:publisher": {"foaf:name": publisher_name},
            **({"dct:issued": issued} if issued else {}),
            **({"dct:modified": modified} if modified else {}),
            **({"dct:license": license_val} if license_val else {}),
            **({"dcat:keyword": keywords} if keywords else {}),
            "dcat:distribution": merged
        }
    }

    if output_path is not None:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(dcat, fh, indent=2, ensure_ascii=False)

    return dcat

# --------- Scraper function (Selenium) ---------
def run_scrapper(url: str, output_json_name: str=None, max_wait=20):
    """
    Perform the Selenium scrape.
    Returns (result_json, soup, elapsed_seconds).
    Raises RuntimeError with details if scraping fails.
    """
    start_time = time.time()
    logging.info("Starting scraper for %s", url)

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    try:
        driver.get(url)
        wait = WebDriverWait(driver, max_wait)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        time.sleep(2)  # small sleep to ensure dynamic content
        soup = BeautifulSoup(driver.page_source, "html.parser")
        result_json = {}
        metadata_dict = {}

        source = "Kaggle" if "kaggle" in url.lower() else "IndiaAI"
        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else None
        result_json["Title"] = title

        info = None
        if source == "Kaggle":
            if title_tag:
                next_tag = title_tag.find_next_sibling()
                if next_tag and next_tag.name in ["p", "div", "span"]:
                    info = next_tag.get_text(strip=True)
        else:
            first_p = soup.find("p")
            info = first_p.get_text(strip=True) if first_p else None
        result_json["Summary"] = info

        about_paragraph = None
        if source == "Kaggle":
            about_header = soup.find(
                lambda tag: tag.name in ["h2", "h3"] and "About Dataset" in tag.text
            )
            if about_header:
                next_p = about_header.find_next("p")
                about_paragraph = next_p.get_text(strip=True) if next_p else None
        else:
            about_header = soup.find(
                lambda tag: tag.name in ["h3", "h2"] and "About Dataset" in tag.text
            )
            if about_header:
                next_p = about_header.find_next("p")
                about_paragraph = next_p.get_text(strip=True) if next_p else None
        result_json["About Dataset"] = about_paragraph

        # metadata (improved, extracts license when possible)
        def try_set_meta(key, value):
            if key and value:
                # normalize key and avoid overwriting existing keys with empty values
                if key not in metadata_dict or not metadata_dict.get(key):
                    metadata_dict[key] = value.strip()

        if source == "Kaggle":
            # 1) Existing simple strong/bold extraction (keep current behavior)
            for li in soup.find_all(["li", "p", "div"]):
                strong = li.find("strong") or li.find("b")
                if strong:
                    key = strong.get_text(strip=True).replace(":", "")
                    # remove the strong text from the container text to get the value
                    value = li.get_text(strip=True).replace(strong.get_text(strip=True), "").strip(" :")
                    try_set_meta(key, value)

            # 2) Look for table-like rows: label in a child and value in sibling
            for row in soup.find_all(class_=re.compile(r"(metadata|info|meta|dataset-)?row", re.I)):
                # attempt common patterns
                cols = row.find_all(["div", "span", "p", "td"])
                if len(cols) >= 2:
                    k = cols[0].get_text(strip=True)
                    v = cols[1].get_text(strip=True)
                    try_set_meta(k.replace(":", ""), v)

            # 3) Parse JSON-LD scripts (application/ld+json) — Kaggle often exposes license here
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    txt = script.string or script.get_text(" ")
                    obj = json.loads(txt)
                    # If it's a dict with license or nested dataset object
                    def extract_from_ld(o):
                        if isinstance(o, dict):
                            if "license" in o and o.get("license"):
                                return o.get("license")
                            # sometimes license is an object with '@id' or 'name'
                            if isinstance(o.get("license"), dict):
                                l = o["license"].get("name") or o["license"].get("@id") or o["license"].get("url")
                                if l:
                                    return l
                            # check nested fields
                            for v in o.values():
                                found = extract_from_ld(v)
                                if found:
                                    return found
                        elif isinstance(o, list):
                            for item in o:
                                found = extract_from_ld(item)
                                if found:
                                    return found
                        return None

                    lic = extract_from_ld(obj)
                    if lic:
                        try_set_meta("License", lic if isinstance(lic, str) else str(lic))
                except Exception:
                    # ignore JSON parse errors
                    pass

            # 4) Look for anchors or text that refer to common license hosts (creativecommons, gnu, mit, apache)
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if re.search(r"(creativecommons|creativecommons.org|opensource.org|apache.org|gnu.org|mit-license|mit\.txt|license)", href, re.I):
                    # prefer anchor text if it looks like a license name, else use href
                    text = a.get_text(strip=True)
                    try_set_meta("License", text or href)

            # 5) Look for any element whose label contains 'license' (case insensitive)
            for el in soup.find_all(text=re.compile(r"\blicense\b", re.I)):
                parent = el.parent
                text = parent.get_text(" ", strip=True)
                # try to split "License: MIT" style
                m = re.search(r"license[:\s\-]*([\w\W]{1,200})", text, re.I)
                if m:
                    try_set_meta("License", m.group(1).strip())
                else:
                    # fallback: nearby sibling
                    sib = parent.find_next_sibling()
                    if sib:
                        try_set_meta("License", sib.get_text(strip=True))

        else:
            # Existing scraping for non-Kaggle (IndiaAI/local HTML)
            metadata_section = soup.find("div", class_="dataset-metadata")
            if metadata_section:
                for block in metadata_section.find_all("div", class_="text-xs"):
                    label_tag = block.find("label")
                    value_tag = block.find("p")
                    if label_tag and value_tag:
                        key = label_tag.get_text(strip=True)
                        value = value_tag.get_text(strip=True)
                        try_set_meta(key, value)

            # Same JSON-LD and license anchor checks for other sites too
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    txt = script.string or script.get_text(" ")
                    obj = json.loads(txt)
                    def extract_from_ld(o):
                        if isinstance(o, dict):
                            if "license" in o and o.get("license"):
                                return o.get("license")
                            if isinstance(o.get("license"), dict):
                                l = o["license"].get("name") or o["license"].get("@id") or o["license"].get("url")
                                if l:
                                    return l
                            for v in o.values():
                                found = extract_from_ld(v)
                                if found:
                                    return found
                        elif isinstance(o, list):
                            for item in o:
                                found = extract_from_ld(item)
                                if found:
                                    return found
                        return None
                    lic = extract_from_ld(obj)
                    if lic:
                        try_set_meta("License", lic if isinstance(lic, str) else str(lic))
                except Exception:
                    pass

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if re.search(r"(creativecommons|opensource.org|apache.org|gnu.org|mit-license|license)", href, re.I):
                    text = a.get_text(strip=True)
                    try_set_meta("License", text or href)


        result_json["DATASET METADATA"] = [metadata_dict]
        result_json["Source"] = source
        result_json["URL"] = url

        if output_json_name:
            filename = f"{output_json_name}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(result_json, f, ensure_ascii=False, indent=4)

        elapsed = round(time.time() - start_time, 2)
        return result_json, soup, elapsed

    except Exception as e:
        logging.exception("Error during scraping for %s", url)
        raise RuntimeError(f"Scraper failed for {url}: {e}") from e
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ---------- Flask route: POST /api/v1/scrape ----------
@app.route("/api/v1/scrape", methods=["POST"])
def api_scrape():
    req = request.get_json(silent=True)
    if not req:
        return jsonify({"error": "expected application/json body"}), 400

    url = req.get("url")
    site_hint = req.get("site_hint")
    return_format = (req.get("return_format") or "jsonld").lower()
    validate_head = bool(req.get("validate_head", False))

    if not url or not isinstance(url, str):
        return jsonify({"error": "missing or invalid 'url' field"}), 400

    # Only allow remote http(s) URLs in public mode
    parsed = urlparse(url)
    if parsed.scheme not in ("http","https"):
        return jsonify({"error":"invalid url scheme, must be http or https"}), 400

    try:
        base_name = re.sub(
            r"[^a-zA-Z0-9_-]+", "_", parsed.path.strip("/").split("/")[-1] or "dataset"
        )[:120]

        try:
            result_json, soup, elapsed = run_scrapper(url, output_json_name=base_name)
        except Exception as e:
            logging.exception("Scraper failed")
            return jsonify({"error":"scraper failed", "details": str(e)}), 500

        try:
            dcat = convert_to_dcat_dynamic(
                result_json, url, soup=soup, site_hint=site_hint, output_path=None
            )
            ttl = dcat_to_turtle(dcat)
        except Exception as e:
            logging.exception("DCAT conversion failed")
            return jsonify({"error":"dcat conversion failed", "details": str(e)}), 500

        # validate_head can be wired here with requests.head if you want

        if return_format == "ttl":
            return Response(ttl, mimetype="text/turtle")
        elif return_format == "both":
            return jsonify({
                "dcat_jsonld": dcat,
                "dcat_ttl": ttl,
                "elapsed_seconds": elapsed
            })
        else:
            return jsonify(dcat)

    except Exception as ex:
        logging.exception("Unexpected error")
        return jsonify({"error":"unexpected error", "details": str(ex)}), 500
    
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
