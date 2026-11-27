"""Bounded, source-restricted document retrieval for the refresh worker."""

import io
import re
import time
import zipfile
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.cookiejar import CookieJar
from urllib import parse, request
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from pypdf import PdfReader

MAX_BYTES = 8 * 1024 * 1024
MAX_DOCUMENTS = 3
MAX_PAGES = 30
MAX_TEXT = 12000
MAX_ANALYSIS_TEXT = 120000
EXTRACTION_VERSION = 4
SOURCE_HOSTS = {
    "UNGM": {"www.ungm.org", "ungm.org"},
    "UNDP Procurement": {"procurement-notices.undp.org", "www.undp.org", "undp.org"},
    "ReliefWeb": {"reliefweb.int", "www.reliefweb.int"},
    "Welthungerhilfe": {"www.welthungerhilfe.org", "welthungerhilfe.org"},
    "Plan International": {"plan-international.org", "www.plan-international.org"},
    "CAF": {"www.cafonline.com", "cafonline.com"},
    "TED": {"ted.europa.eu"},
    "Find a Tender": {"www.find-tender.service.gov.uk", "find-tender.service.gov.uk"},
    "EEAS": {"www.eeas.europa.eu", "eeas.europa.eu"},
    "Fairtrade International": {"www.fairtrade.net", "fairtrade.net"},
    "SIMAP": {"www.simap.ch", "simap.ch"},
    "ICIMOD": {"www.icimod.org", "icimod.org"},
}
FILE_RE = re.compile(r"\.(pdf|docx|doc|xlsx|xls|odt)(?:$|[?#])", re.I)
DATE_RE = re.compile(r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\b")
DEADLINE_RE = re.compile(r"\b(?:submission deadline|deadline (?:for|to|of) (?:submission|receipt|submitting|express(?:ing)? (?:your )?interest)|closing date|(?:bids?|offers?|proposals?|applications?|expressions? of interest)\s+(?:must be\s+)?(?:submitted|received|due)\s+(?:by|before|on)|deadline for receipt)\b", re.I)
EXCLUDE_DEADLINE_RE = re.compile(r"\b(?:questions?|clarifications?|queries|enquir(?:y|ies)|inquiries|pre.?bid|registration|briefing|opening|award|contract|delivery|extension|extended|amendment)\b", re.I)
REQUIREMENT_RE = re.compile(r"\b(?:must|shall|required|requirement|eligib(?:le|ility)|minimum (?:of|experience|qualification)|at least \d+ years?|mandatory|deliverables?|scope of (?:work|services)|terms of reference)\b", re.I)
GUIDANCE_NAME_RE = re.compile(r"(?:bidder|supplier|vendor).{0,30}(?:instruction|guide|manual)|(?:instruction|guide|manual).{0,30}(?:bidder|supplier|vendor)", re.I)
FIT_EVIDENCE_RE = re.compile(
    r"\b(?:photograph\w*|videograph\w*|video production|film(?:ing)?|documentary|visual storytelling|"
    r"audiovisual|audio.visual|model release|image rights|image release|photo consent|visual consent|"
    r"consent (?:management|platform|record|form|capture|collection)|informed consent|consent to (?:film|photograph)|"
    r"image.{0,30}consent|consent.{0,30}(?:image|photo|visual|media|filming)|"
    r"supply (?:of )?(?:cameras?|equipment)|video surveillance)\b", re.I
)

def document_priority(candidate):
    name = candidate[0].lower()
    if GUIDANCE_NAME_RE.search(name):
        return 2
    if re.search(r"terms? of reference|\btor\b|scope|specification|statement of work|\brfp\b|\brfq\b", name):
        return 0
    return 1


def allowed_url(url, hosts):
    parsed = parse.urlsplit(url)
    try:
        return (parsed.scheme == "https" and parsed.hostname in hosts and not parsed.username
                and not parsed.password and parsed.port in (None, 443))
    except ValueError:
        return False


class SourceRedirectHandler(request.HTTPRedirectHandler):
    def __init__(self, hosts):
        self.hosts = hosts

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_url(newurl, self.hosts):
            raise ValueError("Redirected outside the source's allowed hosts")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def parse_document_date(value):
    value = re.sub(r"\s+", " ", value.strip()).replace(",", "")
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    # Numeric dates with two possible interpretations are not safe to infer.
    match = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})", value)
    if match:
        first, second, year = map(int, match.groups())
        if first <= 12 and second <= 12:
            return None
        try:
            return datetime(year, second if first > 12 else first, first if first > 12 else second).date().isoformat()
        except ValueError:
            pass
    return None


def extract_findings(pages, name, url):
    """Return attributed excerpts, not instructions to the application."""
    if GUIDANCE_NAME_RE.search(name or ""):
        return [], []
    deadlines, requirements = [], []
    for page_number, text in pages:
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
        for index, line in enumerate(lines):
            # PDFs commonly split a label and its value across adjacent lines.
            context = line
            if DEADLINE_RE.search(line) and not DATE_RE.search(line) and index + 1 < len(lines):
                context += " " + lines[index + 1]
            context = context[:600]
            attribution = {"document": name, "url": url, "page": page_number, "excerpt": context}
            if DEADLINE_RE.search(context) and not EXCLUDE_DEADLINE_RE.search(context):
                for raw in DATE_RE.findall(context):
                    value = parse_document_date(raw)
                    if value:
                        deadlines.append({**attribution, "date": value})
            if REQUIREMENT_RE.search(line) and len(line) >= 20:
                requirements.append(attribution)
    def unique(items, key):
        seen, result = set(), []
        for item in items:
            identity = key(item)
            if identity not in seen:
                seen.add(identity)
                result.append(item)
        return result
    return unique(deadlines, lambda x: (x["date"], x["document"], x["excerpt"]))[:20], unique(requirements, lambda x: (x["document"], x["excerpt"]))[:20]


def extract_fit_excerpts(pages, name):
    """Keep source-labelled passages, not generic vendor manuals, for fit review."""
    if GUIDANCE_NAME_RE.search(name or ""):
        return []
    excerpts, seen = [], set()
    for page, text in pages:
        for line in text.splitlines():
            cleaned = re.sub(r"\s+", " ", line).strip()
            if len(cleaned) < 15 or not FIT_EVIDENCE_RE.search(cleaned):
                continue
            cleaned = cleaned[:500]
            if cleaned.lower() in seen:
                continue
            seen.add(cleaned.lower())
            excerpts.append({"page": page, "text": cleaned})
            if len(excerpts) >= 30:
                return excerpts
    return excerpts


def extract_pages(data, name):
    if data.startswith(b"%PDF-"):
        reader = PdfReader(io.BytesIO(data))
        pages, total = [], 0
        for index in range(min(len(reader.pages), MAX_PAGES)):
            text = (reader.pages[index].extract_text() or "")[:MAX_ANALYSIS_TEXT - total]
            pages.append((index + 1, text))
            total += len(text)
            if total >= MAX_ANALYSIS_TEXT:
                break
        return pages, len(reader.pages) > MAX_PAGES or total >= MAX_ANALYSIS_TEXT
    if name.lower().endswith(".docx") and data.startswith(b"PK"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            info = archive.getinfo("word/document.xml")
            if info.file_size > MAX_BYTES:
                raise ValueError("Document content too large")
            root = ET.fromstring(archive.read(info))
            ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
            paragraphs = ["".join(node.text or "" for node in p.iter(ns + "t")) for p in root.iter(ns + "p")]
            text = "\n".join(paragraphs)
            return [(None, text[:MAX_ANALYSIS_TEXT])], len(text) > MAX_ANALYSIS_TEXT
    return [], False


def read_source_documents(item, *, headers, ssl_context, timeout=5, stop_at=None):
    source, notice_url = item.get("source"), item.get("link") or ""
    hosts = SOURCE_HOSTS.get(source, set())
    if not allowed_url(notice_url, hosts):
        raise ValueError("Notice host is not supported for this source")
    opener = request.build_opener(request.HTTPSHandler(context=ssl_context),
                                  request.HTTPCookieProcessor(CookieJar()), SourceRedirectHandler(hosts))
    opener.addheaders = list(headers.items())
    candidates = []
    if FILE_RE.search(parse.urlsplit(notice_url).path):
        candidates.append((parse.unquote(parse.urlsplit(notice_url).path.rsplit("/", 1)[-1]), notice_url))
    else:
        try:
            with opener.open(notice_url, timeout=timeout) as response:
                html = response.read(MAX_BYTES + 1)
        except Exception:
            if not item.get("documentLinks"):
                raise
            html = b""
        if len(html) > MAX_BYTES:
            raise ValueError("Notice page is too large")
        soup = BeautifulSoup(html, "html.parser")
        # Exclude site-wide navigation and footer links from the notice's documents.
        for node in soup.select("nav, header, footer"):
            node.decompose()
        if not item.get("scope"):
            description = soup.select_one('[itemprop="description"], .job-description, #noticeDescription')
            if description:
                extracted = description.get_text(" ", strip=True)
                if len(extracted) >= 80:
                    item["scope"] = extracted[:MAX_ANALYSIS_TEXT]
        for anchor in soup.select("a[href]"):
            href = anchor.get("href") or ""
            name = anchor.get_text(" ", strip=True) or parse.unquote(parse.urlsplit(href).path.rsplit("/", 1)[-1])
            if "lnkShowDocument" in (anchor.get("class") or []) or FILE_RE.search(href) or FILE_RE.search(name):
                candidates.append((name, parse.urljoin(notice_url, href)))
    for doc in item.get("documentLinks") or []:
        if isinstance(doc, dict) and doc.get("url") and (FILE_RE.search(doc["url"]) or FILE_RE.search(doc.get("name") or "")):
            candidates.append((doc.get("name") or "Tender document", doc["url"]))
    documents, seen = [], set()
    omitted = False
    for name, url in sorted(candidates, key=document_priority):
        if url in seen:
            continue
        seen.add(url)
        if len(documents) >= MAX_DOCUMENTS or (stop_at and time.monotonic() >= stop_at):
            omitted = True
            break
        entry = {"name": name[:300], "url": url, "status": "unavailable", "text": "", "deadlines": [], "requirements": []}
        if not allowed_url(url, hosts):
            entry["status"] = "external"
        elif not re.search(r"\.(?:pdf|docx)(?:$|[?#])", name, re.I) and not re.search(r"\.(?:pdf|docx)$", parse.urlsplit(url).path, re.I):
            entry["status"] = "unsupported"
        else:
            try:
                with opener.open(request.Request(url, headers={"Referer": notice_url}), timeout=timeout) as response:
                    data = response.read(MAX_BYTES + 1)
                if len(data) > MAX_BYTES:
                    entry["status"] = "too_large"
                else:
                    pages, analysis_truncated = extract_pages(data, name)
                    text = "\n".join(text for _, text in pages).strip()
                    entry["text"] = text[:MAX_TEXT]
                    entry["truncated"] = len(text) > MAX_TEXT
                    entry["analysisTruncated"] = analysis_truncated
                    entry["status"] = "read" if text else "no_text"
                    entry["deadlines"], entry["requirements"] = extract_findings(pages, name, url)
                    entry["fitExcerpts"] = extract_fit_excerpts(pages, name)
            except Exception:
                entry["status"] = "unavailable"
        documents.append(entry)
    if omitted:
        documents.append({"name": "Additional documents", "url": notice_url, "status": "limit",
                          "text": "", "deadlines": [], "requirements": []})
    return documents


def read_ungm_documents(notice_url, *, headers, ssl_context, timeout=8):
    return read_source_documents({"source": "UNGM", "link": notice_url}, headers=headers, ssl_context=ssl_context, timeout=timeout)


def enrich_documents(items, *, headers, ssl_context, budget_seconds=35, max_notices=20):
    """Interleave sources and preserve cached data on failures or budget exhaustion."""
    stop_at = time.monotonic() + budget_seconds
    groups = defaultdict(deque)
    for item in sorted(items, key=lambda row: bool(row.get("documentStatus"))):
        if item.get("source") in SOURCE_HOSTS and item.get("link") and item.get("documentStatus") != "checked":
            groups[item["source"]].append(item)
    candidates = []
    while any(groups.values()):
        for queue in groups.values():
            if queue:
                candidates.append(queue.popleft())
    for item in candidates[:max_notices]:
        if time.monotonic() >= stop_at:
            break
        try:
            docs = read_source_documents(item, headers=headers, ssl_context=ssl_context, stop_at=stop_at)
            item["documents"] = docs
            item["documentStatus"] = "partial" if any(doc["status"] in {"unavailable", "external", "limit"} for doc in docs) else "checked"
            item["documentsCheckedAt"] = datetime.now(timezone.utc).isoformat()
            item["documentExtractionVersion"] = EXTRACTION_VERSION
        except Exception:
            item["documentStatus"] = "unavailable"
    for item in candidates:
        if not item.get("documentStatus"):
            item["documentStatus"] = "pending"


# Backward-compatible entry point for callers of the earlier UNGM-only reader.
def enrich_ungm_items(items, *, headers, ssl_context, budget_seconds=35, max_notices=10):
    enrich_documents([item for item in items if item.get("source") == "UNGM"], headers=headers,
                     ssl_context=ssl_context, budget_seconds=budget_seconds, max_notices=max_notices)


def apply_document_deadline(item):
    """Use one identified submission date only when the source provides none."""
    documents = item.get("documents") or []
    # A file we could not inspect might contain an extension or a conflicting
    # deadline. Show the findings but do not promote them to the opportunity.
    if (item.get("documentStatus") != "checked" or not documents
            or any(doc.get("status") != "read" or doc.get("analysisTruncated") for doc in documents)):
        return
    findings = [finding for doc in documents for finding in doc.get("deadlines") or []]
    dates = {finding.get("date") for finding in findings if finding.get("date")}
    if not item.get("deadline") and len(dates) == 1:
        item["deadline"] = next(iter(dates))
        item["deadlineFromDocument"] = True
