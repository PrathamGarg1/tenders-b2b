
"""Extract structured fields from GeM contract PDFs with English-only canonical parsing."""

from __future__ import annotations

import hashlib
import io
import re
import unicodedata
from typing import Any

import pdfplumber

from gem_contract_schema import empty_directory_record, empty_record

GSTIN_RE = re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b", re.IGNORECASE)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")
PHONE_RE = re.compile(r"(?:\+91[\s-]?)?(?:[6-9]\d{9}|\d{3,5}[-\s]?\d{6,8})")
CONTACT_PHONE_RE = re.compile(
    r"(?:Contact(?:\s*No\.?)?|Phone(?:\s*No\.?)?)\s*[:\-]\s*([0-9+\-\s]{8,24})",
    re.IGNORECASE,
)
MONEY_RE = re.compile(r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")

# Remove Hindi/Devanagari and directional/noise code points for canonical English parsing.
DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]+")
CONTROL_NOISE_RE = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2066-\u2069\ufeff]")
NON_ASCII_RE = re.compile(r"[^\x09\x0A\x0D\x20-\x7E]+")
DUP_PIPE_RE = re.compile(r"(?:\|\s*){2,}")
CID_RE = re.compile(r"\(cid:\d+\)")

STANDARDS_RE = re.compile(
    r"\b(?:IEC|IS|ISO|IEEE|ASTM|BIS)\s*[\d.:,\-\s]+(?:\([^)]+\))?",
    re.IGNORECASE,
)

SECTION_ANCHORS: dict[str, list[str]] = {
    "organisation": ["organisation details", "organization details", "organisation name"],
    "buyer": ["buyer details", "buyer detail", "contact no. :"],
    "paying": ["paying authority details", "payment mode"],
    "seller": ["seller details", "gem seller id", "company name :"],
    "product": ["product details", "product name :", "brand type :"],
    "consignee": ["consignee detail", "consignee details", "consignee"],
    "spec": ["product specification", "specification"],
    "terms": ["terms and conditions", "general terms and conditions"],
}


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _undouble_token(tok: str) -> str:
    if len(tok) >= 4 and len(tok) % 2 == 0:
        left = tok[::2]
        right = tok[1::2]
        if left == right and re.search(r"[A-Za-z0-9]", tok):
            return left
    return tok


def _english_canonicalize(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = CONTROL_NOISE_RE.sub(" ", t)
    t = DEVANAGARI_RE.sub(" ", t)
    t = CID_RE.sub(" ", t)
    t = NON_ASCII_RE.sub(" ", t)
    t = DUP_PIPE_RE.sub(" | ", t)
    t = re.sub(r"[ ]{2,}", " ", t)
    # Keep line breaks because section slicing is line-based.
    t = re.sub(r"[\t\x0b\x0c\r]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _english_lines(page_text: str) -> list[str]:
    canon = _english_canonicalize(page_text)
    out: list[str] = []
    for line in canon.splitlines():
        ln = _normalize_whitespace(line)
        if not ln:
            continue
        # Many lines are bilingual duplicates split by pipes; dedupe segments but keep full context.
        parts = [p.strip() for p in ln.split("|") if p.strip()]
        if parts:
            dedup: list[str] = []
            seen: set[str] = set()
            for p in parts:
                low = p.lower()
                if low in seen:
                    continue
                seen.add(low)
                dedup.append(p)
            ln = " | ".join(dedup)
        ln = " ".join(_undouble_token(t) for t in ln.split(" "))
        ln = re.sub(r",,", ",", ln)
        if sum(ch.isalpha() for ch in ln) == 0 and not re.search(r"\d", ln):
            continue
        out.append(ln)
    return out


def _first_match(patterns: list[re.Pattern[str]], text: str) -> str | None:
    for pat in patterns:
        m = pat.search(text)
        if m:
            return m.group(1).strip() if m.groups() else m.group(0).strip()
    return None


def _unique_emails(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in EMAIL_RE.finditer(text):
        e = m.group(0).lower()
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


def _unique_phones(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in PHONE_RE.finditer(text):
        raw = m.group(0)
        if re.search(r"(volt|hz|amp|kva|kw|%|code)", raw, re.I):
            continue
        digits = re.sub(r"\D", "", raw)
        if len(digits) < 10:
            continue
        norm = digits[-10:]
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out[:8]


def _extract_contact_phones(text: str) -> list[str]:
    """Prefer explicitly labeled contact numbers; fallback to broad phone scan."""
    out: list[str] = []
    seen: set[str] = set()
    for m in CONTACT_PHONE_RE.finditer(text or ""):
        raw = m.group(1)
        digits = re.sub(r"\D", "", raw)
        if len(digits) < 10:
            continue
        norm = digits[-10:]
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    if out:
        return out[:4]
    return _unique_phones(text)


def _moneyish(text: str) -> str | None:
    compact = text.replace(" ", "").replace(",,", ",")
    cands = MONEY_RE.findall(compact)
    if not cands:
        return None
    decoded = [_undouble_token(c.replace(",", "")) for c in cands]
    if len(decoded) >= 2 and all(d.isdigit() for d in decoded[:2]):
        j = "".join(decoded[:2]).lstrip("0")
        if j:
            return j
    cand = max(cands, key=len)
    raw = cand.replace(",", "")
    raw = _undouble_token(raw)
    return raw if raw else None


def _section_windows(lines: list[str]) -> dict[str, tuple[int, int]]:
    idxs: list[tuple[str, int]] = []
    for i, ln in enumerate(lines):
        low = ln.lower()
        for name, anchors in SECTION_ANCHORS.items():
            if any(a in low for a in anchors):
                idxs.append((name, i))
                break
    # Deduplicate first occurrence per section
    first: dict[str, int] = {}
    for n, i in idxs:
        first.setdefault(n, i)
    ordered = sorted(first.items(), key=lambda x: x[1])
    out: dict[str, tuple[int, int]] = {}
    for pos, (name, start) in enumerate(ordered):
        end = ordered[pos + 1][1] if pos + 1 < len(ordered) else len(lines)
        out[name] = (start, end)
    return out


def _slice_lines(lines: list[str], window: tuple[int, int] | None) -> str:
    if not window:
        return ""
    s, e = window
    return "\n".join(lines[s:e])


def _set_directory_field(rec: dict[str, Any], field: str, value: Any, source: str) -> None:
    if value is None:
        return
    if isinstance(value, str):
        value = _normalize_whitespace(value)
        if not value:
            return
    if rec.get(field) not in (None, "", []):
        return
    rec[field] = value
    rec["extraction"]["source_map"][field] = source


def _normalize_price(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return _moneyish(raw) or raw


def _directory_product_is_generic(value: Any) -> bool:
    if value is None:
        return True
    text = _normalize_whitespace(str(value)).lower()
    return text in ("", "various items", "various item", "na", "n/a", "-")


def _directory_value_is_missing(value: Any) -> bool:
    if value is None:
        return True
    text = _normalize_whitespace(str(value))
    if text in ("", "-", "NA", "N/A"):
        return True
    try:
        return float(text.replace(",", "")) <= 0
    except ValueError:
        return False


def _replace_directory_field(rec: dict[str, Any], field: str, value: Any, source: str) -> None:
    if value is None:
        return
    if isinstance(value, str):
        value = _normalize_whitespace(value)
        if not value:
            return
    rec[field] = value
    rec["extraction"]["source_map"][field] = source


def _directory_contact_ready(rec: dict[str, Any]) -> bool:
    return bool(rec.get("seller_name")) and bool(rec.get("seller_email") or rec.get("seller_phone"))


def _fill_directory_from_lines(rec: dict[str, Any], lines: list[str], source_prefix: str) -> None:
    all_txt = "\n".join(lines)
    windows = _section_windows(lines)
    seller_txt = _slice_lines(lines, windows.get("seller"))
    product_txt = _slice_lines(lines, windows.get("product"))
    contact_txt = seller_txt or all_txt

    _set_directory_field(
        rec,
        "seller_name",
        _extract_keyed(seller_txt, ["Company Name"]) or _extract_keyed(all_txt, ["Company Name"]),
        f"{source_prefix}:seller_name",
    )
    _set_directory_field(
        rec,
        "seller_address",
        _extract_keyed(seller_txt, ["Address", "Registered Address"]),
        f"{source_prefix}:seller_address",
    )

    if _directory_product_is_generic(rec.get("product_name")):
        pdf_product = _extract_keyed(
            product_txt,
            ["Product Name", "Item Description", "Description"],
        ) or _extract_keyed(all_txt, ["Product Name", "Item Description", "Description"])
        _replace_directory_field(rec, "product_name", pdf_product, f"{source_prefix}:product_name")

    gst = GSTIN_RE.search(seller_txt) or GSTIN_RE.search(all_txt)
    if gst:
        _set_directory_field(rec, "seller_gstin", gst.group(0).upper(), f"{source_prefix}:seller_gstin")

    emails = _unique_emails(contact_txt)
    phones = _extract_contact_phones(contact_txt)
    if emails:
        rec["seller_emails"] = emails
        rec["seller_email"] = emails[0]
        rec["extraction"]["source_map"]["seller_emails"] = f"{source_prefix}:seller_emails"
        rec["extraction"]["source_map"]["seller_email"] = f"{source_prefix}:seller_email"
    if phones:
        rec["seller_phones"] = phones
        rec["seller_phone"] = phones[0]
        rec["extraction"]["source_map"]["seller_phones"] = f"{source_prefix}:seller_phones"
        rec["extraction"]["source_map"]["seller_phone"] = f"{source_prefix}:seller_phone"

    if _directory_value_is_missing(rec.get("contract_value")):
        total = _extract_keyed(product_txt, ["Total Order Value", "Total Price", "Total Amount"])
        _replace_directory_field(
            rec,
            "contract_value",
            _normalize_price(total),
            f"{source_prefix}:product_total_value",
        )


def _merge_directory_from_full_record(rec: dict[str, Any], full: dict[str, Any]) -> None:
    seller = full.get("seller") or {}
    product = full.get("product") or {}
    extraction = full.get("extraction") or {}

    _set_directory_field(rec, "seller_name", seller.get("company_name"), "full_fallback:seller_name")
    _set_directory_field(rec, "seller_gstin", seller.get("gstin"), "full_fallback:seller_gstin")
    _set_directory_field(rec, "seller_address", seller.get("address"), "full_fallback:seller_address")
    if _directory_product_is_generic(rec.get("product_name")):
        _replace_directory_field(rec, "product_name", product.get("title"), "full_fallback:product_name")
    if _directory_value_is_missing(rec.get("contract_value")):
        _replace_directory_field(
            rec,
            "contract_value",
            _normalize_price(product.get("total_price")),
            "full_fallback:contract_value",
        )

    emails = seller.get("emails") or []
    phones = seller.get("phones") or []
    if emails and not rec.get("seller_emails"):
        rec["seller_emails"] = emails
        rec["seller_email"] = emails[0]
        rec["extraction"]["source_map"]["seller_emails"] = "full_fallback:seller_emails"
        rec["extraction"]["source_map"]["seller_email"] = "full_fallback:seller_email"
    if phones and not rec.get("seller_phones"):
        rec["seller_phones"] = phones
        rec["seller_phone"] = phones[0]
        rec["extraction"]["source_map"]["seller_phones"] = "full_fallback:seller_phones"
        rec["extraction"]["source_map"]["seller_phone"] = "full_fallback:seller_phone"

    if extraction.get("pages") and not rec["extraction"].get("pages"):
        rec["extraction"]["pages"] = extraction.get("pages")
    if extraction.get("text_chars") and not rec["extraction"].get("text_chars"):
        rec["extraction"]["text_chars"] = extraction.get("text_chars")


def _extract_fast_text_lines(pdf_bytes: bytes, max_pages: int) -> tuple[int, int, list[str]]:
    """Extract text only, preferring PyMuPDF when installed and falling back to pdfplumber."""
    if max_pages < 1:
        max_pages = 1

    try:
        import fitz  # type: ignore[import-not-found]

        page_lines: list[str] = []
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            total_pages = len(doc)
            scan_pages = min(total_pages, max_pages)
            for idx in range(scan_pages):
                text = doc.load_page(idx).get_text("text") or ""
                page_lines.extend(_english_lines(text))
        return total_pages, scan_pages, page_lines
    except Exception:
        pass

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        scan_pages = min(total_pages, max_pages)
        page_lines = []
        for page in pdf.pages[:scan_pages]:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            page_lines.extend(_english_lines(text))
        return total_pages, scan_pages, page_lines


def _tables_to_specs(pdf: pdfplumber.PDF) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for page in pdf.pages:
        try:
            tables = page.extract_tables() or []
        except Exception:
            tables = []
        for table in tables:
            for row in table or []:
                if not row:
                    continue
                cells = [_normalize_whitespace(_english_canonicalize(str(c or ""))) for c in row]
                cells = [c for c in cells if c]
                if len(cells) < 2:
                    continue
                # first non-empty as label, rest merged as value
                label, value = cells[0], " | ".join(cells[1:])
                if len(label) > 300 or len(value) > 2500:
                    continue
                key = (label[:180], value[:480])
                if key in seen:
                    continue
                seen.add(key)
                specs.append({"label": label, "value": value})
    return specs


def _set_field(rec: dict[str, Any], path: tuple[str, str], value: Any, source: str) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    obj = rec[path[0]]
    if obj.get(path[1]) not in (None, "", []):
        return
    obj[path[1]] = value
    rec["extraction"]["source_map"][f"{path[0]}.{path[1]}"] = source


def _extract_keyed(text: str, labels: list[str]) -> str | None:
    stop_tokens = [
        " GSTIN",
        " Contact",
        " Email",
        " Address",
        " Department",
        " Designation",
        " Payment Mode",
        " Category",
        " Model",
        " HSN",
        " pieces",
    ]
    for lb in labels:
        pat = re.compile(rf"{re.escape(lb)}\s*[:\-]\s*(.+)", re.I)
        m = pat.search(text)
        if m:
            val = _normalize_whitespace(m.group(1))
            if " | " in val:
                val = val.split(" | ", 1)[0].strip()
            for tok in stop_tokens:
                if tok in val:
                    val = val.split(tok, 1)[0].strip()
            val = val.lstrip(":").strip()
            val = re.sub(r"\(cid:\d+\)", " ", val)
            val = _normalize_whitespace(val)
            if val and val not in ("-", "NA", "N/A"):
                return val
    return None


def _extract_keyed_allow_na(text: str, labels: list[str]) -> str | None:
    for lb in labels:
        pat = re.compile(rf"{re.escape(lb)}\s*[:\-]\s*([^\n|]+)", re.I)
        m = pat.search(text)
        if m:
            val = _normalize_whitespace(m.group(1)).lstrip(":").strip()
            val = re.sub(r"\(cid:\d+\)", " ", val)
            val = _normalize_whitespace(val)
            if val:
                return val
    return None


def _split_category_quadrant(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    v = _normalize_whitespace(value)
    m = re.search(r"\((Q[A-Z0-9]+)\)\s*$", v, re.I)
    if not m:
        return v, None
    q = m.group(1).upper()
    name = _normalize_whitespace(v[: m.start()])
    return (name or None), q


def _fill_entities_from_sections(rec: dict[str, Any], lines: list[str], windows: dict[str, tuple[int, int]]) -> None:
    all_txt = "\n".join(lines)
    buyer_txt = _slice_lines(lines, windows.get("buyer"))
    org_txt = _slice_lines(lines, windows.get("organisation"))
    seller_txt = _slice_lines(lines, windows.get("seller"))
    product_txt = _slice_lines(lines, windows.get("product"))
    consignee_txt = _slice_lines(lines, windows.get("consignee"))

    # Buyer
    org_name = _extract_keyed(org_txt, ["Organisation Name", "Organization Name"])
    _set_field(rec, ("buyer", "organisation_name"), org_name, "organisation_section:keyed")
    _set_field(rec, ("buyer", "designation"), _extract_keyed(buyer_txt, ["Designation"]), "buyer_section:keyed")
    _set_field(rec, ("buyer", "department"), _extract_keyed(org_txt, ["Department"]), "organisation_section:keyed")
    if not rec["buyer"].get("organisation_name"):
        _set_field(
            rec,
            ("buyer", "organisation_name"),
            _extract_keyed(all_txt, ["Organisation Name", "Organization Name"]),
            "full_text_fallback:keyed",
        )
    if not rec["buyer"].get("department"):
        _set_field(
            rec,
            ("buyer", "department"),
            _extract_keyed(all_txt, ["Department"]),
            "full_text_fallback:keyed",
        )
    if not rec["buyer"].get("designation"):
        _set_field(
            rec,
            ("buyer", "designation"),
            _extract_keyed(all_txt, ["Designation"]),
            "full_text_fallback:keyed",
        )
    if not rec["buyer"].get("organisation_name"):
        m_org = re.search(r"Organisation\s+(.+)", all_txt, re.I)
        if m_org:
            _set_field(
                rec,
                ("buyer", "organisation_name"),
                _normalize_whitespace(m_org.group(1)),
                "full_text_fallback:regex",
            )
    if rec["buyer"].get("organisation_name") and "details" in str(rec["buyer"]["organisation_name"]).lower():
        rec["buyer"]["organisation_name"] = None
    if not rec["buyer"].get("organisation_name"):
        for ln in lines:
            if re.search(r"\bDirector of\b", ln, re.I):
                _set_field(
                    rec,
                    ("buyer", "organisation_name"),
                    _normalize_whitespace(ln),
                    "full_text_fallback:line_pick",
                )
                break
    rec["buyer"]["emails"] = _unique_emails(buyer_txt or org_txt)
    rec["buyer"]["phones"] = _extract_contact_phones(buyer_txt or org_txt)
    if rec["buyer"]["emails"]:
        rec["extraction"]["source_map"]["buyer.emails"] = "buyer_section:regex"
    if rec["buyer"]["phones"]:
        rec["extraction"]["source_map"]["buyer.phones"] = "buyer_section:regex"

    # Seller
    _set_field(rec, ("seller", "company_name"), _extract_keyed(seller_txt, ["Company Name"]), "seller_section:keyed")
    if not rec["seller"].get("company_name"):
        _set_field(
            rec,
            ("seller", "company_name"),
            _extract_keyed(all_txt, ["Company Name"]),
            "full_text_fallback:keyed",
        )
    gst = GSTIN_RE.search(seller_txt)
    if gst:
        _set_field(rec, ("seller", "gstin"), gst.group(0).upper(), "seller_section:regex")
    _set_field(rec, ("seller", "address"), _extract_keyed(seller_txt, ["Address", "Registered Address"]), "seller_section:keyed")
    _set_field(rec, ("seller", "selling_status"), _extract_keyed(seller_txt, ["Selling As", "Selling Status"]), "seller_section:keyed")
    flags: list[str] = []
    for pat in [r"Reseller\s+not\s+verified\s+by\s+OEM", r"Catalogue\s+not\s+verified\s+by\s+OEM", r"OEM\s+verified"]:
        if re.search(pat, seller_txt, re.I):
            flags.append(re.search(pat, seller_txt, re.I).group(0))
    rec["seller"]["verification_flags"] = list(dict.fromkeys(flags))
    rec["seller"]["emails"] = _unique_emails(seller_txt)
    rec["seller"]["phones"] = _extract_contact_phones(seller_txt)
    if rec["seller"]["emails"]:
        rec["extraction"]["source_map"]["seller.emails"] = "seller_section:regex"
    if rec["seller"]["phones"]:
        rec["extraction"]["source_map"]["seller.phones"] = "seller_section:regex"

    # Product
    _set_field(rec, ("product", "title"), _extract_keyed(product_txt, ["Product Name", "Item Description", "Description"]), "product_section:keyed")
    _set_field(
        rec,
        ("product", "brand"),
        _extract_keyed_allow_na(product_txt, ["Brand"]),
        "product_section:keyed",
    )
    _set_field(rec, ("product", "brand_type"), _extract_keyed(product_txt, ["Brand Type"]), "product_section:keyed")
    _set_field(
        rec,
        ("product", "category"),
        _extract_keyed(product_txt, ["Category Name & Quadrant", "Category Name"]),
        "product_section:keyed",
    )
    _set_field(
        rec,
        ("product", "catalogue_status"),
        _extract_keyed(product_txt, ["Catalogue Status", "Catalog Status"]),
        "product_section:keyed",
    )
    _set_field(
        rec,
        ("product", "selling_as"),
        _extract_keyed(product_txt, ["Selling As", "Selling Status"]),
        "product_section:keyed",
    )
    _set_field(rec, ("product", "model"), _extract_keyed(product_txt, ["Model"]), "product_section:keyed")
    _set_field(
        rec, ("product", "hsn_code"), _extract_keyed(product_txt, ["HSN Code"]), "product_section:keyed"
    )
    _set_field(rec, ("product", "uom"), _extract_keyed(product_txt, ["Unit", "UOM", "Unit of Measurement"]), "product_section:keyed")
    _set_field(rec, ("product", "quantity"), _extract_keyed(product_txt, ["Ordered Quantity", "Quantity"]), "product_section:keyed")
    if not rec["product"].get("title"):
        _set_field(
            rec,
            ("product", "title"),
            _extract_keyed(all_txt, ["Product Name", "Item Description", "Description"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("brand_type"):
        _set_field(
            rec,
            ("product", "brand_type"),
            _extract_keyed(all_txt, ["Brand Type"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("brand"):
        _set_field(
            rec,
            ("product", "brand"),
            _extract_keyed_allow_na(all_txt, ["Brand"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("category"):
        _set_field(
            rec,
            ("product", "category"),
            _extract_keyed(all_txt, ["Category Name & Quadrant", "Category Name"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("category"):
        m_cat = re.search(
            r"Category\s+Name(?:\s*&+\s*Quadrant)?\s*[:\-]+\s*([^\n|]+)",
            all_txt,
            re.I,
        )
        if m_cat:
            _set_field(
                rec,
                ("product", "category"),
                _normalize_whitespace(m_cat.group(1)),
                "full_text_fallback:regex",
            )
    if rec["product"].get("category"):
        cname, quad = _split_category_quadrant(rec["product"].get("category"))
        _set_field(rec, ("product", "category_name"), cname, "derived:category_split")
        _set_field(rec, ("product", "quadrant"), quad, "derived:category_split")
    if not rec["product"].get("catalogue_status"):
        _set_field(
            rec,
            ("product", "catalogue_status"),
            _extract_keyed(all_txt, ["Catalogue Status", "Catalog Status"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("selling_as"):
        _set_field(
            rec,
            ("product", "selling_as"),
            _extract_keyed(all_txt, ["Selling As", "Selling Status"]),
            "full_text_fallback:keyed",
        )
    if rec["product"].get("selling_as"):
        sa = str(rec["product"]["selling_as"])
        m_sa = re.search(
            r"(Reseller\s+not\s+verified\s+by\s+OEM|OEM\s+verified|Manufacturer|Reseller)",
            sa,
            re.I,
        )
        if m_sa:
            sa = m_sa.group(1)
        sa = re.sub(r"\s+\d+\s+pieces.*$", "", sa, flags=re.I)
        sa = re.sub(r"\s+\d+\s*$", "", sa)
        rec["product"]["selling_as"] = _normalize_whitespace(sa)
    if not rec["product"].get("model"):
        _set_field(
            rec,
            ("product", "model"),
            _extract_keyed(all_txt, ["Model"]),
            "full_text_fallback:keyed",
        )
    if not rec["product"].get("hsn_code"):
        _set_field(
            rec,
            ("product", "hsn_code"),
            _extract_keyed(all_txt, ["HSN Code"]),
            "full_text_fallback:keyed",
        )

    up = _extract_keyed(product_txt, ["Unit Price"])
    if up:
        _set_field(rec, ("product", "unit_price"), _moneyish(up), "product_section:keyed_money")
    tp = _extract_keyed(product_txt, ["Total Order Value", "Total Price", "Total Amount"])
    if tp:
        _set_field(rec, ("product", "total_price"), _moneyish(tp), "product_section:keyed_money")

    # Consignee
    _set_field(rec, ("consignee", "raw"), consignee_txt[:1800] if consignee_txt else None, "consignee_section:raw")
    caddr = _extract_keyed(consignee_txt, ["Address"])
    if caddr:
        _set_field(rec, ("consignee", "address"), caddr, "consignee_section:keyed")
    elif not rec["consignee"].get("address"):
        _set_field(
            rec,
            ("consignee", "address"),
            _extract_keyed(all_txt, ["Address"]),
            "full_text_fallback:keyed",
        )


def _merge_product_from_specs(rec: dict[str, Any]) -> None:
    for row in rec.get("specifications") or []:
        lab = (row.get("label") or "").lower()
        val = _normalize_whitespace(row.get("value") or "")
        if not val:
            continue
        if "brand type" in lab:
            _set_field(rec, ("product", "brand_type"), val, "spec_table")
        elif lab.startswith("brand"):
            _set_field(rec, ("product", "brand"), val, "spec_table")
        elif "category name" in lab:
            _set_field(rec, ("product", "category"), val, "spec_table")
        elif "catalogue status" in lab or "catalog status" in lab:
            _set_field(rec, ("product", "catalogue_status"), val, "spec_table")
        elif "selling as" in lab or "selling status" in lab:
            _set_field(rec, ("product", "selling_as"), val, "spec_table")
        elif lab == "model" or lab.endswith(" model"):
            _set_field(rec, ("product", "model"), val, "spec_table")
        elif "hsn code" in lab:
            _set_field(rec, ("product", "hsn_code"), val, "spec_table")
        elif "unit price" in lab and "total" not in lab:
            _set_field(rec, ("product", "unit_price"), _moneyish(val), "spec_table")
        elif "total order value" in lab or "total price" in lab:
            _set_field(rec, ("product", "total_price"), _moneyish(val), "spec_table")
        elif "quantity" in lab:
            _set_field(rec, ("product", "quantity"), val, "spec_table")
        elif lab in ("unit", "uom") or "unit of measurement" in lab:
            _set_field(rec, ("product", "uom"), val, "spec_table")


def _standards_from_text(text: str) -> list[str]:
    found = STANDARDS_RE.findall(text or "")
    return list(dict.fromkeys(_normalize_whitespace(s) for s in found if s.strip()))[:80]


def _post_quality(rec: dict[str, Any], all_text: str) -> None:
    qf: list[str] = rec["extraction"]["quality_flags"]
    rr: list[str] = rec["extraction"]["reject_reasons"]

    if not rec["seller"].get("company_name"):
        qf.append("missing_seller_company")
    if not rec["buyer"].get("organisation_name"):
        qf.append("missing_buyer_organisation")
    if not rec["product"].get("title"):
        qf.append("missing_product_title")
    if (rec["extraction"].get("text_chars") or 0) < 600:
        qf.append("low_text_density")
    if len(rec.get("specifications") or []) < 3:
        qf.append("low_spec_rows")

    # Reject if core entities are mostly missing.
    critical_missing = sum(
        [
            1 if not rec["seller"].get("company_name") else 0,
            1 if not rec["product"].get("title") else 0,
            1 if not rec["buyer"].get("organisation_name") else 0,
        ]
    )
    if critical_missing >= 2:
        rr.append("missing_core_entities")

    if not rec["seller"].get("gstin") and "gstin" in all_text.lower():
        qf.append("gstin_label_present_value_missing")

    rec["extraction"]["quality_flags"] = list(dict.fromkeys(qf))
    rec["extraction"]["reject_reasons"] = list(dict.fromkeys(rr))
    rec["extraction"]["is_reject"] = bool(rec["extraction"]["reject_reasons"])


def _post_directory_quality(rec: dict[str, Any]) -> None:
    qf: list[str] = rec["extraction"]["quality_flags"]
    rr: list[str] = rec["extraction"]["reject_reasons"]

    if not rec.get("product_name"):
        qf.append("missing_product_name")
    if not rec.get("contract_value"):
        qf.append("missing_contract_value")
    if not rec.get("seller_name"):
        qf.append("missing_seller_name")
    if not (rec.get("seller_email") or rec.get("seller_phone")):
        qf.append("missing_seller_contact")
    if (rec["extraction"].get("text_chars") or 0) < 300:
        qf.append("low_text_density")

    if not rec.get("seller_name") and not (rec.get("seller_email") or rec.get("seller_phone")):
        rr.append("missing_seller_identity_and_contact")

    rec["extraction"]["quality_flags"] = list(dict.fromkeys(qf))
    rec["extraction"]["reject_reasons"] = list(dict.fromkeys(rr))
    rec["extraction"]["is_reject"] = bool(rec["extraction"]["reject_reasons"])


def extract_directory_record(
    pdf_bytes: bytes,
    contract_meta: dict[str, Any] | None = None,
    *,
    max_pages: int = 4,
    allow_slow_fallback: bool = True,
) -> dict[str, Any]:
    """Fast app/search record: product/value from listing metadata, seller contacts from PDF text."""
    meta_in = contract_meta or {}
    rec = empty_directory_record(
        {
            "contract_no": meta_in.get("contract_no"),
            "list_date": meta_in.get("list_date"),
            "primary_item": meta_in.get("primary_item"),
            "list_price": _normalize_price(meta_in.get("list_price")),
        }
    )

    if rec.get("product_name"):
        rec["extraction"]["source_map"]["product_name"] = "list_metadata"
    if rec.get("contract_value"):
        rec["extraction"]["source_map"]["contract_value"] = "list_metadata"

    if not pdf_bytes:
        rec["extraction"]["is_reject"] = True
        rec["extraction"]["reject_reasons"] = ["empty_pdf_bytes"]
        rec["extraction"]["quality_flags"] = ["missing_seller_name", "missing_seller_contact", "low_text_density"]
        return rec

    pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
    rec["source_pdf_sha256"] = pdf_hash
    rec["extraction"]["pdf_sha256"] = pdf_hash

    try:
        pages, pages_scanned, page_lines = _extract_fast_text_lines(pdf_bytes, max_pages)
        rec["extraction"]["pages"] = pages
        rec["extraction"]["pages_scanned"] = pages_scanned
        rec["extraction"]["text_chars"] = len("\n".join(page_lines))
        _fill_directory_from_lines(rec, page_lines, "directory_fast")

        if allow_slow_fallback and not _directory_contact_ready(rec):
            full = extract_contract_record(pdf_bytes, meta_in)
            _merge_directory_from_full_record(rec, full)
            rec["extraction"]["mode"] = "directory_fast_with_full_fallback"

    except Exception:
        rec["extraction"]["text_chars"] = 0
        rec["extraction"]["reject_reasons"].append("pdf_parse_exception")

    _post_directory_quality(rec)
    return rec


def extract_contract_record(pdf_bytes: bytes, contract_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Parse a GeM contract PDF into English-only structured record."""
    meta_in = contract_meta or {}
    rec = empty_record(
        {
            "contract_no": meta_in.get("contract_no"),
            "list_date": meta_in.get("list_date"),
            "primary_item": meta_in.get("primary_item"),
            "list_price": meta_in.get("list_price"),
        }
    )

    if not pdf_bytes:
        rec["extraction"]["is_reject"] = True
        rec["extraction"]["reject_reasons"] = ["empty_pdf_bytes"]
        return rec

    rec["extraction"]["pdf_sha256"] = hashlib.sha256(pdf_bytes).hexdigest()

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            rec["extraction"]["pages"] = len(pdf.pages)
            page_lines: list[str] = []
            raw_pages: list[str] = []
            for page in pdf.pages:
                try:
                    t = page.extract_text() or ""
                except Exception:
                    t = ""
                raw_pages.append(t)
                page_lines.extend(_english_lines(t))

            all_text = "\n".join(page_lines)
            rec["extraction"]["text_chars"] = len(all_text)
            windows = _section_windows(page_lines)

            rec["specifications"] = _tables_to_specs(pdf)
            _fill_entities_from_sections(rec, page_lines, windows)
            _merge_product_from_specs(rec)

            if not rec["product"].get("title") and meta_in.get("primary_item"):
                _set_field(rec, ("product", "title"), meta_in.get("primary_item"), "list_metadata")

            if not rec["seller"].get("gstin"):
                m = GSTIN_RE.search(all_text)
                if m:
                    _set_field(rec, ("seller", "gstin"), m.group(0).upper(), "full_text_fallback")

            rec["standards_mentions"] = _standards_from_text(all_text)
            _post_quality(rec, all_text)

    except Exception:
        rec["extraction"]["text_chars"] = 0
        rec["extraction"]["is_reject"] = True
        rec["extraction"]["reject_reasons"] = ["pdf_parse_exception"]

    return rec
