"""
Symplectic Elements API XML Parser — Full Schema Version

Parses <api:object> elements from the Symplectic Elements REST API
into flat Python dicts matching the structure the DenormalizationEngine expects.

Produces the following entity keys (which map to child tables via config):
  records          → one row per <record> (all tables)
  fields           → one row per <field> across all records (grant)
  address          → from address-list fields (grant, user, teaching_activity)
  person           → from person-list fields  (publication)
  keywords         → keywords field + all-labels/keywords (publication)
  identifiers      → identifiers field (publication) OR user-identifier-associations (user)
  links            → links field (publication)
  funding          → funding-acknowledgements field (publication)
  subtypes         → items-type fields (publication)
  journal          → <journal> top-level element (publication)
  group_properties → <group-properties> scalar fields (group)
  group_membership → <group-properties><group-membership> (group)
  org_data         → <organisation-defined-data> elements (user)
  search_settings  → <user-search-settings><default> (user)
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_NS = "http://www.symplectic.co.uk/publications/api"

# Top-level child elements that need special structural handling (not flat-scalar)
_STRUCTURAL_TAGS = frozenset({
    "records", "fields", "relationships",
    "group-properties", "journal", "all-labels",
    "user-search-settings", "user-identifier-associations",
    "organisation-defined-data",
})


def _T(tag: str) -> str:
    """Return fully-qualified tag: {namespace}tag"""
    return f"{{{_NS}}}{tag}"


class SymplecticParser:
    """
    Parses Symplectic Elements API XML objects into dicts
    compatible with DenormalizationEngine.
    """

    @classmethod
    def parse_payload(cls, xml_str: str, root_key: str, entity_key: str) -> Dict[str, Any]:
        """
        Parse a Symplectic API XML string.
        Returns: { root_key: { "engine": ..., "pubdate": ..., entity_key: { flat_fields + list_children } } }
        Scalar fields go into the flat parent row; list values are child table data.
        """
        try:
            root = ET.fromstring(xml_str.strip())
        except ET.ParseError as e:
            logger.error(f"SymplecticParser: XML parse error: {e}")
            raise

        category      = root.get("category", "")
        obj_id        = root.get("id", "")
        last_modified = root.get("last-modified-when", "")

        # ── Flat entity: root attributes ──────────────────────────────────────
        entity: Dict[str, Any] = {
            f"{entity_key}_id":   obj_id,
            "category":           category,
            "type_display_name":  root.get("type-display-name", "") or None,
            "object_type":        root.get("type", "") or None,
            "type_id":            root.get("type-id", "") or None,
            "created_when":       root.get("created-when", "") or None,
            "last_modified_when": last_modified or None,
            "last_affected_when": root.get("last-affected-when", "") or None,
        }
        # User-specific root attributes
        for attr in ("proprietary-id", "username", "authenticating-authority"):
            val = root.get(attr)
            if val is not None:
                entity[cls._norm(attr)] = val or None

        # ── Flat scalar top-level child elements ──────────────────────────────
        for child in root:
            tag = cls._strip_ns(child.tag)
            if tag in _STRUCTURAL_TAGS:
                continue
            col  = cls._norm(tag)
            text = (child.text or "").strip()
            if text:
                entity[col] = text

        # ── Records: extract ALL records + first-record native fields ─────────
        first_source_name = ""
        records_list: List[Dict] = []
        fields_list:  List[Dict] = []   # raw field dump across all records
        native_scalars: Dict[str, Any]   = {}
        native_lists:   Dict[str, List]  = {}

        records_el = root.find(_T("records"))
        if records_el is not None:
            for rec_idx, rec_el in enumerate(records_el.findall(_T("record"))):
                rec_id    = rec_el.get("id", "")
                src_name  = rec_el.get("source-display-name", "")
                rec_row: Dict[str, Any] = {
                    "record_id":            rec_id or None,
                    "source_name":          rec_el.get("source-name", "") or None,
                    "source_display_name":  src_name or None,
                    "id_at_source":         rec_el.get("id-at-source", "") or None,
                    "record_last_modified": rec_el.get("last-modified-when", "") or None,
                    "format":               rec_el.get("format", "") or None,
                }
                vs_el = rec_el.find(_T("verification-status"))
                if vs_el is not None:
                    rec_row["verification_status"] = (vs_el.text or "").strip() or None
                cc_el = rec_el.find(_T("citation-count"))
                if cc_el is not None:
                    rec_row["citation_count"] = (cc_el.text or "").strip() or None
                records_list.append(rec_row)

                if rec_idx == 0:
                    first_source_name = src_name
                    entity["source_name"]  = src_name or None
                    entity["id_at_source"] = rec_el.get("id-at-source", "") or None

                native_el = rec_el.find(_T("native"))
                if native_el is not None:
                    for field_el in native_el.findall(_T("field")):
                        fname = field_el.get("name", "")
                        ftype = field_el.get("type", "")
                        fdname = field_el.get("display-name", "")
                        # Raw fields dump → grant_fields / activity_records fields etc.
                        fields_list.append({
                            "record_id":    rec_id or None,
                            "field_name":   fname or None,
                            "display_name": fdname or None,
                            "field_type":   ftype or None,
                            "field_value":  cls._get_field_text(field_el),
                        })
                        # Parse scalars + list children from FIRST record only
                        if rec_idx == 0:
                            for col, val in cls._extract_field(fname, ftype, field_el):
                                if isinstance(val, list):
                                    # Merge lists (multiple items-type fields both key as "subtypes")
                                    if col in native_lists:
                                        native_lists[col] = native_lists[col] + val
                                    else:
                                        native_lists[col] = val
                                else:
                                    native_scalars[col] = val

        entity.update(native_scalars)

        # ── Category-specific structural elements ─────────────────────────────

        # GROUP: group-properties → group_properties + group_membership
        group_properties_list: List[Dict] = []
        group_membership_list: List[Dict] = []
        gp_el = root.find(_T("group-properties"))
        if gp_el is not None:
            gp_row: Dict[str, Any] = {}
            for child in gp_el:
                tag = cls._strip_ns(child.tag)
                if tag in ("group-membership", "children", "parent"):
                    continue
                col  = cls._norm(tag)
                text = (child.text or "").strip()
                if text:
                    gp_row[col] = text
            if gp_row:
                group_properties_list = [gp_row]

            gm_el = gp_el.find(_T("group-membership"))
            if gm_el is not None:
                gm_row: Dict[str, Any] = {}
                for child in gm_el:
                    tag = cls._strip_ns(child.tag)
                    if tag in ("explicit-group-members", "implicit-group-members"):
                        gm_row[cls._norm(tag) + "_href"] = child.get("href", "") or None
                        continue
                    col  = cls._norm(tag)
                    text = (child.text or "").strip()
                    if text:
                        gm_row[col] = text
                if gm_row:
                    group_membership_list = [gm_row]

        # PUBLICATION: <journal> top-level element
        journal_list: List[Dict] = []
        journal_el = root.find(_T("journal"))
        if journal_el is not None:
            j_row: Dict[str, Any] = {
                "issn":  journal_el.get("issn", "") or None,
                "title": journal_el.get("title", "") or None,
                "href":  journal_el.get("href", "") or None,
            }
            # journal title can also be in journal/records/record/title
            for rec_el in journal_el.findall(f".//{_T('record')}"):
                title_el = rec_el.find(_T("title"))
                if title_el is not None and title_el.text:
                    j_row["title"] = j_row.get("title") or title_el.text.strip()
            journal_list = [j_row]

        # PUBLICATION: <all-labels> → additional keywords
        al_keywords: List[Dict] = []
        al_el = root.find(_T("all-labels"))
        if al_el is not None:
            kws_el = al_el.find(_T("keywords"))
            if kws_el is not None:
                for kw_el in kws_el.findall(_T("keyword")):
                    al_keywords.append({
                        "keyword": (kw_el.text or "").strip() or None,
                        "scheme":  kw_el.get("scheme", "") or None,
                        "origin":  kw_el.get("origin", "") or None,
                        "source":  kw_el.get("source", "") or None,
                    })

        # USER: <organisation-defined-data> elements
        org_data_list: List[Dict] = []
        for od_el in root.findall(_T("organisation-defined-data")):
            org_data_list.append({
                "field_number": od_el.get("field-number", "") or None,
                "field_name":   od_el.get("field-name", "") or None,
                "value":        (od_el.text or "").strip() or None,
            })

        # USER: <user-search-settings><default>
        search_settings_list: List[Dict] = []
        uss_el = root.find(_T("user-search-settings"))
        if uss_el is not None:
            def_el = uss_el.find(_T("default"))
            if def_el is not None:
                ss_row: Dict[str, Any] = {}
                for child in def_el:
                    tag = cls._strip_ns(child.tag)
                    ss_row[cls._norm(tag)] = (child.text or "").strip() or None
                if ss_row:
                    search_settings_list = [ss_row]

        # USER: <user-identifier-associations>
        user_identifiers_list: List[Dict] = []
        uia_el = root.find(_T("user-identifier-associations"))
        if uia_el is not None:
            for uia in uia_el.findall(_T("user-identifier-association")):
                user_identifiers_list.append({
                    "scheme":   uia.get("scheme", "") or None,
                    "status":   uia.get("status", "") or None,
                    "decision": uia.get("decision", "") or None,
                    "value":    (uia.text or "").strip() or None,
                })

        # ── Attach all list children to entity ────────────────────────────────
        if records_list:
            entity["records"] = records_list
        if fields_list:
            entity["fields"] = fields_list

        # Native-field list children (address, person, keywords, identifiers, links, funding, subtypes)
        for k, v in native_lists.items():
            if k in entity and isinstance(entity[k], list):
                entity[k] = entity[k] + v
            else:
                entity[k] = v

        # Merge all-labels keywords with any keywords from native fields
        if al_keywords:
            existing = entity.get("keywords", [])
            entity["keywords"] = (existing if isinstance(existing, list) else []) + al_keywords

        if group_properties_list:
            entity["group_properties"] = group_properties_list
        if group_membership_list:
            entity["group_membership"] = group_membership_list
        if journal_list:
            entity["journal"] = journal_list
        if org_data_list:
            entity["org_data"] = org_data_list
        if search_settings_list:
            entity["search_settings"] = search_settings_list
        if user_identifiers_list:
            entity["identifiers"] = user_identifiers_list

        return {
            root_key: {
                "engine":  first_source_name or "symplectic",
                "pubdate": last_modified,
                entity_key: entity,
            }
        }

    # ── private helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _strip_ns(tag: str) -> str:
        """Strip XML namespace: {http://...}localname → localname"""
        return tag.split("}")[-1] if "}" in tag else tag

    @staticmethod
    def _norm(name: str) -> str:
        """
        Normalize a field/attribute name:
          - Replace hyphens with underscores
          - Strip leading 'c_' prefix (BU-specific Symplectic field convention)
          - Disambiguate 'type' → 'field_type' to avoid collision with object_type
            (applies to all categories: grant type, activity type, publication type, etc.)
        """
        n = name.replace("-", "_")
        n = n[2:] if n.startswith("c_") else n
        if n == "type":
            n = "field_type"
        return n

    @classmethod
    def _get_field_text(cls, field_el) -> Optional[str]:
        """Extract a simple text representation of any field element for raw field dumps."""
        ftype = field_el.get("type", "")
        ns_map = {
            "text":    lambda e: (e.find(_T("text")), None),
            "integer": lambda e: (e.find(_T("integer")), None),
            "boolean": lambda e: (e.find(_T("boolean")), None),
        }
        if ftype in ns_map:
            el, _ = ns_map[ftype](field_el)
            return (el.text or "").strip() or None if el is not None else None
        if ftype == "date":
            el = field_el.find(_T("date"))
            if el is not None:
                parts = [el.findtext(_T("year")) or "",
                         (el.findtext(_T("month")) or "").zfill(2),
                         (el.findtext(_T("day"))   or "").zfill(2)]
                return "-".join(p for p in parts if p.strip("0")) or None
        if ftype == "money":
            el = field_el.find(_T("money"))
            return (el.text or "").strip() or None if el is not None else None
        if ftype == "organisation":
            org_el = field_el.find(_T("organisation"))
            if org_el is not None:
                name_el = org_el.find(_T("name"))
                return (name_el.text or "").strip() or None if name_el is not None else None
        if ftype == "items":
            items_el = field_el.find(_T("items"))
            if items_el is not None:
                vals = [(i.text or "").strip() for i in items_el.findall(_T("item")) if i.text]
                return ", ".join(vals) or None
        # Default: concatenate all text content
        all_text = " ".join((el.text or "").strip() for el in field_el.iter() if el.text and el.text.strip())
        return all_text.strip() or None

    @classmethod
    def _extract_field(cls, name: str, ftype: str, field_el) -> List[Tuple[str, Any]]:
        """
        Extract one or more (col, val) pairs from an <api:field> element.
        Returns a list to support multi-column types (e.g. money → amount + currency).
        List values signal child table data.
        """
        col = cls._norm(name)

        if ftype == "text":
            el  = field_el.find(_T("text"))
            val = (el.text or "").strip() if el is not None else None
            return [(col, val or None)]

        if ftype == "date":
            el = field_el.find(_T("date"))
            if el is not None:
                day   = (el.findtext(_T("day"))   or "").strip()
                month = (el.findtext(_T("month")) or "").strip()
                year  = (el.findtext(_T("year"))  or "").strip()
                if year and month and day:
                    return [(col, f"{year}-{month.zfill(2)}-{day.zfill(2)}")]
                if year and month:
                    return [(col, f"{year}-{month.zfill(2)}")]
                if year:
                    return [(col, year)]
            return [(col, None)]

        if ftype == "money":
            el = field_el.find(_T("money"))
            if el is not None:
                raw = (el.text or "").strip()
                try:
                    amount = float(raw) if raw else None
                except ValueError:
                    amount = None
                currency = el.get("iso-currency", "") or None
                return [(col, amount), (f"{col}_currency", currency)]
            return [(col, None), (f"{col}_currency", None)]

        if ftype == "organisation":
            org_el = field_el.find(_T("organisation"))
            if org_el is not None:
                name_el = org_el.find(_T("name"))
                val = (name_el.text or "").strip() if name_el is not None else None
                return [(f"{col}_org_name", val or None)]
            return [(f"{col}_org_name", None)]

        if ftype == "address-list":
            addrs_el = field_el.find(_T("addresses"))
            result: List[Dict] = []
            if addrs_el is not None:
                for addr_el in addrs_el.findall(_T("address")):
                    addr: Dict[str, Any] = {
                        "iso_country_code": addr_el.get("iso-country-code", "") or None,
                        "privacy":          addr_el.get("privacy", "") or None,
                    }
                    for line_el in addr_el.findall(_T("line")):
                        line_type = (line_el.get("type", "line") or "line").replace("-", "_")
                        addr[line_type] = (line_el.text or "").strip() or None
                    result.append(addr)
            return [("address", result)]

        if ftype == "person-list":
            persons_el = field_el.find(_T("people"))
            result: List[Dict] = []
            if persons_el is not None:
                for person_el in persons_el.findall(_T("person")):
                    person: Dict[str, Any] = {}
                    for sub in person_el:
                        sub_tag = cls._strip_ns(sub.tag)
                        # Skip complex nested sub-elements; capture simple scalars
                        if sub_tag in ("identifiers", "addresses", "links", "separate-first-names"):
                            continue
                        person[cls._norm(sub_tag)] = (sub.text or "").strip() or None
                    # Capture first identifier per person
                    ids_el = person_el.find(_T("identifiers"))
                    if ids_el is not None:
                        for id_el in ids_el.findall(_T("identifier")):
                            scheme = (id_el.get("scheme", "") or "identifier").replace("-", "_")
                            person[f"id_{scheme}"] = (id_el.text or "").strip() or None
                    if person:
                        result.append(person)
            return [("person", result)]

        if ftype == "keywords":
            kws_el = field_el.find(_T("keywords"))
            result: List[Dict] = []
            if kws_el is not None:
                for kw_el in kws_el.findall(_T("keyword")):
                    result.append({
                        "keyword": (kw_el.text or "").strip() or None,
                        "scheme":  kw_el.get("scheme", "") or None,
                    })
            return [("keywords", result)]

        if ftype == "identifiers":
            ids_el = field_el.find(_T("identifiers"))
            result: List[Dict] = []
            if ids_el is not None:
                for id_el in ids_el.findall(_T("identifier")):
                    result.append({
                        "scheme": id_el.get("scheme", "") or None,
                        "value":  (id_el.text or "").strip() or None,
                    })
            return [("identifiers", result)]

        if ftype == "links":
            links_el = field_el.find(_T("links"))
            result: List[Dict] = []
            if links_el is not None:
                for link_el in links_el.findall(_T("link")):
                    result.append({
                        "href": link_el.get("href", "") or None,
                        "type": (link_el.get("type", "") or "").replace("-", "_") or None,
                    })
            return [("links", result)]

        if ftype == "funding-acknowledgements":
            result: List[Dict] = []
            for grant_el in field_el.findall(f".//{_T('grant')}"):
                grant_row: Dict[str, Any] = {
                    "funding_grant_id": (grant_el.findtext(_T("grant-id")) or "").strip() or None,
                    "organisation":     (grant_el.findtext(_T("organisation")) or "").strip() or None,
                }
                oi_el = grant_el.find(_T("org-identifiers"))
                if oi_el is not None:
                    for id_el in oi_el.findall(_T("identifier")):
                        scheme = (id_el.get("scheme", "") or "identifier").replace("-", "_")
                        grant_row[f"org_id_{scheme}"] = (id_el.text or "").strip() or None
                result.append(grant_row)
            return [("funding", result)]

        if ftype == "integer":
            el = field_el.find(_T("integer"))
            val = (el.text or "").strip() if el is not None else None
            try:
                return [(col, int(val))] if val else [(col, None)]
            except (ValueError, TypeError):
                return [(col, None)]

        if ftype == "boolean":
            el = field_el.find(_T("boolean"))
            val = (el.text or "").strip().lower() if el is not None else None
            if val == "true":  return [(col, True)]
            if val == "false": return [(col, False)]
            return [(col, None)]

        if ftype == "items":
            items_el = field_el.find(_T("items"))
            result: List[Dict] = []
            if items_el is not None:
                for item_el in items_el.findall(_T("item")):
                    val = (item_el.text or "").strip()
                    if val:
                        result.append({"field_name": col, "value": val})
            # All items-type fields are merged into entity["subtypes"]
            return [("subtypes", result)]

        if ftype == "pagination":
            pg_el = field_el.find(_T("pagination"))
            if pg_el is not None:
                begin = (pg_el.findtext(_T("begin-page")) or "").strip() or None
                end   = (pg_el.findtext(_T("end-page"))   or "").strip() or None
                return [(f"{col}_begin", begin), (f"{col}_end", end)]
            return [(f"{col}_begin", None), (f"{col}_end", None)]

        # Fallback: try to read any text content
        el  = field_el.find(_T("text"))
        val = (el.text or "").strip() if el is not None else None
        return [(col, val or None)]
