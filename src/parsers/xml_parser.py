"""
XML Parser
Parses XML payloads into Python dicts matching the same structure
the DenormalizationEngine expects from JSON parsing:
  { "<root_key>": { "engine": ..., "<entity_key>": { ...scalars... }, ... } }

Supports:
  - Nested elements as child dicts
  - Repeated elements with the same tag as lists (→ child tables)
  - XML attributes merged as fields
  - Namespace stripping (e.g. {http://ns}tag → tag)
  - CDATA / text content on leaf nodes
  - Nil wrappers (nil="true" attribute → None)
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, Union

logger = logging.getLogger(__name__)


class XMLParser:

    @staticmethod
    def parse_payload(xml_str: str) -> Dict[str, Any]:
        """
        Parse an XML string into a nested dictionary.

        The returned dict has the root element tag as its only key:
            { "mycvs": { "engine": ..., "mycv": { ... } } }

        This mirrors the JSON structure:
            { "mycvs": { "engine": ..., "mycv": { ... } } }

        Args:
            xml_str: Raw XML string from the `data` column

        Returns:
            Nested dict with root tag as top-level key
        """
        try:
            root = ET.fromstring(xml_str.strip())
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            raise

        tag = XMLParser._strip_ns(root.tag)
        return {tag: XMLParser._element_to_dict(root)}

    @staticmethod
    def _strip_ns(tag: str) -> str:
        """Strip XML namespace prefix: {http://...}localname → localname"""
        return tag.split('}')[-1] if '}' in tag else tag

    @staticmethod
    def _element_to_dict(element) -> Union[str, None, Dict[str, Any]]:
        """
        Recursively convert an XML element to a Python value.

        Rules:
          - Leaf with text only        → str (or None if empty)
          - nil="true" attribute        → None
          - Element with attributes     → dict with attr keys + child keys
          - Repeated child tags         → list of dicts (→ child table)
          - Single child tag            → dict
        """
        # nil="true" → None
        if element.attrib.get('nil', '').lower() == 'true':
            return None

        result: Dict[str, Any] = {}

        # Merge XML attributes as plain fields (skip nil, already handled)
        for attr_name, attr_val in element.attrib.items():
            clean = XMLParser._strip_ns(attr_name)
            if clean != 'nil':
                result[clean] = attr_val

        children = list(element)

        if not children:
            # Leaf node
            text = (element.text or '').strip()
            if result:
                # Has attributes + text
                if text:
                    result['_value'] = text
                return result
            return text if text else None

        # Has child elements — build child dict, handle repeated tags as lists
        child_accum: Dict[str, Any] = {}
        for child in children:
            child_tag = XMLParser._strip_ns(child.tag)
            child_val = XMLParser._element_to_dict(child)
            if child_tag in child_accum:
                existing = child_accum[child_tag]
                if isinstance(existing, list):
                    existing.append(child_val)
                else:
                    child_accum[child_tag] = [existing, child_val]
            else:
                child_accum[child_tag] = child_val

        result.update(child_accum)

        # Also include text content of the element itself (mixed content)
        text = (element.text or '').strip()
        if text:
            result['_value'] = text

        return result
