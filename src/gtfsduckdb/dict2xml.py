from lxml import etree
from lxml.etree import CDATA
from lxml.etree import QName

_CONST_ATTRIB_INDICATOR = '#'

def _build_xml_element(parent, d, wrap_lists, nsmap):    
    if isinstance(d, dict):
        attrib = {}
        content = {}

        for key, value in d.items():
            if key == '$name' or key == '$text' or key == '$ns':
                pass
            elif key.startswith(_CONST_ATTRIB_INDICATOR):
                attrib[key.replace(_CONST_ATTRIB_INDICATOR, '')] = str(value)
            else:
                content[key.replace(_CONST_ATTRIB_INDICATOR, '')] = value

        parent.attrib.update(attrib)

        if parent.text is None:
            for key, value in content.items():
                if isinstance(value, list):
                    if wrap_lists:
                        wrap = etree.SubElement(parent, key)
                        for item in value:
                            if not '$name' in item:
                                raise RuntimeError('wrapped list element does not contain required key $name')
                            
                            name = item['$name']

                            if '$ns' in item and item['$ns'] in nsmap:
                                ns_url: str = nsmap[item['$ns']]
                                name = QName(f"{{{ns_url}}}{name}")

                            child = etree.SubElement(wrap, name)
                            _build_xml_element(child, item, wrap_lists, nsmap)
                    else:
                        for item in value:
                            name = item['$name']

                            if '$ns' in item and item['$ns'] in nsmap:
                                ns_url: str = nsmap[item['$ns']]
                                name = QName(f"{{{ns_url}}}{name}")

                            child = etree.SubElement(parent, name)
                            _build_xml_element(child, item, wrap_lists, nsmap)
                else:
                    if isinstance(value, dict) and '$name' in value:
                        name = value['$name']
                    else: 
                        name = key

                    if isinstance(value, dict) and '$ns' in value and value['$ns'] in nsmap:
                        ns_url: str = nsmap[value['$ns']]
                        name = QName(f"{{{ns_url}}}{name}")

                    child = etree.SubElement(parent, name)

                    if isinstance(value, dict) and '$text' in value:
                        text: str = value['$text']
                        if text.startswith('<![CDATA['):
                            text = text.replace('<![CDATA[', '').replace(']]>', '')
                            child.text = CDATA(text)
                        else:
                            child.text = str(text)

                        _build_xml_element(child, value, wrap_lists, nsmap)
                    else:
                        _build_xml_element(child, value, wrap_lists, nsmap)
    else:
        d = str(d)
        if d.startswith('<![CDATA['):
            d = d.replace('<![CDATA[', '').replace(']]>', '')
            parent.text = CDATA(d)
        else:
            parent.text = str(d)

def dict2xml(root, d, pretty_print=True, xml_declaration=True, wrap_lists=False, nsmap: dict[str, str] = {}) -> str:
    root = etree.Element(root, nsmap=nsmap)
    _build_xml_element(root, d, wrap_lists, nsmap)

    if pretty_print:
        etree.indent(root, space="    ")

    xml_str: str = etree.tostring(
        root, 
        pretty_print=pretty_print, 
        xml_declaration=xml_declaration, 
        encoding='UTF-8'
    )

    return xml_str