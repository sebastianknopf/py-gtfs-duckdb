from lxml import etree
from lxml.etree import CDATA

_CONST_ATTRIB_INDICATOR = '#'

def _build_xml_element(parent, d, wrap_lists):    
    if isinstance(d, dict):
        attrib = {}
        content = {}

        for key, value in d.items():
            if key == '$name' or key == '$text':
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
                            
                            child = etree.SubElement(wrap, item['$name'])
                            _build_xml_element(child, item, wrap_lists)
                    else:
                        for item in value:
                            child = etree.SubElement(parent, key)
                            _build_xml_element(child, item, wrap_lists)
                else:
                    if isinstance(value, dict) and '$name' in value:
                        name = value['$name']
                    else: 
                        name = key

                    child = etree.SubElement(parent, name)

                    if isinstance(value, dict) and '$text' in value:
                        child.text = value['$text']
                        _build_xml_element(child, value, wrap_lists)
                    else:
                        _build_xml_element(child, value, wrap_lists)
    else:
        d = str(d)
        if d.startswith('<![CDATA['):
            d = d.replace('<![CDATA[', '').replace(']]>', '')
            parent.text = CDATA(d)
        else:
            parent.text = str(d)

def dict2xml(root, d, pretty_print=True, xml_declaration=True, wrap_lists=False) -> str:
    root = etree.Element(root)
    _build_xml_element(root, d, wrap_lists)

    if pretty_print:
        etree.indent(root, space="    ")

    xml_str: str = etree.tostring(
        root, 
        pretty_print=pretty_print, 
        xml_declaration=xml_declaration, 
        encoding='UTF-8'
    )

    return xml_str