"""PowerPoint slide operations shared by report renderers."""
from copy import deepcopy

from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.oxml.ns import qn


def clone_template_slide(deck, source):
    """Copy slide-local assets while retaining the original layout/master."""
    slide = deck.slides.add_slide(source.slide_layout)
    for shape in list(slide.shapes):
        slide.shapes._spTree.remove(shape.element)
    relation_ids = {}
    for relationship in source.part.rels.values():
        if relationship.reltype == RT.NOTES_SLIDE:
            continue
        if relationship.is_external:
            rid = slide.part.rels.get_or_add_ext_rel(relationship.reltype, relationship.target_ref)
        else:
            rid = slide.part.rels.get_or_add(relationship.reltype, relationship.target_part)
        relation_ids[relationship.rId] = rid
    elements = [deepcopy(shape.element) for shape in source.shapes]
    background = source._element.cSld.find(qn("p:bg"))
    if background is not None:
        elements.append(deepcopy(background))
    for element in elements:
        for node in element.iter():
            for attr, value in list(node.attrib.items()):
                if attr.startswith("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"):
                    node.set(attr, relation_ids[value])
        if element.tag == qn("p:bg"):
            slide._element.cSld.insert(0, element)
        else:
            slide.shapes._spTree.insert_element_before(element, "p:extLst")
    return slide


def drop_slide(deck, index: int) -> None:
    """Remove the slide entry and its deck relationship."""
    slide_id = deck.slides._sldIdLst[index]
    deck.part.drop_rel(slide_id.rId)
    deck.slides._sldIdLst.remove(slide_id)
