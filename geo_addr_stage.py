'''geo_addr_stage -- stage address geocoding data from MPC

'''

from collections import namedtuple
from xml.etree import ElementTree as ET
import pkg_resources as pkg


class Crosswalk(object):
    '''Crosswalk from addresses to census tract etc.

    >>> cx = Crosswalk.from_meta_file(Crosswalk._test_meta())
    >>> cx
    Crosswalk(nhgis_Block_2010_wCodes)

    >>> set(['GTRACT', 'GZCTA5', 'GBLOCK']) - set(cx.header())
    set([])
    >>> len(cx.header())
    24

    >>> print cx.as_table().ddl()
    '''

    def __init__(self, metadata):
        self.metadata = metadata

    @classmethod
    def from_meta_file(cls, fp):
        return cls(ET.parse(fp).getroot())

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           self.name())

    def name(self):
        return self.metadata.find(
            './spdoinfo/ptvctinf/esriterm').attrib['Name']

    def header(self):
        return [attr.name for attr in self.attrs()]

    def attrs(self):
        return [GeoAttr.from_xml(a)
                for a in self.metadata.findall('./eainfo/detailed/attr')]

    def as_table(self):
        return SQLTable(self.name(),
                        [attr.column_def() for attr in self.attrs()])

    @classmethod
    def _test_meta(cls,
                   crosswalk_meta='block_crosswalk.txt.xml'):
        return pkg.resource_stream(__name__, crosswalk_meta)


class GeoAttr(object):
    @classmethod
    def from_xml(cls, elt):
        name = elt.find('attrlabl').text
        attrtype = elt.find('attrtype').text
        if attrtype == 'OID':
            return OIDAttr(name)
        elif attrtype == 'String':
            width = int(elt.find('attwidth').text)
            return StringAttr(name, width)
        else:
            raise ValueError((attrtype, elt))


class OIDAttr(GeoAttr,
              namedtuple('OIDAttr', 'name')):
    def column_def(self):
        return (self.name, 'INT')


class StringAttr(GeoAttr,
                 namedtuple('StringAttr', 'name width')):
    def column_def(self):
        return (self.name, 'VARCHAR2(%d)' % self.width)


class SQLTable(namedtuple('SQLTable', 'name columns')):
    def ddl(self):
        coldefs = '\n, '.join('%s %s' % (col_name, typespec)
                            for (col_name, typespec) in self.columns)
        return 'create table %s (\n%s\n)' % (self.name, coldefs)
