'''geo_addr_stage -- stage address geocoding data from MPC

'''

from xml.etree import ElementTree as ET
import pkg_resources as pkg


class Crosswalk(object):
    '''Crosswalk from addresses to census tract etc.

    >>> Crosswalk.from_meta_file(Crosswalk._test_meta())
    ... #doctest: +ELLIPSIS
    OID,GSTATE,GCOUNTY,GTRACT,...
    '''

    def __init__(self, metadata):
        self.metadata = metadata

    @classmethod
    def from_meta_file(cls, fp):
        return cls(ET.parse(fp).getroot())

    def __repr__(self):
        return self.header()

    def header(self):
        return ','.join(attr.find('attrlabl').text for attr in self.attrs())

    def attrs(self):
        return self.metadata.findall('./eainfo/detailed/attr')

    @classmethod
    def _test_meta(cls,
                   crosswalk_meta='block_crosswalk.txt.xml'):
        return pkg.resource_stream(__name__, crosswalk_meta)
