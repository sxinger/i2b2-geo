'''geo_addr_stage -- stage address geocoding data from MPC

'''

from collections import namedtuple
from xml.etree import ElementTree as ET
import pkg_resources as pkg


class Geocoded(namedtuple(
        'Geocoded',
        'OID,Join_Count,TARGET_FID,Loc_name,Status,Score,Match_type,'
        'X,Y,Match_addr,DISP_LON,DISP_LAT,SIDE,'
        'ARC_Address,ARC_City,ARC_State,'
        'ARC_Zip,ADDRESS,CITY,STATE,ZIP,New_X,New_Y,ID,BLOCK_ID,'
        'FIPSST,FIPSCO,FIPSSTCO,TRACT_ID,ST_ABRV,CO_NAME,ST_NAME')):
    '''

    >>> print Geocoded.sql_def('t').ddl()
    ... #doctest: +ELLIPSIS
    create table t (
    OID INT
    , Join_Count INT
    , TARGET_FID INT
    , Loc_name VARCHAR2(128)
    ...
    , X NUMERIC
    , Y NUMERIC
    ...
    , ST_NAME VARCHAR2(128)
    )

    >>> import sqlite3
    >>> db0 = sqlite3.connect(':memory:')
    >>> Geocoded.load(db0, 'geocoded', [Geocoded.exemplar()._asdict()])
    >>> c = db0.cursor()
    >>> c.execute('select * from geocoded') and None
    >>> c.fetchmany() == [Geocoded.exemplar()]
    True
    '''
    @classmethod
    def exemplar(cls):
        return cls(OID=-1, Join_Count=2, TARGET_FID=1228303,
                   Loc_name='CityState',
                   Status='M', Score=100, Match_type='A',
                   X=-94.626819999943, Y=39.11352000044769,
                   Match_addr='KANSAS CITY, KS',
                   DISP_LON=None, DISP_LAT=None, SIDE=None,
                   ARC_Address='3901 KUMC RAINBOW BOULEVARD WE WESCOS',
                   ARC_City='KANSAS CITY', ARC_State='Kansas', ARC_Zip=None,
                   ADDRESS='3901 KUMC RAINBOW BOULEVARD WE WESCOS',
                   CITY='KANSAS CITY', STATE='Kansas', ZIP=None,
                   New_X=-94.626819999943, New_Y=39.11352000044769,
                   ID=202090419001002, BLOCK_ID=1002,
                   FIPSST=20, FIPSCO=209, FIPSSTCO=20209, TRACT_ID='041900',
                   ST_ABRV='KS', CO_NAME='Wyandotte', ST_NAME='Kansas')

    @classmethod
    def sql_def(cls, table_name):
        type_of = lambda x: ('INT' if isinstance(x, int) else
                             'VARCHAR2(128)' if isinstance(x, str) else
                             'NUMERIC' if isinstance(x, float) else
                             'VARCHAR2(1)' if x is None else
                             _error(ValueError(x)))
        col_defs = [type_of(x) for x in cls.exemplar()]
        return SQLTable(table_name, zip(cls._fields, col_defs))

    @classmethod
    def load(cls, conn, table_name, data,
             chunk_size=1000):
        tdef = cls.sql_def(table_name)
        cur = conn.cursor()
        cur.execute(tdef.ddl())
        chunk = []

        def flush():
            conn.executemany(tdef.insert_dml(), chunk)
            del chunk[:]

        for record in data:
            if len(chunk) >= chunk_size:
                flush()
            chunk.append(record)

        flush()


def _error(x):
    raise x


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
    ... #doctest: +ELLIPSIS
    create table nhgis_Block_2010_wCodes (
    OID INT
    , GSTATE VARCHAR2(4)
    , GCOUNTY VARCHAR2(8)
    , GTRACT VARCHAR2(14)
    ...
    , GCOUSUB_20 VARCHAR2(10)
    )
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

    def insert_dml(self):
        colnames = ', '.join(n for (n, _) in self.columns)
        params = ', '.join(':' + n for (n, _) in self.columns)
        return 'insert into {name} ({colnames}) values ({params})'.format(
            name=self.name, colnames=colnames, params=params)
