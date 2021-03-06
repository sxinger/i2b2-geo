/*****************************************/
/*construct a geo_dimension in EAV format*/
/*i2b2 metadata                          */
/*****************************************/

select * from acs_metadata;
select * from ruca_map;

/*ACS ontology*/
drop table heron_terms_geo purge;
create table heron_terms_geo as
select
  c_hlevel
, '\i2b2\Demographics\' || path c_fullname
, c_name
, 'N' c_synonym_cd
, c_visualattributes
, c_basecode
, 'patient_num' c_facttablecolumn
, 'geo_dimension' c_tablename
, 'VAR_CODE' c_columnname
, 'T' c_columndatatype -- TODO
, 'LIKE' c_operator -- TODO?
, coalesce(c_basecode, '@') c_dimcode
, '@' m_applied_path
, sysdate update_date
--, ora_hash(path)
from (
------------- RUCA codes ---------------
select 2 c_hlevel
     , 'FA' c_visualattributes
     , null c_basecode
     , 'primary RUCA code' c_name
     , 'RUCA\' path
from dual

union all

--https://depts.washington.edu/uwruca/ruca-codes.php
select 3 c_hlevel
     , 'LA' c_visualattributes
     , ('RUCA:' || ruca1) c_basecode
     , case when ruca1 = 1 then 'Metropolitan area core: primary flow within an Urbanized Area (UA)'
            when ruca1 = 2 then 'Metropolitan area high commuting: primary flow 30% or more to a UA'
            when ruca1 = 3 then 'Metropolitan area low commuting: primary flow 10% to 30% to a UA'
            when ruca1 = 4 then 'Micropolitan area core: primary flow within an Urban Cluster (UC) of 10,000 through 49,999 (large UC)'
            when ruca1 = 5 then 'Micropolitan high commuting: primary flow 30% or more to a large UC'
            when ruca1 = 6 then 'Micropolitan low commuting: primary flow 10% to 30% to a large UC'
            when ruca1 = 7 then 'Small town core: primary flow within an Urban Cluster of 2,500 through 9,999 (small UC)'
            when ruca1 = 8 then 'Small town high commuting: primary flow 30% or more to a small UC'
            when ruca1 = 9 then 'Small town low commuting: primary flow 10% through 29% to a small UC'
            when ruca1 = 10 then 'Rural areas: primary flow to a tract outside a UA or UC (including self)'
            else 'undefined'
       end as c_name
     , ('RUCA\' || ruca1 || '\') path
from ruca_map
group by ruca1

union all

------------- ACS subjects (selective) ---------------
select 2 c_hlevel
     , 'FA' c_visualattributes
     , null c_basecode
     , ('ACS subject about' || SUBJECTGRP || ':' || SUBJECTNAME) c_name
     , (SUBJECTGRP || '-' || SUBJECTNAME || '\') path
from acs_metadata
group by SUBJECTGRP, SUBJECTNAME

union all

select HLEVEL+2 c_hlevel
      ,NODE_POS c_visualattributes
      ,('ACS|' || TABLE_ID || ':' || VARIABLE_CODE || '_' ||GEO_LEVEL) c_basecode
      ,VARIABLE_LABEL c_name
      ,(SUBJECTGRP || '-' || SUBJECTNAME || '\' || VARIABLE_LABEL) path
from acs_metadata
)
;

--select * from heron_terms_geo;

