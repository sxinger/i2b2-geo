/*****************************************/
/*construct a geo_dimension in EAV format*/
/*Staging                                */
/*****************************************/

whenever sqlerror continue;
drop table acs_metadata;
drop table geo_dimension;
whenever sqlerror exit;

/*assumption 1: new addresses have been geocoded and added to address_mapped
*/
select * from address_mapped where 1 = 0;

/*assumption 2: the metadata table for the staged ACS schemas/tables is available
*/
select * from mpc.acs_fields where 1 = 0;

/*assumption 3: acs tables have been profiled and uploaded
  -- generated using get_ACS_ont.R 
  -- a reusable mapping is saved in ./curated_data
*/
--drop table acs_meta; --if need to be updated
select * from acs_meta where 1 = 0;

/*assumption 4: RUCA mapping have been uploaded (census tract)
  uploaded from external data source: 
  -- https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca2010revised.xlsx?v=7723.7
  -- downloaded and saved in ./curated_data
*/
select * from ruca_map where 1 = 0;

/*Step 1: collect and select acs table/variable of interests and link to staged ACS tables*/
create table acs_metadata as
select atc.TABLE_NAME,
       case when atc.TABLE_NAME like '%COUNTY%' then 'c'
            when atc.TABLE_NAME like '%TRACT%' then 't'
            when atc.TABLE_NAME like '%ZCTA%' then 'z'
            when atc.TABLE_NAME like '%BLCK%GRP' then 'b'
            else 'o'
       end as GEO_LEVEL,
       acs.VARIABLE_CODE,
       meta.TABLE_SUBJECT,
       meta.SUBJECTGRP,
       meta.SUBJECTNAME,
       meta.NODE_POS,
       meta.TABLE_ID,
       meta.LINE,
       meta.UNIQUEID,
       meta.SUMMARY_TYPE,
       meta.SEX,
       meta.RACE,
       meta.ETHNICITY,
       meta.AGE,
       meta.FAMTYPE,
       meta.HLEVEL,
       meta.VARIABLE_LABEL
from mpc.acs_fields acs  -- attach variable_code
join all_tab_columns atc -- attach staged table name
on atc.COLUMN_NAME = acs.VARIABLE_CODE
join ACS_meta meta
on acs.TABLE_SOURCE_CODE = meta.TABLE_ID and
   acs.VARIABLE_SEQUENCE = meta.LINE and
   -- subject pre-selection
   (
    (meta.TABLE_SUBJECT in ('15','17') and SEX='NI' and RACE='NI' and ETHNICITY='NI' and AGE='NI') -- education, poverty
    or 
    (meta.TABLE_SUBJECT in ('19') and meta.SUMMARY_TYPE= 'num') -- income
    ) 
    and
   (
    atc.TABLE_NAME like '%ZCTA%' -- ZCTA level
    or
    atc.TABLE_NAME like '%TRACT%' -- census tract level
    )
    and
    meta.VARIABLE_LABEL not like '%AGGREGATE%' -- remove geographical-level aggregates
;

--select TABLE_SUBJECT, SUBJECTNAME, count(*) from acs_metadata
--group by TABLE_SUBJECT, SUBJECTNAME;


/*Step 2: collect RUCA, ACS variables*/
-- initialize geo_dimension
create table geo_dimension (
 GEO_ID   varchar2(200),
 ACS_VAR  varchar2(50),
 ACS_VAL  number(38,2)
)
;

--add RUCA codes (at census tract level)
insert into geo_dimension
select distinct
       (ref.FIPS11 || '-' ||ref.ADDRESS_ZIP9)
      ,'RUCA'
      ,ruca.ruca1
from address_mapped ref
join ruca_map ruca
on ref.FIPS11 = ruca.FIPS11
;
commit;

--add selected ACS variables at county, zip, and tract level
--https://stackoverflow.com/questions/45637308/how-to-unpivot-with-dynamic-columns-oracle
declare
   sql_stmt clob;
   
   cursor acs_tbl is
   select table_name, table_id, geo_level,
          listagg ('acs_tbl.' || variable_code || ' as ' || variable_code, ',')
            within group (order by variable_code) subquery,
          listagg (variable_code || ' as ''' || variable_code || '_' || geo_level || '''',',')
            within group (order by variable_code) pivot_clause
  from acs_metadata
--  where rownum <=3 /*uncomment this line for testing*/
  group by table_name, table_id, geo_level;

begin
  for rec in acs_tbl
  loop
  --county
  if rec.geo_level = 'c' then
    sql_stmt := 'INSERT INTO /*+ APPEND*/ geo_dimension'
              || ' SELECT * FROM ('
              || ' SELECT distinct (adm.FIPS11 || ''-'' || adm.ADDRESS_ZIP9) geo_id,' || rec.subquery 
              || ' FROM MPC.' || rec.TABLE_NAME || ' acs_tbl'
              || ' JOIN address_mapped adm ON substr(adm.FIPS11,1,5) = (acs_tbl.STATE || acs_tbl.COUNTY)'
              || ' ) UNPIVOT EXCLUDE NULLS ('
              || '  ACS_VAL for ACS_VAR in (' || rec.pivot_clause || '))'; 
   
   -- zip
   elsif rec.geo_level = 'z' then
    sql_stmt := 'INSERT INTO /*+ APPEND*/ geo_dimension'
              || ' SELECT * FROM ('
              || ' SELECT distinct (adm.FIPS11 || ''-'' || adm.ADDRESS_ZIP9) geo_id,' || rec.subquery 
              || ' FROM MPC.' || rec.TABLE_NAME || ' acs_tbl'
              || ' JOIN address_mapped adm ON substr(adm.ADDRESS_ZIP9,1,5) = acs_tbl.ZCTA5'
              || ' ) UNPIVOT EXCLUDE NULLS ('
              || '  ACS_VAL for ACS_VAR in (' || rec.pivot_clause || '))'; 
  -- tract
  else
    sql_stmt := 'INSERT INTO /*+ APPEND*/ geo_dimension'
              || ' SELECT * FROM ('
              || ' SELECT distinct (adm.FIPS11 || ''-'' || adm.ADDRESS_ZIP9) geo_id,' || rec.subquery 
              || ' FROM MPC.' || rec.TABLE_NAME || ' acs_tbl'
              || ' JOIN address_mapped adm ON adm.FIPS11 = (acs_tbl.STATE || acs_tbl.COUNTY || acs_tbl.TRACT)'
              || ' ) UNPIVOT EXCLUDE NULLS ('
              || '  ACS_VAL for ACS_VAR in (' || rec.pivot_clause || '))'; 
  end if;
  execute immediate sql_stmt;
  commit;
  end loop;
end;
-- 2,686 seconds

--select count(distinct geo_id) from geo_dimension;
----39,236
--
--select count(*) from geo_dimension; 
---- 12,174,157
--
---- check for duplicates
--select geo_id, acs_var, count(*)
--from geo_dimension
--group by geo_id, acs_var
--having count(*) > 1;


