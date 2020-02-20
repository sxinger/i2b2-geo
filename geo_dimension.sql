/*****************************************/
/*construct a geo_dimension in EAV format*/
/* Staging                               */
/*****************************************/

/*assumption 1: addresses have all been geocoded*/
select * from address_mapped;

/*assumption 2: acs tables have been profiled*/
select * from ACS_meta;

/*assumption 3: RUCA mapping have been uploaded (census tract)*/
select * from ruca_map_cur;


/*Step 1: collect and select acs table/variable of interests and link to staged ACS tables*/
drop table acs_metadata purge;
create table acs_metadata as
select acs.VARIABLE_CODE,
       atc.TABLE_NAME,
       case when atc.TABLE_NAME like '%COUNTY%' then 'c'
            when atc.TABLE_NAME like '%TRACT%' then 't'
            when atc.TABLE_NAME like '%ZCTA%' then 'z'
            when atc.TABLE_NAME like '%BLCK%GRP' then 'b'
            else 'o'
       end as GEO_LEVEL,
       meta.TABLE_ID,
       meta.TABLE_SUBJECT,
       meta.TABLE_TITLE,
--       acs.TABLE_LABEL,
       meta.TABLE_UNIVERSE,
--       acs.TABLE_UNIVERSE TABLE_UNIVERSE2,
       meta.LINE,
       meta.UNIQUEID,
       meta.STUB,  -- "Total" suggests denominator field
       meta.STUB2,
       meta.SUMMARY_TYPE,
       meta.SEX,
       meta.RACE,
       meta.ETHNICITY,
       meta.AGE,
       meta.FAMTYPE
from mpc.acs_fields acs
join all_tab_columns atc
on atc.COLUMN_NAME = acs.VARIABLE_CODE
join ACS_meta meta
on acs.TABLE_SOURCE_CODE = meta.TABLE_ID and
   acs.VARIABLE_SEQUENCE = meta.LINE and
   -- subject selection
   (meta.TABLE_SUBJECT in ('15','17','23','27') or 
    (meta.TABLE_SUBJECT in ('19','20') and meta.SUMMARY_TYPE= 'num')) and
   (atc.TABLE_NAME like '%COUNTY%' or
    atc.TABLE_NAME like '%ZCTA%' or
    atc.TABLE_NAME like '%TRACT%')
;


/*Step 2: collect RUCA, ACS variables*/
-- initialize geo_dimension
drop table geo_dimension purge;
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
join ruca_map_cur ruca
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
          listagg (variable_code || ' as ''' || variable_code || '''',',')
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

--  4,179 seconds


select count(*) from geo_dimension; 

-- check for duplicates
select geo_id, acs_var, count(*)
from geo_dimension
group by geo_id, acs_var
having count(*) > 1;


