/** acs_fields -- external table definition for geocoding metadata

Copyright (c) 2016 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

Refs:
   
 - 12 External Tables Concepts
   Oracle Database Online Documentation 11g Release 1 (11.1) / Data Warehousing and Business Intelligence
   https://docs.oracle.com/cd/B28359_01/server.111/b28319/et_concepts.htm

-- https://www.packtpub.com/books/content/creating-external-tables-your-oracle-10g11g-database
-- CREATE DIRECTORY EXTTABDIR AS '/home/oracle/external_table_dest';

alter session set current_schema = MPC;
*/




drop directory GEO_CENSUS_STAGE;
-- all data files symlinked into the same directory, since
-- oracle won't descend into subdirectories
CREATE DIRECTORY GEO_CENSUS_STAGE AS '/d1/geo-census/mn-census-data';
grant read, write on directory GEO_CENSUS_STAGE to mpc;

drop table MPC.acs_fields;
create table MPC.acs_fields (
  data_type          varchar2(1),
  dataset_code       varchar2(80),
  table_source_code  varchar2(80),
  table_label        varchar2(255),
  table_universe     varchar2(255),
  table_sequence     integer,
  variable_sequence  integer,
  variable_code      varchar2(80),
  variable_label     varchar2(255),
  start_column       integer,
  width              integer,
  implied_decimal_places  integer,
  multiplier         integer,
  appears_in_extracts integer -- TODO: boolean check
)
organization external (
  type oracle_loader
  default directory geo_census_stage
  access parameters (
    records delimited by newline
    skip 1
    fields terminated by ',' optionally enclosed by '"'
    MISSING FIELD VALUES ARE NULL
    (
      data_type,
      dataset_code,
      table_source_code,
      table_label,
      table_universe,
      table_sequence,
      variable_sequence,
      variable_code,
      variable_label,
      start_column,
      width,
      implied_decimal_places,
      multiplier,
      appears_in_extracts
    )
  )
  location ('index_of_data_fields__acs_20135a.csv')
);

-- select * from acs_fields;

create directory staging_tools as '/d1/geo-census/tools';
grant read, execute on directory STAGING_TOOLS to mpc;

drop table mpc.acs_zcta_200;

create table mpc.acs_zcta_200 (
  FILEID VARCHAR2(6),
  STUSAB VARCHAR2(2),
  SUMLEVEL VARCHAR2(3),
  COMPONENT VARCHAR2(2),
  LOGRECNO INTEGER
) organization external (
  type oracle_loader
  default directory geo_census_stage
  access parameters (
    records delimited by newline
    preprocessor staging_tools:'zcat.sh'
    fields lrtrim
    (
      FILEID position (1-6) char(6),
      STUSAB position (7-2) char(2),
      SUMLEVEL position (9-3) char(3),
      COMPONENT position (12-2) char(2),
      LOGRECNO position (14-7) char(7) NULLIF LOGRECNO = '.'
    )
  )
  location ('acs_20135a_zcta_860.dat.gz')
)
;

select * from mpc.ACS_ZCTA_200
;


-- groups from https://en.wikipedia.org/wiki/Household_income_in_the_United_States#/media/File:US_county_household_median_income_2012.png
create materialized view household_income as
select zcta5, UHD001 dollars
     , case
       when UHD001 < 35000 then 0
       when UHD001 < 42000 then 35000
       when UHD001 < 52000 then 42000
       when UHD001 < 59000 then 52000
       when UHD001 < 74000 then 59000
       else                     74000
       end dollar_group
from mpc.acs_zcta_2
;


select 'Over' || to_char(hi.dollar_group, '$999,999') income_group, sex_cd, count(patient_num)
from nightherondata.patient_dimension pat
join household_income hi on substr(zip_cd, 1, 5) = hi.zcta5
group by sex_cd, hi.dollar_group
having count(patient_num) > 100
order by dollar_group, sex_cd
;


select *
FROM I2B2CENSUS.ACS_20135A_GEOCODED_KUMC
;

-- https://informatics.gpcnetwork.org/trac/Project/ticket/140#comment:9
select /*distinct*/ pat.add_line_1 || ' ' || pat.add_line_2 address,
 pat.city, zs.name state, pat.zip 
from clarity.patient pat 
left join clarity.zc_state zs on zs.state_c = pat.state_c;
;

drop materialized view patient_dimension_geo;
create materialized view patient_dimension_geo as
select pat.pat_id, addr.id
     -- , addr.address
     , addr.city
     , co_name
     , addr.st_name
     , addr.zip
     --, x, y
     , block_id, tract_id 
from clarity.patient pat
left join clarity.zc_state zst on pat.state_c = zst.state_c
left join I2B2CENSUS.ACS_20135A_GEOCODED_KUMC addr
  on addr.zip = pat.zip
 and addr.city = pat.city
 and addr.address = pat.add_line_1 || ' ' || pat.add_line_2
 and addr.st_name = zst.name
;


select qty, count(*) from (
select count(*) qty, pat_id
from patient_dimension_geo
group by pat_id
) group by qty order by qty
;


select * from (
select count(*) qty, pat_id
from patient_dimension_geo
group by pat_id
having count(*) > 1
) dups join patient_dimension_geo g on dups.pat_id = g.pat_id
join clarity.patient cp on cp.pat_id = dups.pat_id
order by qty desc, g.id;


