/*- collect new addresses from patient_dimension, and
  - convert to DeGauss input format
  GeGauss: https://degauss.org/
*/

whenever sqlerror continue;
drop table address_all;
drop table new_address_degauss_input;
whenever sqlerror exit;

select * from nightherondata.patient_dimension where 1 = 0;
select * from clarity.zc_state where 1 = 0;

-- collect all patients' addresses
create table address_all as
--Note: there could be duplicates due to duplicate state name
-- e.g. select * from clarity.zc_state where TITLE = 'CHIHUAHUA'
with zc_state_undup as (
select * from (
    select TITLE, ABBR, row_number() over (partition by TITLE order by ABBR) rn
    from clarity.zc_state)
where rn = 1
)
    ,pat_address as (
select distinct
       pat.PATIENT_NUM,
       replace(trim(both ' ' from (UPPER(pat.add_line_1) || ' ' || UPPER(pat.add_line_2))),'((BAD ADDR)|(RETURN MAIL)|(RETURNED MAIL))+','') ADDRESS_STREET, /*leading and trailing space issue*/
       UPPER(pat.city) ADDRESS_CITY,
       zc.abbr ADDRESS_STATE,
       case when length(pat.zip_cd) < 5 then LPAD(pat.zip_cd,5,'0')
            when pat.zip_cd is null then '99999'
            else pat.zip_cd
       end as ADDRESS_ZIP9, 
       case when regexp_like((UPPER(pat.add_line_1) || ' ' || UPPER(pat.add_line_2)),'((UNKNOWN)|(UNK )|( UN )|(N\\A)|(99999))+','i') then 'UN' 
            when regexp_like((UPPER(pat.add_line_1) || ' ' || UPPER(pat.add_line_2)),'((BAD ADDR)|(RETURN MAIL)|(RETURNED MAIL))+','i') then 'OL'
            else 'HO' 
       end as ADDRESS_USE,
       case when regexp_like((UPPER(pat.add_line_1) || ' ' || UPPER(pat.add_line_2)),'((P\.O\.)|(PO BOX))+','i') then 'PO'
            else 'BO' 
       end as ADDRESS_TYPE
from nightherondata.patient_dimension pat
left join zc_state_undup zc on UPPER(pat.state_cd) = zc.title
)
select src.PATIENT_NUM
      ,src.ADDRESS_STREET
      ,src.ADDRESS_CITY
      ,src.ADDRESS_STATE
      ,src.ADDRESS_ZIP9
      ,trim(both ' ' from (src.ADDRESS_STREET || ' ' || src.ADDRESS_CITY || ' ' || src.ADDRESS_STATE || ' ' || src.ADDRESS_ZIP9)) RAW_ADDRESS
      ,substr(src.ADDRESS_ZIP9,1,5) ADDRESS_ZIP5
from pat_address src
;

-- filter our new addresses
create or replace view address_update as
with multi_match as (
select src.ADDRESS_STREET
      ,src.ADDRESS_CITY
      ,src.ADDRESS_STATE
      ,src.ADDRESS_ZIP9
      ,trim(both ' ' from (src.ADDRESS_STREET || ' ' || src.ADDRESS_CITY || ' ' || src.ADDRESS_STATE || ' ' || src.ADDRESS_ZIP9)) RAW_ADDRESS
      ,mp.fips11
      ,mp.locator
      ,mp.score
      ,mp.lon
      ,mp.lat
from address_all src
left join address_mapped mp
on trim(both ' ' from (src.ADDRESS_STREET || ' ' || src.ADDRESS_CITY || ' ' || src.ADDRESS_STATE || ' ' || src.ADDRESS_ZIP9)) = mp.RAW_ADDRESS
)
   ,multi_match_rk as (
select RAW_ADDRESS
      ,ADDRESS_STREET
      ,ADDRESS_CITY
      ,ADDRESS_STATE
      ,ADDRESS_ZIP9
      ,fips11
      ,LOCATOR
      ,SCORE
      ,LON
      ,LAT
      ,row_number() over (partition by RAW_ADDRESS, ADDRESS_CITY, ADDRESS_STATE, ADDRESS_ZIP9 order by SCORE asc) rn
from multi_match
)
select RAW_ADDRESS
      ,ADDRESS_STREET
      ,ADDRESS_CITY
      ,ADDRESS_STATE
      ,ADDRESS_ZIP9
      ,fips11 
      ,LOCATOR
      ,SCORE
      ,LON
      ,LAT
from multi_match_rk
where rn = 1
;

--convert into DeGauss format: id, address
create table new_address_degauss_input as
select distinct
       dense_rank(RAW_ADDRESS) as id
      ,trim(both ' ' from RAW_ADDRESS) address
from address_update
;

/**********************************************************************************************************
--Input files for geocoding: new_address_degauss_input
-- run DeGAUSS docker with new_address_degauss_input as input file
-- con: need to install docker, the set up is not as straightforward
-- pro: very versaltile in dealing  with "no match" by implementing some approximation 

--Output files: new_address_degauss_output, upload back
--required columns:
--     address
--     street
--     city
--     state
--     zip
--     tract
--     precision
--     score
--     lon
--     lat
**********************************************************************************************************/

