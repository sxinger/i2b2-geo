/*****************************************/
/*merge with patient_dimension           */
/*DE-ID and ETL                          */
/*****************************************/

whenever sqlerror continue;
drop table geo_num_tbl;
drop table geo_cnt_tbl;
drop table geo_cnt_perc_tbl;
whenever sqlerror exit;

select * from address_mapped where 1 = 0;
select * from geo_dimension where 1 = 0;
select * from acs_metadata where 1 = 0;

/* ==================================================================
 * Separate "cnt" (count) variables from "num" (numerical) variables
 * ==================================================================
 */
create table geo_num_tbl as
select am.patient_num
      ,(g.ACS_VAR || ':' || g.ACS_VAL) concept_cd
--      ,to_number(replace('-','',g.GEO_ID)) instance_num
      ,null nval_num
      ,'@' tval_num
from address_mapped am
join geo_dimension g
on (am.FIPS11 || '-' || am.ADDRESS_ZIP9) = g.GEO_ID and 
   g.ACS_VAR = 'RUCA'
union all
select am.patient_num
      ,('ACS|' || meta.TABLE_ID || ':' || g.ACS_VAR) concept_cd
--      ,to_number(replace('-','',g.GEO_ID)) instance_num
      ,g.ACS_VAL nval_num
      ,(floor(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))*power(10,length(g.ACS_VAL)-1) || '-' || ceil(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))*power(10,length(g.ACS_VAL)-1)) tval_char -- convert to value range
from address_mapped am -- link to patient
join geo_dimension g -- primary key: geo_id
on (am.FIPS11 || '-' || am.ADDRESS_ZIP9) = g.GEO_ID
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='num'
;

select count(*), count(distinct patient_num), count(distinct concept_cd),
       max(nval_num), min(nval_num)
from geo_num_tbl;

--79,343,137	
--1,788,828	
--81	
--250,001	
--2,499

create table geo_cnt_tbl as
select am.patient_num
      ,meta.TABLE_ID 
      ,g.ACS_VAR
--      ,('ACS|' || meta.TABLE_ID || ':' || g.ACS_VAR) concept_cd
--      ,to_number(replace('-','',g.GEO_ID)) instance_num
      ,g.ACS_VAL
      ,(floor(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))*power(10,length(g.ACS_VAL)-1) || '-' || ceil(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))*power(10,length(g.ACS_VAL)-1)) tval_char -- convert to value range
      ,g.GEO_ID
      ,meta.LINE
from address_mapped am
join geo_dimension g
on (am.FIPS11 || '-' || am.ADDRESS_ZIP9) = g.GEO_ID
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='cnt'
;

--select count(*), count(distinct patient_num), count(distinct (TABLE_ID||ACS_VAR))
--from geo_cnt_tbl;
--543,442,383	
--1,788,846	
--306


/* ===========================================================
 * Calculate percentage range for "cnt" (count) ACS variables
 * ===========================================================
 */
create table geo_cnt_perc_tbl as                   
select n.patient_num
      ,('ACS|' || n.TABLE_ID || ':' || n.ACS_VAR) concept_cd
--      ,to_number(replace('-','',n.GEO_ID)) instance_num
      ,case when d.ACS_VAL = 0 then '0'
            else (floor(round(n.ACS_VAL/d.ACS_VAL*100)/10)*10 || '-' || ceil(round(n.ACS_VAL/d.ACS_VAL*100)/10)*10) 
       end as tval_char -- convert to percentage range
from geo_cnt_tbl n 
join geo_cnt_tbl d
on n.GEO_ID = d.GEO_ID and d.LINE=1 and n.LINE <> 1 and 
   n.TABLE_ID = d.TABLE_ID
;

/* ====================================
 * Create the observation_fact_geo view
 * ====================================
 */
create or replace view observation_fact_geo as
select gnum.patient_num
      ,gnum.concept_cd
      ,gnum.tval_char
      ,null units_cd
--      ,mod(gnum.patient_num, &&heron_etl_chunks)+1 as part
from geo_num_tbl gnum
union all
select gperc.patient_num
      ,gperc.concept_cd
      ,gperc.tval_char
      ,'%' units_cd
--      ,mod(gperc.patient_num, &&heron_etl_chunks)+1 as part
from geo_cnt_perc_tbl gperc
;
