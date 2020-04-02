/*****************************************/
/*merge with patient_dimension           */
/*ETL                                    */
/*****************************************/

/* ==================================================================
 * Separate "cnt" (count) variables from "num" (numerical) variables
 * ==================================================================
 */
create table geo_num_tbl as
select am.patient_num
      ,(g.ACS_VAR || ':' || g.ACS_VAL) concept_cd
      ,meta.TABLE_ID instance_num
      ,null nval_num
from address_mapped am
join geo_dimension g
on (am.FIPS11 || '-' || am.ZIP) = g.GEO_ID and
   g.ACS_VAR = 'RUCA'
union all
select am.patient_num
      ,('ACS|' || meta.TABLE_ID || ':' || g.ACS_VAR) concept_cd
      ,meta.TABLE_ID instance_num
      ,g.ACS_VAL nval_num
from address_mapped am
join geo_dimension g
on (am.FIPS11 || '-' || am.ZIP) = g.GEO_ID
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='num'
;

create table geo_cnt_tbl as
select am.patient_num
      ,('ACS|' || meta.TABLE_ID || ':' || g.ACS_VAR) concept_cd
      ,meta.TABLE_ID instance_num
      ,g.ACS_VAL nval_num
      ,g.GEO_ID
from address_mapped am
join geo_dimension g
on (am.FIPS11 || '-' || am.ZIP) = g.GEO_ID
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='cnt'
;

/* ===========================================================
 * Calculate percentage range for "cnt" (count) ACS variables
 * ===========================================================
 */
create table geo_cnt_perc_tbl as                   
select am.patient_num
      ,('ACS|' || meta.TABLE_ID || ':' || g.ACS_VAR) concept_cd
      ,'%' units_cd
      ,meta.TABLE_ID instance_num
      ,(floor(round(n.ACS_VAR/d.ACS_VAL*100)/10)*10 || '-' || ceil(round(n.ACS_VAR/d.ACS_VAL*100)/10)*10) tval_char -- convert to percentage range
from address_mapped am
join geo_dimension n 
on (am.FIPS11 || '-' || am.ZIP) = n.GEO_ID
join geo_cnt_tbl d
on n.GEO_ID = d.GEO_ID and d.LINE=1 and
   regexp(n.ACS_VAR,'[^_]',1,1) = regexp(d.ACS_VAR,'[^_]',1,1)
;


/* ====================================
 * Create the observation_fact_geo view
 * ====================================
 */
create or replace view observation_fact_geo as
select 
    gn.*,
    mod(gn.pat_enc_csn_id, &&heron_etl_chunks)+1 as part
from geo_dimension gn
union 
select 
    gn.*,
    mod(gn.pat_enc_csn_id, &&heron_etl_chunks)+1 as part
from geo_dimension gn
;
