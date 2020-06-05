/*****************************************/
/*ETL to i2b2 observation_fact           */
/*****************************************/

/* ==================================================================
 * Anonymize "num" (numeric) variables
 * - e.g. 78000 -> 70000 - 80000
 * ==================================================================
 */

create or replace view geo_num_v as
select g.GEO_ID
      ,meta.GEO_LEVEL
      ,meta.TABLE_ID
      ,g.ACS_VAR
      ,((floor(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))-0.5)*power(10,length(g.ACS_VAL)) || '-' || (ceil(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))+0.5)*power(10,length(g.ACS_VAL))) tval_char
from geo_dimension g
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='num'
;

/* ===========================================================
 * Anonymize "cnt" (count) variables
 * Calculate percentage range for "cnt" (count) ACS variables
 * ===========================================================
 */
create or replace view geo_cnt_v as
select g.GEO_ID
      ,meta.GEO_LEVEL
      ,meta.TABLE_ID
      ,g.ACS_VAR
      ,g.ACS_VAL
      ,((floor(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))-0.5)*power(10,length(g.ACS_VAL)) || '-' || (ceil(g.ACS_VAL/power(10,length(g.ACS_VAL)-1))+0.5)*power(10,length(g.ACS_VAL))) tval_char
      ,meta.LINE
from geo_dimension g
join acs_metadata meta
on (meta.VARIABLE_CODE || '_' || meta.GEO_LEVEL) = g.ACS_VAR and
    meta.SUMMARY_TYPE='cnt'
;

create or replace view geo_cnt_perc_v as                   
select g.GEO_ID
      ,g.GEO_LEVEL
      ,g.TABLE_ID
      ,g.ACS_VAR
      ,g.LINE
      ,case when g.ACS_VAL = d.ACS_VAL then '100'
            else (floor(round(g.ACS_VAL/d.ACS_VAL*100)/10)*10 || '-' || ceil(round(g.ACS_VAL/d.ACS_VAL*100)/10)*10) 
       end as tval_char -- convert to percentage range
from geo_cnt_v g
join geo_cnt_v d
on g.GEO_ID = d.GEO_ID and d.LINE=1 and
   g.TABLE_ID = d.TABLE_ID and
   g.GEO_LEVEL = d.GEO_LEVEL
;

/* =====================================
 * Create the observation_fact_geo table
 * =====================================
 */
create table observation_fact_geo (
    patient_num integer,
    concept_cd varchar(40),
    tval_char varchar(8),
    modifier_cd varchar(8)
);

insert into observation_fact_geo (
    select pat.patient_num,(geo.ACS_VAR || ':' || geo.ACS_VAL),'@','@'
    from geo_dimension geo
    join patient_dimension pat  -- add geo_id to patient_dimension@id
    on pat.geo_id = geo.geo_id
    where geo.ACS_VAR = 'RUCA'
);
commit;

insert into observation_fact_geo (
    select pat.patient_num,('ACS|' || geo.TABLE_ID || ':' || geo.ACS_VAR)
          ,geo.tval_char,('GEO_LEVEL:' || geo.GEO_LEVEL)
    from geo_num_v geo
    join patient_dimension pat  -- add geo_id to patient_dimension@id
    on pat.geo_id = geo.geo_id
);
commit;

insert into observation_fact_geo (
    select pat.patient_num,('ACS|' || geo.TABLE_ID || ':' || geo.ACS_VAR)
          ,(geo.tval_char || '%'),('GEO_LEVEL:' || geo.GEO_LEVEL)
    from geo_cnt_perc_v geo
    join patient_dimension pat  -- add geo_id to patient_dimension@id
    on pat.geo_id = geo.geo_id
    where geo.LINE <> 1
);
commit;
