/*initial collection of addresses for geocoding*/

whenever sqlerror continue;
drop table address_mapped;
whenever sqlerror exit;

--create empty address_mapped table
--following the CDM LDS_ADRESS_HISTORY table spec
create table address_mapped (
 PATIENT_NUM    NUMBER(38,0)   
,RAW_ADDRESS	VARCHAR2(128 BYTE)
,ADDRESS_STREET VARCHAR2(128 BYTE)
,ADDRESS_CITY	VARCHAR2(128 BYTE)
,ADDRESS_STATE	VARCHAR2(128 BYTE)
,ADDRESS_ZIP9	VARCHAR2(128 BYTE)
,FIPS11     	VARCHAR2(11 BYTE)
,LOCATOR    	VARCHAR2(20 BYTE)
,SCORE      	NUMBER(38,4)
,LON        	NUMBER(38,4)
,LAT        	NUMBER(38,4)
)
;
