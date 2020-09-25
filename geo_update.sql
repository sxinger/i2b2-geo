/*add geocoded addresses (from DeGauss) to address_mapped table*/

select * from new_address_degauss_output where 1 = 0;

insert into address_mapped
select alladdr.patient_num
      ,newaddr.address
      ,upper(newaddr.street)
      ,upper(newaddr.city)
      ,newaddr.state
      ,newaddr.zip
      ,newaddr.tract
      ,UPPER(newaddr.precision)
      ,newaddr.score
      ,newaddr.lon
      ,newaddr.lat
from new_address_degauss_output newaddr
join address_update alladdr
on newaddr.address = trim(both ' ' from alladdr.RAW_ADDRESS)
;
commit;
