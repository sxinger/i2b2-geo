# Geocoding for i2b2

In the Greater Plains Collaborative (GPC), we seek to integrate a
variety of data sources for comparative effectiveness research using
i2b2, a clinical data warehouse. The
[GIS & Mapping services from the Minnesota Population Center][gis-mapping]
is one such data source.

This is a repository of ETL code that integrates socio-economic data
from the US [Census of Population and Housing][Census] and the
American Community Survey ([ACS][]) into i2b2.

Materials from MU in the `ACS_Census` branch were originally presented in a
[19 Apr 2016 demo to gpc-dev][19Apr], the Infrastructure and Software
Development group of GPC.

[gis-mapping]: https://pop.umn.edu/member-services/gis-mapping
[19Apr]: https://informatics.gpcnetwork.org/trac/Project/ticket/140#comment:47
[ACS]: http://www.census.gov/programs-surveys/acs/
[Census]: http://www.census.gov/prod/www/decennial.html

## References

  - [Serving the enterprise and beyond with informatics for integrating biology and the bedside (i2b2).](http://www.ncbi.nlm.nih.gov/pubmed/20190053)
    Murphy SN, Weber G, Mendis M, Gainer V, Chueh HC, Churchill S, Kohane I.
    J Am Med Inform Assoc. 2010 Mar-Apr;17(2):124-30. doi: 10.1136/jamia.2009.000893.
  - [The Greater Plains Collaborative: a PCORnet Clinical Research Data Network](http://jamia.bmj.com/content/21/4/637.full)
    Lemuel R Waitman, Lauren S Aaronson, Prakash M Nadkarni, Daniel W Connolly, James R Campbell
    J Am Med Inform Assoc 2014;21:637-641 doi:10.1136/amiajnl-2014-002756
