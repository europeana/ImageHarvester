CRF Migration
==============

It's used to continously send new UIM records to the CRF Harvester for processing.
It does this by polling the UIM records DB for new / fresh records and adding them to the CRF DB

The CRF migration application handles :
 * the conversion from UIM records to CRF harvester jobs
 * the continous importing of new / fresh UIM records (from the records DB) to the CRF jobs (into the CRF DB)
 * save / restore migration point between restarts