CRF Publisher
==============

It's used to continously publish finished CRF jobs to the corelib databases (MongoDB for record details & Solr for querying / faceting).
It does this by polling the CRF DB and sending batches of updates to the target MongoDB & Solr.

The CRF publication application handles :
 * the conversion from from CRF harvester job results to corelib records & Solr docs
 * the continous publishing of new / fresh CRF job results (from the CRF DB) into the corelib DB's
 * save / restore publishing point between restarts