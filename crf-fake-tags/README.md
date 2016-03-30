CRF Fake Tags
==============

The fake tags library handles the conversion from properties (ie. colour, orientation, etc.) to a 32 bit set represented
as an integer. The properties can have single or multiple values.

Corelib has search & compute facets with OR / AND filter criteria's. In the current implementation of the search (using Solr)
we cannot do this using subdocuments as Solr has poor support for subdocument querying & facetings. As such we decided to
represent the subdocuments as bit sets where bits at specific positions "encode" values of properties (that belong to the subdocuments).

In the Solr implementation the subdocuments are stored as an array of integers where each is a "fake tag".
Solr supports querying & faceting for multi value fields (ie. arrays). The fake tags library becomes obsolete once Solr has
good support for querying & faceting on subdocuments.

The "fake tags" library offers the following features :
 * encode a set of properties to a bit set (32 bit integer)
 * encode an individual property to a bit set (leaving all the other bits set to 0)
 * decode a bit set into a set of properties with values
 * decode a bit set into a single property with values