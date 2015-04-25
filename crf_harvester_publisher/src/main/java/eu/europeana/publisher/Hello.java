package eu.europeana.publisher;

import com.mongodb.*;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Created by paul on 25/04/15.
 */
public class Hello {
    public static void main(String[] args) throws IOException, SolrServerException {
        final Mongo sourceMongo = new Mongo("europeana8.busymachines.com",
                27017);

        DB sourceDB = sourceMongo.getDB("newHarvester");
        final Boolean auth = sourceDB.authenticate("harvester_europeana", "Nhck0zCfcu0M6kK"
                .toCharArray());

        if (auth ==false){
            System.out.println("Cannot auth");
        }

        final DBCollection sourceDocumentProcessingStatisticsCollection = sourceDB
                .getCollection("SourceDocumentProcessingStatistics");

        DateTime startTimeStamp = new DateTime("2015-05-01T00:24+0100");
        BasicDBObject findQuery;
        findQuery = new BasicDBObject();
        findQuery.put("updatedAt",new BasicDBObject("$gt",startTimeStamp.toDate()));
        findQuery.put("state", "SUCCESS");

        final BasicDBObject fields = new BasicDBObject(
                "sourceDocumentReferenceId", true)
                .append("httpResponseContentType", true)
                .append("referenceOwner.recordId", true).append("_id", false)
                .append("updatedAt", true);

        // Sort query results in ascending order by "updatedAt" field.
        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("updatedAt", 1);

        DBCursor sourceDocumentProcessingStatisticsCursor = sourceDocumentProcessingStatisticsCollection
                .find(findQuery)
                .sort(sortOrder)
                .limit(1000)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        System.out.println("Retrieving SourceDocumentProcessingStatistics from cursor with size "
                + sourceDocumentProcessingStatisticsCursor.size());

        System.out.println("The mongo query is "+sourceDocumentProcessingStatisticsCursor.getQuery().toString());
    }
}
