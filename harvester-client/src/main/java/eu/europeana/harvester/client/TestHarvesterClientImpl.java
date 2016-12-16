package eu.europeana.harvester.client;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.*;
import java.net.UnknownHostException;

/**
 * Created by ymamakis on 11/7/16.
 */
public class TestHarvesterClientImpl {


    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongo = new MongoClient("mongo1.crf.europeana.eu", 27017);
        Morphia morphia = new Morphia();
        String dbName = "crf_harvester_second";

        Datastore datastore = morphia.createDatastore(mongo, dbName,"harvester_europeana","Nhck0zCfcu0M6kK".toCharArray());
        HarvesterClient client = new HarvesterClientImpl(datastore,new HarvesterClientConfig(WriteConcern.ACKNOWLEDGED));
        ReferenceOwner owner  = new ReferenceOwner("11614","11614_Ag_EU_OpenUp!_RBGK",null);
        //System.out.println(interval.toDurationMillis());
        client.deactivateJobs(owner);
    }
}
