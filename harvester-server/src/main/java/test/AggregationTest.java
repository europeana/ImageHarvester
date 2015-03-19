package test;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import com.mongodb.*;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.mongo.ProcessingJobDaoImpl;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.Page;
import eu.europeana.harvester.domain.ProcessingJob;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregationTest {

    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        Datastore datastore = null;
        DB db = null;
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "newHarvesterBackup";
            //db = mongo.getDB("admin");
            //Boolean auth = db.authenticate("harvester_europeana", "Nhck0zCfcu0M6kK".toCharArray());
            //System.out.println(auth);

            datastore = morphia.createDatastore(mongo, dbName);
            ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);

            Map<String, Integer> ips = processingJobDao.getIpDistribution();
            for(Map.Entry<String, Integer> ip : ips.entrySet()) {
                System.out.println(ip.getKey() + " " + ip.getValue());
            }

            System.out.println("Map: " + ips.size());

            final List<ProcessingJob> processingJobs = processingJobDao.getDiffusedJobsWithState(JobState.READY, new Page(0, 3500), ips, new HashMap<String, Boolean>());
            for(ProcessingJob processingJob : processingJobs) {
                System.out.println(processingJob.getId() + " " + processingJob.getIpAddress());
            }

            System.out.println("Map: " + ips.size());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }
}
