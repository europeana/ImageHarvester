package eu.europeana.publisher;

import com.typesafe.config.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.Publisher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PublisherMain {

    private static Logger LOG = LogManager.getLogger(PublisherMain.class.getName());

    public static void main(String[] args) throws IOException, SolrServerException {
        LOG.info("Start publishing ...");

        String configFilePath;
        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/publishing.conf";
        } else if(1 == args.length) {
            configFilePath = args[0];
        }
        else {
            configFilePath = args[0];

        }

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        }
        catch (ConfigException.Null e) {
           LOG.info("startTimestamp is null");
        }

        final String solrURL = config.getString("solr.url");

        final Integer batch = config.getInt("config.batch");

        final List<? extends Config> sourceMongoConfigList = config.getConfigList("sourceMongo");
        final List<? extends Config> targetMongoConfigList = config.getConfigList("targetMongo");

        if (sourceMongoConfigList.size() != targetMongoConfigList.size()) {
            LOG.error("sourceMongo configuration and targetMongo configuration from " + configFile + " have different sizes!");
            System.exit(-1);
        }

        if (0 == sourceMongoConfigList.size()) {
            LOG.error("Empty source/target Mongo configurations from " + configFile);
            System.exit(-1);
        }

        final Iterator<? extends Config> sourceMongoIter = sourceMongoConfigList.iterator();
        final Iterator<? extends Config> targetMongoIter = targetMongoConfigList.iterator();

        while (sourceMongoIter.hasNext() && targetMongoIter.hasNext()) {
            final Config sourceMongoConfig = sourceMongoIter.next();
            final Config targetMongoConfig = targetMongoIter.next();

            final String sourceHost = sourceMongoConfig.getString("host");
            final Integer sourcePort = sourceMongoConfig.getInt("port");
            final String sourceDBName = sourceMongoConfig.getString("dbName");
            final String sourceDBUsername = sourceMongoConfig.getString("username");
            final String sourceDBPassword = sourceMongoConfig.getString("password");

            final String targetHost = targetMongoConfig.getString("host");
            final Integer targetPort = targetMongoConfig.getInt("port");
            final String targetDBName = targetMongoConfig.getString("dbName");
            final String targetDBUsername = targetMongoConfig.getString("username");
            final String targetDBPassword = targetMongoConfig.getString("password");

            final PublisherConfig publisherConfig = new PublisherConfig(sourceHost, sourcePort, sourceDBName,
                    sourceDBUsername, sourceDBPassword, targetHost, targetPort, targetDBName, targetDBUsername,
                    targetDBPassword, startTimestamp, solrURL, batch);

            final Publisher publisher = new Publisher(publisherConfig);
            publisher.start();
            //*/
        }
    }
}
