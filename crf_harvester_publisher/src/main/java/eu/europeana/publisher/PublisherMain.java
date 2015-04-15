package eu.europeana.publisher;

import com.google.common.io.Files;
import com.typesafe.config.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.Publisher;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;
import scala.Char;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
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


        String startTimestampFile = null;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (StringUtils.isEmpty(file)) {
                    LOG.info("File is empty => startTimestamp is null");
                }
                else {
                    startTimestamp = DateTime.parse(file.trim());
                    LOG.info("startTimestamp loaded from file is "+startTimestamp);
                }
            }
            else {
                LOG.info("startTimestampFile is null");
            }
        }
        catch (ConfigException.Null e) {
            LOG.info("startTimestampFile is null");
        }
        catch (FileNotFoundException e) {
            LOG.info("Timestamp file doesn't exist => startTimestampFile is null");
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
                    targetDBPassword, startTimestamp,startTimestampFile, solrURL, batch);

            final Publisher publisher = new Publisher(publisherConfig);
            publisher.start();
            //*/
        }
    }
}
