package utilities;

import com.typesafe.config.*;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.GraphiteReporterConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logging.LoggingComponent;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 09.06.2015.
 */
public class ConfigUtils {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConfigUtils.class.getName());


    public static PublisherConfig createPublisherConfig (final String pathToConfigFile) throws IOException {
        final File configFile = new File(pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + pathToConfigFile + " " + configFile.getAbsolutePath());
            return null;
        }
        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                                                               ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MongoConfig sourceDBConfig = MongoConfig.valueOf(config.getConfig("sourceMongo"));
        final List<DBTargetConfig> targetDBConfigs = new LinkedList<>();
        for (final Config target: config.getConfigList("targets")) {
            targetDBConfigs.add(DBTargetConfig.valueOf(target));
        }

        final Integer batch = config.getInt("config.batch");

        final DateTime startTimestamp = readStartTimestamp(config);
         String startTimestampFilename = null;

        try {
            startTimestampFilename = config.getString("criteria.startTimestampFile");
        }
        catch(ConfigException.Null e) {

        }

        Long sleepSecondsAfterEmptyBatch = null;

        try {
            sleepSecondsAfterEmptyBatch = config.getLong("criteria.sleepSecondsAfterEmptyBatch");
        }
        catch (ConfigException.Null e) {
            LOG.error("Error reading sleepSecondsAfterEmptyBatch", e);
        }

        int delayInSecondsForRemainingRecordsStatistics =
                config.hasPath("delayInMinutesForRemainingRecordsStatistics") ?
                        config.getInt("delayInMinutesForRemainingRecordsStatistics") :
                        10;

        final String stopGracefullyFilename = config.getString("criteria.stopGracefullyFile");

        return  new PublisherConfig(sourceDBConfig,
                                    targetDBConfigs,
                                    graphiteReporterConfig,
                                    startTimestamp, startTimestampFilename,stopGracefullyFilename,
                                    sleepSecondsAfterEmptyBatch,
                                    batch,
                                    delayInSecondsForRemainingRecordsStatistics);
    }

    private static DateTime readStartTimestamp(final Config config) throws IOException {
        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        }
        catch (ConfigException.Null e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp is null. Defaulting to 1 Jan 1970..");
        }


        String startTimestampFile;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (StringUtils.isEmpty(file)) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                              "Start timestamp file is null. Defaulting to 1 Jan 1970..");
                }
                else {
                    startTimestamp = DateTime.parse(file.trim());
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                              "Start timestamp {} loaded from file.",startTimestamp);
                }
            }
            else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                          "Start timestamp is null. Defaulting to 1 Jan 1970..");
            }
        }
        catch (ConfigException.Null e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp is null. Defaulting to 1 Jan 1970..");
        }
        catch (FileNotFoundException e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                      "Start timestamp file does not exist. Defaulting to 1 Jan 1970..");
        }

        return startTimestamp;
    }

}
