package utilities;

import com.typesafe.config.*;
import eu.europeana.publisher.domain.GraphiteReporterConfig;
import eu.europeana.publisher.domain.MongoConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 09.06.2015.
 */
public class ConfigUtils {
    public static PublisherConfig createPublisherConfig (final String pathToConfigFile) {
        final File configFile = new File(pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + pathToConfigFile + " " + configFile.getAbsolutePath());
            return null;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults()
                                                                                             .setSyntax(ConfigSyntax.CONF));

        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        } catch (ConfigException.Null e) {
        }


        String startTimestampFile = null;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (!StringUtils.isEmpty(file)) {
                    startTimestamp = DateTime.parse(file.trim());
                }
            }
        } catch (ConfigException.Null e) {
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            fail("Fail to load startTimestampFile: " + startTimestampFile + "\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
            return null;
        }

        final String solrURL = config.getString("solr.url");

        final Integer batch = config.getInt("config.batch");

        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MongoConfig sourceConfig = new MongoConfig (config.getConfigList("sourceMongo").get(0));
        final MongoConfig targetConfig = new MongoConfig (config.getConfigList("targetMongo").get(0));

        return new PublisherConfig(sourceConfig, targetConfig,
                                   graphiteReporterConfig, startTimestamp,
                                   startTimestampFile, solrURL, batch
        );

    }
}
