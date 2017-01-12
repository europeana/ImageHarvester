package eu.europeana.crfmigration.domain;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MigratorConfig {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MigratorConfig.class.getName());

    private static final SimpleDateFormat parserSDF=new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");

    public static final MigratorConfig loadFromConfigFilePath(final String filePath) throws IOException, ParseException {
        String configFilePath;
        if(filePath == null) {
            configFilePath = "./extra-files/config-files/migration.conf";
        }
        else {
            configFilePath = filePath;
        }

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                    "Config file not found. Exiting.");
            System.exit(-1);
        }

        if (!configFile.canRead()) {
            throw new IOException("Cannot read file " + configFilePath);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        return loadFromConfig(config);
    }

        public static final MigratorConfig loadFromConfig(final Config config) throws UnknownHostException, ParseException {

        final MongoConfig sourceMongoConfig = MongoConfig.valueOf(config.getConfig("sourceMongo"));
        final MongoConfig targetMongoConfig = MongoConfig.valueOf(config.getConfig("targetMongo"));


        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final int batch = config.getInt("config.batch");
        final DateTime dateFilter;
        try {
            dateFilter = new DateTime(parserSDF.parse(config.getString("config.dateFilter")));
            LOG.debug("Date filter set to " + dateFilter.toString());
        } catch (ParseException e) {
            LOG.error("Date specified in config.dateFilter must conform to the standard pattern" + parserSDF.toPattern());
            throw e;
        }


        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        return new MigratorConfig(sourceMongoConfig, targetMongoConfig, graphiteReporterConfig, batch,dateFilter);

    }

    private final MongoConfig sourceMongoConfig;
    private final MongoConfig targetMongoConfig;
    private final GraphiteReporterConfig graphiteReporterConfig;
    private final int batch;
    private final DateTime dateFilter;
    public MigratorConfig(MongoConfig sourceMongoConfig, MongoConfig targetMongoConfig, GraphiteReporterConfig graphiteReporterConfig, int batch,final DateTime dateFilter) {
        this.sourceMongoConfig = sourceMongoConfig;
        this.targetMongoConfig = targetMongoConfig;
        this.graphiteReporterConfig = graphiteReporterConfig;
        this.batch = batch;
        this.dateFilter = dateFilter;
    }

    public MongoConfig getSourceMongoConfig() {
        return sourceMongoConfig;
    }

    public MongoConfig getTargetMongoConfig() {
        return targetMongoConfig;
    }

    public GraphiteReporterConfig getGraphiteReporterConfig() {
        return graphiteReporterConfig;
    }

    public int getBatch() {
        return batch;
    }

    public DateTime getDateFilter() {
        return dateFilter;
    }

    public MigratorConfig withBatchSize(int newBatch) {
        return new MigratorConfig(sourceMongoConfig,targetMongoConfig,graphiteReporterConfig,newBatch,dateFilter);
    }

}
