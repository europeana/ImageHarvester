package eu.europeana.crfmigration;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.GraphiteReporterConfig;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.crfmigration.logic.MigrationManager;
import eu.europeana.crfmigration.logic.MigrationMetrics;
import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;


public class Migrator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Migrator.class.getName());
    private static final SimpleDateFormat parserSDF=new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");


    public void start() throws IOException, ParseException {
        LOG.info("Migrator starting ");

        final String configFilePath = "./extra-files/config-files/migration.conf";
        final File configFile = new File(configFilePath);

        if (!configFile.canRead()) {
            throw new IOException("Cannot read file " + configFilePath);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));


        final MongoConfig sourceMongoConfig = MongoConfig.valueOf(config.getConfig("sourceMongo"));
        final MongoConfig targetMongoConfig = MongoConfig.valueOf(config.getConfig("targetMongo"));


        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final int batch = config.getInt("config.batch");
        final DateTime dateFilter;
        try {
            dateFilter = new DateTime(parserSDF.parse(config.getString("config.dateFilter")));
            LOG.info("Date filter set to "+dateFilter.toString());
        } catch (ParseException e) {
            LOG.error("Date specified in config.dateFilter must conform to the standard pattern" + parserSDF.toPattern());
            throw e;
        }


        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongoConfig, targetMongoConfig, graphiteReporterConfig, batch,dateFilter);

        // Prepare the graphite reporter
        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteReporterConfig.getGraphiteServer(),
                graphiteReporterConfig.getGraphitePort()));

        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(MigrationMetrics.METRIC_REGISTRY)
                .prefixedWith(graphiteReporterConfig.getGraphiteMasterId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);

        graphiteReporter.start(30, TimeUnit.SECONDS);

        final Slf4jReporter reporter = Slf4jReporter.forRegistry(MigrationMetrics.METRIC_REGISTRY)
                .outputTo(org.slf4j.LoggerFactory.getLogger(LOG.getName()))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(1, TimeUnit.MINUTES);

        // Prepare the persistence
        final MigratorEuropeanaDao migratorEuropeanaDao = new MigratorEuropeanaDao(migrationConfig.getSourceMongoConfig());
        final MigratorHarvesterDao migratorHarvesterDao = new MigratorHarvesterDao(migrationConfig.getTargetMongoConfig()

        );

        // Prepare the migrator & start it
        final MigrationManager migrationManager = new MigrationManager(migratorEuropeanaDao,
                migratorHarvesterDao,
                dateFilter.toDate(),
                migrationConfig.getBatch());
        migrationManager.migrate();
    }

    public void stop() {
        LOG.info("Migrator stopped ");
        System.exit(0);
    }

    public static void main(String[] args) throws IOException, ParseException {
        final Migrator migrator = new Migrator();
        migrator.start();
    }

}
