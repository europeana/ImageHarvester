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
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.crfmigration.logic.MigrationManager;
import eu.europeana.crfmigration.logic.MigrationMetrics;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;


public class Migrator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Migrator.class.getName());
    private final Date dateFilter;

    public Migrator(Date dateFilter) {
        this.dateFilter = dateFilter;
    }


    public void start() throws IOException {
        LOG.info("Migrator starting ");

        LOG.info("Date to filter: " + dateFilter);

        final String configFilePath = "migration.conf";
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




        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongoConfig, targetMongoConfig, graphiteReporterConfig, batch);

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
                dateFilter,
                migrationConfig.getBatch());
        migrationManager.migrate();
    }

    public void stop() {
        LOG.info("Migrator stopped ");
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {

        Date dateFilter = null;

        if (1 == args.length) {
            try {
                dateFilter = ISODateTimeFormat.dateTime().parseDateTime(args[0]).toDate();
            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "The timestamp must respect the ISO 861 format! E.q: yyyy-MM-dd'T'HH:mm:ss.SSSZZ defaulting to begining of time", e);
                dateFilter = DateTime.now().minusYears(20).toDate();
            }
        } else if (args.length > 1) {
            System.out.println("Too many arguments. Please provide only one !");
            System.exit(-1);
        }

        final Migrator migrator = new Migrator(dateFilter);
        migrator.start();
    }

}
