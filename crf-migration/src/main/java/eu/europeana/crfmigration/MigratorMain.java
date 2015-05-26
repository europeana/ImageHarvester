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
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.Migrator;
import eu.europeana.crfmigration.logic.MigratorMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MigratorMain {
    private static final Logger LOG = LogManager.getLogger(Migrator.class.getName());

    public static void main(String[] args) throws IOException {
        LOG.info("Starting Migrator ");
        Date d = null;

        if (1 == args.length) {
            try {
                d = ISODateTimeFormat.dateTime().parseDateTime(args[0]).toDate();
            } catch (Exception e) {
                LOG.error("The timestamp must respect the ISO 861 format! E.q: yyyy-MM-dd'T'HH:mm:ss.SSSZZ ", e);
                System.exit(-1);
            }
        } else if (args.length > 1) {
            System.out.println("Too many arguments. Please provide only one !");
            System.exit(-1);
        }

        LOG.info("Date to filter: " + d);

        final String configFilePath = "./extra-files/config-files/migration.conf";
        final File configFile = new File(configFilePath);

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final String sourceHost = config.getString("sourceMongo.host");
        final Integer sourcePort = config.getInt("sourceMongo.port");
        final String sourceDBName = config.getString("sourceMongo.dbName");
        final String sourceDBUsername = config.getString("sourceMongo.username");
        final String sourceDBPassword = config.getString("sourceMongo.password");

        final String targetHost = config.getString("targetMongo.host");
        final Integer targetPort = config.getInt("targetMongo.port");
        final String targetDBName = config.getString("targetMongo.dbName");
        final String targetDBUsername = config.getString("targetMongo.username");
        final String targetDBPassword = config.getString("targetMongo.password");

        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer = config.getString("metrics.graphiteServer");
        final Integer graphitePort = config.getInt("metrics.graphitePort");

        final int batch = config.getInt("config.batch");

        final MongoConfig sourceMongoConfig = new MongoConfig(sourceHost, sourcePort, sourceDBName, sourceDBUsername, sourceDBPassword);
        final MongoConfig targetMongoConfig = new MongoConfig(targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword);

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(graphiteServer, graphiteMasterId, graphitePort);

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongoConfig, targetMongoConfig, graphiteReporterConfig, batch);

        final MigratorMetrics metrics = new MigratorMetrics();

        // Prepare the graphite reporter
        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteReporterConfig.getGraphiteServer(),
                graphiteReporterConfig.getGraphitePort()));

        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(MigratorMetrics.metricRegistry)
                .prefixedWith(graphiteReporterConfig.getGraphiteMasterId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);

        graphiteReporter.start(30, TimeUnit.SECONDS);

        // Prepare the LOG reporter
        // TODO : This is probably not correct!
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(MigratorMetrics.metricRegistry)
                .outputTo(org.slf4j.LoggerFactory.getLogger(LOG.getName()))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(1, TimeUnit.MINUTES);

        // Prepare the persistence
        final MigratorEuropeanaDao migratorEuropeanaDao = new MigratorEuropeanaDao(migrationConfig.getSourceMongoConfig(), metrics);
        final MigratorHarvesterDao migratorHarvesterDao = new MigratorHarvesterDao(metrics, migrationConfig.getTargetMongoConfig());

        // Prepare the migrator & start it
        final Migrator migrator = new Migrator(migratorEuropeanaDao,
                migratorHarvesterDao,
                metrics,
                d,
                migrationConfig.getBatch());
        migrator.migrate();
        //*/
    }
}
