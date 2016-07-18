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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class Migrator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Migrator.class.getName());
    private static final SimpleDateFormat parserSDF=new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");
    private final MigratorConfig config;

    public Migrator(final MigratorConfig config) {
        if (null == config) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config;
    }


    public void start() throws IOException, ParseException, InterruptedException, ExecutionException, TimeoutException {
        LOG.debug("Migrator starting ");

        final GraphiteReporterConfig graphiteReporterConfig = new GraphiteReporterConfig(config.getGraphiteReporterConfig().getGraphiteServer(), config.getGraphiteReporterConfig().getGraphiteMasterId(), config.getGraphiteReporterConfig().getGraphitePort());

        final MigratorConfig migrationConfig = new MigratorConfig(config.getSourceMongoConfig(), config.getTargetMongoConfig(), graphiteReporterConfig, config.getBatch(),config.getDateFilter());

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
                config.getDateFilter().toDate(),
                migrationConfig.getBatch());
        migrationManager.migrate();
    }

    public void stop() {
        LOG.debug("Migrator stopped ");
        System.exit(0);
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException, TimeoutException {
        final MigratorConfig migratorConfig = MigratorConfig.loadFromConfigFilePath(args[0]);
        final Migrator migrator = new Migrator(migratorConfig);
        migrator.start();
    }

}
