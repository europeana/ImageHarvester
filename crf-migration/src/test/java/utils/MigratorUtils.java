package utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.MigrationManager;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MigratorUtils {
    public static final String PATH_PREFIX = "src/test/resources/";

    public static MigratorConfig createMigratorConfig(final String pathToConfigFile) {
        final File configFile = new File(PATH_PREFIX + pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + configFile.getAbsolutePath());
            return null;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults()
                                                                                             .setSyntax(ConfigSyntax
                                                                                                                .CONF));
        final Integer batch = config.getInt("config.batch");

        final Config sourceMongoConfig = config.getConfig("sourceMongo");
        final Config targetMongoConfig = config.getConfig("targetMongo");

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

        final MongoConfig sourceMongo = new MongoConfig(sourceHost, sourcePort, sourceDBName, sourceDBUsername, sourceDBPassword);
        final MongoConfig targetMongo = new MongoConfig(targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword);

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongo, targetMongo, null, batch);

        return migrationConfig;
    }

    public static void runMigrator(final MigratorConfig migratorConfig, final Date dateFilter) throws IOException {


            final MigratorEuropeanaDao migratorEuropeanaDao = new MigratorEuropeanaDao(migratorConfig.getSourceMongoConfig());
            final MigratorHarvesterDao migratorHarvesterDao = new MigratorHarvesterDao(migratorConfig.getTargetMongoConfig()

                                                                                       );

            // Prepare the migrator & start it
            final MigrationManager migrationManager = new MigrationManager(migratorEuropeanaDao,
                                                                           migratorHarvesterDao,
                                                                           dateFilter,
                                                                           migratorConfig.getBatch());

            migrationManager.migrate();

    }
}
