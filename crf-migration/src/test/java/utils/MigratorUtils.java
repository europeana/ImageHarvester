package utils;

import com.mongodb.ServerAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.crfmigration.logic.MigrationManager;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MigratorUtils {
    public static final String PATH_PREFIX = "src/test/resources/";
    private static final SimpleDateFormat parserSDF=new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");

    public static MongoConfig readMongoConfig (final Config config) throws UnknownHostException {
        final List<ServerAddress> mongoServerAddresses = new LinkedList<>();

        for (final Config hostConfig: config.getConfigList("hosts")) {
            mongoServerAddresses.add(new ServerAddress(hostConfig.getString("host"), hostConfig.getInt("port")));
        }

        return new MongoConfig(mongoServerAddresses,
                               config.getString("dbName"),
                               config.getString("username"),
                               config.getString("password")
        );
    }

    public static MigratorConfig createMigratorConfig(final String pathToConfigFile) throws UnknownHostException, ParseException {
        final File configFile = new File(PATH_PREFIX + pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + configFile.getAbsolutePath());
            return null;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults()
                                                                                             .setSyntax(ConfigSyntax
                                                                                                                .CONF));
        final Integer batch = config.getInt("config.batch");
        final DateTime dateFilter = new DateTime(parserSDF.parse(config.getString("config.dateFilter")));

        final MongoConfig sourceMongo = readMongoConfig(config.getConfig("sourceMongo"));
        final MongoConfig targetMongo = readMongoConfig(config.getConfig("targetMongo"));

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongo, targetMongo, null, batch,dateFilter);

        return migrationConfig;
    }

    public static void runMigrator(final MigratorConfig migratorConfig, final Date dateFilter) throws IOException, InterruptedException, ExecutionException, TimeoutException {


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
