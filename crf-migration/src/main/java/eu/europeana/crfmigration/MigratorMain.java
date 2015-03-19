package eu.europeana.crfmigration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.Migrator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class MigratorMain {
    private static final Logger LOG = LogManager.getLogger(Migrator.class.getName());

    public static void main(String[] args) throws IOException {
        LOG.info("Starting Migrator ");
        Date d = null;

        if (1 == args.length) {
            try {
               d =  ISODateTimeFormat.dateTime().parseDateTime(args[0]).toDate();
            }
            catch (Exception e) {
                LOG.error("The timestamp must respect the ISO 861 format! E.q: yyyy-MM-dd'T'HH:mm:ss.SSSZZ ", e);
                System.exit(-1);
            }
        }
        else if (args.length > 1) {
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


        final MongoConfig migrationConfig =
                new MongoConfig(sourceHost, sourcePort, sourceDBName, sourceDBUsername, sourceDBPassword,
                        targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword);

        final Migrator migrator = new Migrator(migrationConfig, d);
        migrator.migrate();
        //*/
    }
}
