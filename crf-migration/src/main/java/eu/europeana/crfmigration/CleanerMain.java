package eu.europeana.crfmigration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.Cleaner;

import java.io.File;
import java.io.IOException;

public class CleanerMain {
    public static void main(String[] args) throws IOException {
        final String configFilePath = "./extra-files/config-files/migration.conf";
        final File configFile = new File(configFilePath);

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final String targetHost = config.getString("clean.host");
        final Integer targetPort = config.getInt("clean.port");
        final String targetDBName = config.getString("clean.dbName");
        String targetDBUsername = "";
        String targetDBPassword = "";

        if(config.hasPath("clean.username")) {
            targetDBUsername = config.getString("clean.username");
            targetDBPassword = config.getString("clean.password");
        }

        final int batch = config.getInt("config.batch");

        final MongoConfig cleanerConfig =
                new MongoConfig("", 0, "", "", "", batch,
                                targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword,
                                null, null, null
                               );

        final Cleaner cleaner = new Cleaner(cleanerConfig);
        cleaner.clean();
    }
}
