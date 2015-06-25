package eu.europeana.crfmigration;

import com.mongodb.ServerAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.Cleaner;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class CleanerMain {
    private static MongoConfig readMongoConfig (final Config config) throws UnknownHostException {
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

    public static void main(String[] args) throws IOException {
        final String configFilePath = "./extra-files/config-files/migration.conf";
        final File configFile = new File(configFilePath);

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final MongoConfig cleanerConfig = readMongoConfig(config.getConfig("clean"));
        final Cleaner cleaner = new Cleaner(cleanerConfig);
        cleaner.clean();
    }
}
