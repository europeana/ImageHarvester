package eu.europeana;

import eu.europeana.domain.MigrationMongoConfig;
import eu.europeana.logic.Migrator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException {
        final String configFilePath = "./crf-migration/src/main/resources/migration.conf";

        final Properties prop = new Properties();
        final InputStream input = new FileInputStream(configFilePath);
        prop.load(input);

        final String host = prop.getProperty("host");
        final Integer port = Integer.valueOf(prop.getProperty("port"));
        final String dbName = prop.getProperty("dbName");

        final MigrationMongoConfig config = new MigrationMongoConfig(host, port, dbName);

        final Migrator migrator = new Migrator(config);
        migrator.migrate();
    }
}
