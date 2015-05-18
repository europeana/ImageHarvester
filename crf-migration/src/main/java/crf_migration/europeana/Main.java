package crf_migration.europeana;

import crf_migration.europeana.domain.MigrationMongoConfig;
import crf_migration.europeana.logic.Migrator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException {
        final String configFilePath = "./crf_migration/src/main/resources/migration.conf";

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
