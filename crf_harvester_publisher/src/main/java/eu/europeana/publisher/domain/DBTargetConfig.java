package eu.europeana.publisher.domain;

import com.typesafe.config.Config;
import eu.europeana.harvester.domain.MongoConfig;

import java.net.UnknownHostException;

/**
 * Created by salexandru on 29.06.2015.
 */
public class DBTargetConfig {
    private final MongoConfig mongoConfig;
    private final String solrUrl;
    private final Boolean solrCommitEnabled;

    private final String name;


    public DBTargetConfig(MongoConfig mongoConfig, String solrUrl, Boolean solrCommitEnabled, String name) {
        this.mongoConfig = mongoConfig;
        this.solrUrl = solrUrl;
        this.solrCommitEnabled = solrCommitEnabled;
        this.name = name;
    }

    public Boolean getSolrCommitEnabled() {
        return solrCommitEnabled;
    }

    public MongoConfig getMongoConfig() {
        return mongoConfig;
    }

    public String getSolrUrl() {
        return solrUrl;
    }

    public String getName() {
        return name;
    }

    public static DBTargetConfig valueOf(final Config config) throws UnknownHostException {
        final MongoConfig mongoConfig = MongoConfig.valueOf(config.getConfig("mongo"));
        final String solrUrl = config.getString("solrUrl");
        final Boolean solrCommitEnabled = (config.hasPath("solrCommitEnabled")) ? config.getBoolean("solrCommitEnabled") : false;
        final String name = config.getString("name");

        return new DBTargetConfig(mongoConfig, solrUrl, solrCommitEnabled, name);
    }
}
