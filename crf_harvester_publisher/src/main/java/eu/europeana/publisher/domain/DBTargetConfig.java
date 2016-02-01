package eu.europeana.publisher.domain;

import com.typesafe.config.Config;
import eu.europeana.harvester.domain.MongoConfig;

import java.net.UnknownHostException;

/**
 * Created by salexandru on 29.06.2015. Modified by ymamakis on 12.11.2015
 */
public class DBTargetConfig {
	private final MongoConfig mongoConfig;
	private final String solrUrl;
	private final Boolean solrCommitEnabled;

	private final String name;
	// Added zookeeper
	private final String zookeeperUrl;
	private final String collection;
	private final int solrBatchSize;

	public DBTargetConfig(MongoConfig mongoConfig, String solrUrl,
			Boolean solrCommitEnabled, String name, String zookeeperUrl,
			String collection, int solrBatchSize) {
		this.mongoConfig = mongoConfig;
		this.solrUrl = solrUrl;
		this.solrCommitEnabled = solrCommitEnabled;
		this.name = name;
		this.zookeeperUrl = zookeeperUrl;
		this.collection = collection;
		this.solrBatchSize = solrBatchSize;
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

	public String getCollection() {
		return collection;
	}

	public String getZookeeperUrl() {
		return zookeeperUrl;
	}
	
	public int getSolrBatchSize(){
		return solrBatchSize;
	}

	public static DBTargetConfig valueOf(final Config config)
			throws UnknownHostException {
		final MongoConfig mongoConfig = MongoConfig.valueOf(config
				.getConfig("mongo"));
		final String solrUrl = config.getString("solrUrl");
		final Boolean solrCommitEnabled = (config.hasPath("solrCommitEnabled")) ? config
				.getBoolean("solrCommitEnabled") : false;
		final String name = config.getString("name");
		final String zookeeperUrl = config.getString("zookeeperUrl");
		final String collection = config.getString("collection");
		final int solrBatchSize = (config.hasPath("solrBatchSize"))?config.getInt("solrBatchSize"):1000;
		return new DBTargetConfig(mongoConfig, solrUrl, solrCommitEnabled,
				name, zookeeperUrl, collection, solrBatchSize);
	}
}
