package eu.europeana.publisher.dao;

import eu.europeana.publisher.domain.DBTargetConfig;

import java.net.UnknownHostException;

/**
 * Created by salexandru on 14.08.2015.
 */
public class PublisherWriter {
    private final String connectionId;
    private final PublisherHarvesterDao harvesterDao;
    private final SOLRWriter solrWriter;

    public PublisherWriter (final DBTargetConfig config) throws UnknownHostException {
        if (null == config) {
            throw new IllegalArgumentException("Config passed to publisher writer is null");
        }
        connectionId = config.getName();
        harvesterDao = new PublisherHarvesterDao(config);
        solrWriter = new SOLRWriter(config);
    }

    public PublisherHarvesterDao getHarvesterDao () {
        return harvesterDao;
    }

    public SOLRWriter getSolrWriter () {
        return solrWriter;
    }

    public String getConnectionId () {
        return connectionId;
    }
}
