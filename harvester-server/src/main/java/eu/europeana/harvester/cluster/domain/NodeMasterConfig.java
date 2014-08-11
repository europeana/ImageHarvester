package eu.europeana.harvester.cluster.domain;

import eu.europeana.harvester.httpclient.response.ResponseType;

/**
 * Stores various configuration arguments that controls the behavior of the node master actor.
 */
public class NodeMasterConfig {

    /**
     * Number of slaves per node at startup.
     */
    private final Integer nrOfDownloaderSlaves;

    /**
     * Number of slaves per node at startup.
     */
    private final Integer nrOfExtractorSlaves;

    /**
     * Number of slaves per node at startup.
     */
    private final Integer nrOfPingerSlaves;

    /**
     * The number of retries to reach a slave before restarting it.
     */
    private final Integer nrOfRetries;

    /**
     * The absolute path on disk where the content of the download will be saved.
     */
    private final String pathToSave;

    /**
     * The type of the response: diskStorage or memoryStorage
     */
    private final ResponseType responseType;

    public NodeMasterConfig(final Integer nrOfDownloaderSlaves, final Integer nrOfExtractorSlaves,
                            final Integer nrOfPingerSlaves, final Integer nrOfRetries, final String pathToSave,
                            final ResponseType responseType) {
        this.nrOfDownloaderSlaves = nrOfDownloaderSlaves;
        this.nrOfExtractorSlaves = nrOfExtractorSlaves;
        this.nrOfPingerSlaves = nrOfPingerSlaves;
        this.nrOfRetries = nrOfRetries;
        this.pathToSave = pathToSave;
        this.responseType = responseType;
    }

    public int getNrOfDownloaderSlaves() {
        return nrOfDownloaderSlaves;
    }

    public Integer getNrOfExtractorSlaves() {
        return nrOfExtractorSlaves;
    }

    public Integer getNrOfPingerSlaves() {
        return nrOfPingerSlaves;
    }

    public int getNrOfRetries() {
        return nrOfRetries;
    }

    public String getPathToSave() {
        return pathToSave;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

}
