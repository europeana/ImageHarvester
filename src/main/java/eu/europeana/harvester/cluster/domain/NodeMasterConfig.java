package eu.europeana.harvester.cluster.domain;

import eu.europeana.harvester.httpclient.response.ResponseType;

/**
 * Stores various configuration arguments that controls the behavior of the node master actor.
 */
public class NodeMasterConfig {

    /**
     * Number of slaves per node at startup.
     */
    private final int nrOfSlaves;

    /**
     * The minimum number of slaves if there are no tasks to do.
     */
    private final int minNrOfSlaves;

    /**
     * Tha maximum number of slaves when the node gets a bigger load of tasks.
     */
    private final int maxNrOfSlaves;

    /**
     * The number of retries to reach a slave before restarting it.
     */
    private final int nrOfRetries;

    /**
     * The absolute path on disk where the content of the download will be saved.
     */
    private final String pathToSave;

    /**
     * The type of the response: diskStorage or memoryStorage
     */
    private final ResponseType responseType;

    public NodeMasterConfig(int nrOfSlaves, int minNrOfSlaves, int maxNrOfSlaves, int nrOfRetries, String pathToSave,
                            ResponseType responseType) {
        this.nrOfSlaves = nrOfSlaves;
        this.minNrOfSlaves = minNrOfSlaves;
        this.maxNrOfSlaves = maxNrOfSlaves;
        this.nrOfRetries = nrOfRetries;
        this.pathToSave = pathToSave;
        this.responseType = responseType;
    }

    public int getNrOfSlaves() {
        return nrOfSlaves;
    }

    public int getMinNrOfSlaves() {
        return minNrOfSlaves;
    }

    public int getMaxNrOfSlaves() {
        return maxNrOfSlaves;
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
