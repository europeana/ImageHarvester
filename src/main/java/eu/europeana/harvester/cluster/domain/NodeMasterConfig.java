package eu.europeana.harvester.cluster.domain;

import eu.europeana.harvester.httpclient.response.ResponseType;

/**
 * Stores various configuration arguments that controls the behavior of the node master actor.
 */
public class NodeMasterConfig {

    private final int nrOfSlaves;

    private final int minNrOfSlaves;

    private final int maxNrOfSlaves;

    private final int nrOfRetries;

    private final String pathToSave;

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
