package eu.europeana.harvester.client;

import com.mongodb.WriteConcern;

/**
 * Contains different configuration values:<br/>
 *      - writeConcern :describes the guarantee that MongoDB provides when reporting on the success of a write operation
 */
public class HarvesterClientConfig {

    private final WriteConcern writeConcern;

    public HarvesterClientConfig(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
