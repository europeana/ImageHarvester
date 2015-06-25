package eu.europeana.harvester.db.dummy;

import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by salexandru on 25.06.2015.
 *
 * a basic do nothing implementation. In case you want to ignore
 */
public class DummyMediaStorageClientImpl implements MediaStorageClient {
    @Override
    public Boolean checkIfExists (String id) {
        return false;
    }

    @Override
    public MediaFile retrieve (String id, Boolean withContent) throws IOException, NoSuchAlgorithmException {
        return null;
    }

    @Override
    public void createOrModify (MediaFile mediaFile) {}

    @Override
    public void delete (String id) throws IOException {}
}
