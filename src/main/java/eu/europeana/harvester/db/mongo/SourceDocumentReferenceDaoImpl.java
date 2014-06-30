package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.SourceDocumentReference;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with source_document_reference collection
 */
public class SourceDocumentReferenceDaoImpl implements SourceDocumentReferenceDao {

    private final Datastore datastore;

    public SourceDocumentReferenceDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern) {
        if(read(sourceDocumentReference.getId()) == null) {
            datastore.save(sourceDocumentReference, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void createOrModify(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern) {
        datastore.save(sourceDocumentReference, writeConcern);
    }

    @Override
    public SourceDocumentReference read(String id) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("id").equal(id);

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("id").equal(sourceDocumentReference.getId());

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.save(sourceDocumentReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("id").equal(sourceDocumentReference.getId());

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(sourceDocumentReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public SourceDocumentReference findByUrl(String url) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("url").equal(url);

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }
}
