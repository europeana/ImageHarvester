package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
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
    public void create(SourceDocumentReference sourceDocumentReference) {
        datastore.save(sourceDocumentReference);
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
    public boolean update(SourceDocumentReference sourceDocumentReference) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("id").equal(sourceDocumentReference.getId());

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(sourceDocumentReference);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(SourceDocumentReference sourceDocumentReference) {
        Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class);
        query.criteria("id").equal(sourceDocumentReference.getId());

        List<SourceDocumentReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(sourceDocumentReference);

            return true;
        }

        return false;
    }

    @Override
    public void createOrModify(SourceDocumentReference sourceDocumentReference) {
        if(!update(sourceDocumentReference)) {
            create(sourceDocumentReference);
        }
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
