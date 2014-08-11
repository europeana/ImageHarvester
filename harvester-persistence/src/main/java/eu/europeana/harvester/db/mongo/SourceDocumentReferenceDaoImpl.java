package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.SourceDocumentReference;

/**
 * MongoDB DAO implementation for CRUD with source_document_reference collection
 */
public class SourceDocumentReferenceDaoImpl implements SourceDocumentReferenceDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
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
        return datastore.get(SourceDocumentReference.class, id);
    }

    @Override
    public boolean update(SourceDocumentReference sourceDocumentReference, WriteConcern writeConcern) {
        if(read(sourceDocumentReference.getId()) != null) {
            datastore.save(sourceDocumentReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(SourceDocumentReference.class, id);
    }

    @Override
    public SourceDocumentReference findByUrl(String url) {
        final HashFunction hf = Hashing.md5();
        final String id = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash().toString();

        return read(id);
    }
}
