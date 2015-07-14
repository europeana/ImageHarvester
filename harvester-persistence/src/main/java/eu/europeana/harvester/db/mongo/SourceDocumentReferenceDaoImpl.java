package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModify(Collection<SourceDocumentReference> sourceDocumentReferences, WriteConcern writeConcern) {
        if (null == sourceDocumentReferences || sourceDocumentReferences.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(sourceDocumentReferences, writeConcern);
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
    public List<SourceDocumentReference> read(List<String> ids) {
        if(ids.size()==0)
            return new ArrayList<>(0);
        else {
            final Query<SourceDocumentReference> query = datastore.createQuery(SourceDocumentReference.class)
                    .field("_id").hasAnyOf(ids).retrievedFields(true, "id", "referenceOwner", "urlSourceType", "url", "ipAddress", "lastStatsId", "redirectPathDepth", "redirectionPath", "active")
                    .hintIndex("_id_");
            if (query == null) {
                return new ArrayList<>(0);
            }
            return query.asList();
        }
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
    public List<SourceDocumentReference> findByRecordID(String recordID) {
        final Query<SourceDocumentReference> query = datastore.find(SourceDocumentReference.class, "referenceOwner.recordId", recordID);
        if(query == null) {return new ArrayList<>(0);}

        return query.asList();
    }

    @Override
    public List<SourceDocumentReference> deactivateDocuments (ReferenceOwner owner) {
        if (null == owner || (owner.equals(new ReferenceOwner()))) {
            throw new IllegalArgumentException("The reference owner cannot be null and must have at least one field not null");
        }

        final Query<SourceDocumentReference> query = datastore.createQuery(SourceDocumentReference.class);

        if (null != owner.getCollectionId()) {
            query.criteria("referenceOwner.collectionId").equal(owner.getCollectionId());
        }

        if (null != owner.getRecordId()) {
            query.criteria("referenceOwner.recordId").equal(owner.getRecordId());
        }

        if (null != owner.getProviderId()) {
            query.criteria("referenceOwner.providerId").equal(owner.getProviderId());
        }

        if (null != owner.getExecutionId()) {
            query.criteria("referenceOwner.executionId").equal(owner.getExecutionId());
        }

        final UpdateOperations<SourceDocumentReference> updateOperations = datastore.createUpdateOperations(SourceDocumentReference.class);

        updateOperations.set("active", false);

        datastore.update(query, updateOperations);

        return query.asList();

    }
}
