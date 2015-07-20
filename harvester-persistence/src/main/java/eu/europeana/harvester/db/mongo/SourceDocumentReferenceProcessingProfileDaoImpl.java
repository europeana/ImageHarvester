package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceProcessingProfileDao;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;

import java.util.*;

/**
 * Created by salexandru on 17.07.2015.
 */
public class SourceDocumentReferenceProcessingProfileDaoImpl implements SourceDocumentReferenceProcessingProfileDao {
    private final Datastore datastore;

    public SourceDocumentReferenceProcessingProfileDaoImpl (Datastore datastore) {this.datastore = datastore;}

    @Override
    public boolean create (SourceDocumentReferenceProcessingProfile sourceDocumentReferenceProcessingProfile,
                           WriteConcern writeConcern) {
        if (null == read(sourceDocumentReferenceProcessingProfile.getId())) {
            datastore.save(sourceDocumentReferenceProcessingProfile, writeConcern);
            return true;
        }
        return false;
    }

    @Override
    public void createOrModify (SourceDocumentReferenceProcessingProfile sourceDocumentReferenceProcessingProfile,
                                WriteConcern writeConcern) {
        datastore.save(sourceDocumentReferenceProcessingProfile, writeConcern);
    }

    @Override
    public Iterable<Key<SourceDocumentReferenceProcessingProfile>> createOrModify (Collection<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles,
                                                                                   WriteConcern writeConcern) {
        return datastore.save(sourceDocumentReferenceProcessingProfiles, writeConcern);
    }

    @Override
    public SourceDocumentReferenceProcessingProfile read (String id) {
        return datastore.get (SourceDocumentReferenceProcessingProfile.class, id);
    }

    @Override
    public List<SourceDocumentReferenceProcessingProfile> read (List<String> ids) {
        return null == ids || (0 == ids.size()) ?
                       Collections.EMPTY_LIST :
                       datastore.get(SourceDocumentReferenceProcessingProfile.class, ids).asList();
    }

    @Override
    public boolean update (SourceDocumentReferenceProcessingProfile sourceDocumentReferenceProcessingProfile,
                           WriteConcern writeConcern) {
        if (null != read(sourceDocumentReferenceProcessingProfile.getId())) {
            datastore.save(sourceDocumentReferenceProcessingProfile, writeConcern);
        }
        return false;
    }

    @Override
    public WriteResult delete (String id) {
        return datastore.delete(SourceDocumentReferenceProcessingProfile.class, id);
    }

    @Override
    public List<SourceDocumentReferenceProcessingProfile> findByRecordID (String recordID) {
        return datastore.find(SourceDocumentReferenceProcessingProfile.class, "referenceOwner.recordId", recordID).asList();
    }

    @Override
    public List<SourceDocumentReferenceProcessingProfile> getJobToBeEvaluated () {
        final Query<SourceDocumentReferenceProcessingProfile> query = datastore.createQuery(SourceDocumentReferenceProcessingProfile.class);

        query.criteria("toBeEvaluatedAt").lessThan(new Date());

        final List<SourceDocumentReferenceProcessingProfile> profiles = query.asList();
        final Iterator<SourceDocumentReferenceProcessingProfile> profileIterator = profiles.listIterator();

        while (profileIterator.hasNext()) {
            if (!profileIterator.next().getActive()) {
                profileIterator.remove();
            }
        }

        return profiles;
    }

    @Override
    public List<SourceDocumentReferenceProcessingProfile> deactivateDocuments (ReferenceOwner owner) {
        if (null == owner || (owner.equals(new ReferenceOwner()))) {
            throw new IllegalArgumentException("The reference owner cannot be null and must have at least one field not null");
        }

        final Query<SourceDocumentReferenceProcessingProfile> query = datastore.createQuery(SourceDocumentReferenceProcessingProfile.class);

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

        final UpdateOperations<SourceDocumentReferenceProcessingProfile> updateOperations = datastore.createUpdateOperations(SourceDocumentReferenceProcessingProfile.class);

        updateOperations.set("active", false);

        datastore.update(query, updateOperations);

        return query.asList();
    }
}
