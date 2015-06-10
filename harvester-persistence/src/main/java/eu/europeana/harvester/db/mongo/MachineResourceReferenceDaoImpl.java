package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.domain.MachineResourceReference;
import eu.europeana.harvester.domain.Page;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with machine_resource_reference collection
 */
public class MachineResourceReferenceDaoImpl implements MachineResourceReferenceDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public MachineResourceReferenceDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        if(read(machineResourceReference.getId()) == null) {
            datastore.save(machineResourceReference, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public com.google.code.morphia.Key<MachineResourceReference> createOrModify(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        return datastore.save(machineResourceReference, writeConcern);
    }

    @Override
    public Iterable<com.google.code.morphia.Key<MachineResourceReference>> createOrModify(Collection<MachineResourceReference> machineResourceReferences, WriteConcern writeConcern) {
        if (null == machineResourceReferences || machineResourceReferences.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(machineResourceReferences, writeConcern);
    }

    @Override
    public MachineResourceReference read(String id) {
        return datastore.get(MachineResourceReference.class, id);
    }

    @Override
    public boolean update(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        if(read(machineResourceReference.getId()) != null) {
            datastore.save(machineResourceReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(MachineResourceReference.class, id);
    }

    @Override
    public List<MachineResourceReference> getAllMachineResourceReferences(Page page) {
        final Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.offset(page.getFrom());
        query.limit(page.getLimit());

        return query.asList();
    }

}
