package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.domain.MachineResourceReference;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with processing_limits collection
 */
public class MachineResourceReferenceDaoImpl implements MachineResourceReferenceDao {

    private final Datastore datastore;

    public MachineResourceReferenceDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public void create(MachineResourceReference processingLimits) {
        datastore.save(processingLimits);
    }

    @Override
    public MachineResourceReference read(String id) {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.criteria("id").equal(id);

        List<MachineResourceReference> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(MachineResourceReference machineResourceReference) {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.criteria("id").equal(machineResourceReference.getId());

        List<MachineResourceReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(machineResourceReference);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(MachineResourceReference machineResourceReference) {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.criteria("id").equal(machineResourceReference.getId());

        List<MachineResourceReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(machineResourceReference);

            return true;
        }

        return false;
    }

    @Override
    public void createOrModify(MachineResourceReference machineResourceReference) {
        if(!update(machineResourceReference)) {
            create(machineResourceReference);
        }
    }

    @Override
    public List<MachineResourceReference> getAllMachineResourceReferences() {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);

        return query.asList();
    }

}
