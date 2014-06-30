package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
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
    public boolean create(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        if(read(machineResourceReference.getId()) == null) {
            datastore.save(machineResourceReference, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void createOrModify(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        datastore.save(machineResourceReference, writeConcern);
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
    public boolean update(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.criteria("id").equal(machineResourceReference.getId());

        List<MachineResourceReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.save(machineResourceReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(MachineResourceReference machineResourceReference, WriteConcern writeConcern) {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);
        query.criteria("id").equal(machineResourceReference.getId());

        List<MachineResourceReference> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(machineResourceReference, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public List<MachineResourceReference> getAllMachineResourceReferences() {
        Query<MachineResourceReference> query = datastore.find(MachineResourceReference.class);

        return query.asList();
    }

}
