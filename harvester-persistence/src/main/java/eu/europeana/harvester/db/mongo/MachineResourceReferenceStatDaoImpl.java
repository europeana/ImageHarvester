package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.MachineResourceReferenceStatDao;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;

/**
 * MongoDB DAO implementation for CRUD with machine_resource_reference_stat collection
 */
public class MachineResourceReferenceStatDaoImpl implements MachineResourceReferenceStatDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public MachineResourceReferenceStatDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(MachineResourceReferenceStat machineResourceReferenceStat, WriteConcern writeConcern) {
        if(read(machineResourceReferenceStat.getId()) == null) {
            datastore.save(machineResourceReferenceStat, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public MachineResourceReferenceStat read(String id) {
        return datastore.get(MachineResourceReferenceStat.class, id);
    }

    @Override
    public boolean update(MachineResourceReferenceStat machineResourceReferenceStat, WriteConcern writeConcern) {
        if(read(machineResourceReferenceStat.getId()) != null) {
            datastore.save(machineResourceReferenceStat, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(MachineResourceReferenceStat.class, id);
    }
}
