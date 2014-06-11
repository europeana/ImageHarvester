package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.MachineResourceReferenceStatDao;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;

import java.util.List;

public class MachineResourceReferenceStatDaoImpl implements MachineResourceReferenceStatDao {

    private final Datastore datastore;

    public MachineResourceReferenceStatDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public void create(MachineResourceReferenceStat machineResourceReferenceStat) {
        datastore.save(machineResourceReferenceStat);
    }

    @Override
    public MachineResourceReferenceStat read(String id) {
        Query<MachineResourceReferenceStat> query = datastore.find(MachineResourceReferenceStat.class);
        query.criteria("id").equal(id);

        List<MachineResourceReferenceStat> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(MachineResourceReferenceStat machineResourceReferenceStat) {
        Query<MachineResourceReferenceStat> query = datastore.find(MachineResourceReferenceStat.class);
        query.criteria("id").equal(machineResourceReferenceStat.getId());

        List<MachineResourceReferenceStat> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(machineResourceReferenceStat);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(MachineResourceReferenceStat machineResourceReferenceStat) {
        Query<MachineResourceReferenceStat> query = datastore.find(MachineResourceReferenceStat.class);
        query.criteria("id").equal(machineResourceReferenceStat.getId());

        List<MachineResourceReferenceStat> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(machineResourceReferenceStat);

            return true;
        }

        return false;
    }
}
