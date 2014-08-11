package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;

/**
 * DAO for CRUD with machine_resource_reference_stat collection
 */
public interface MachineResourceReferenceStatDao {

    /**
     * Persists a MachineResourceReferenceStat object
     * @param machineResourceReferenceStat - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(MachineResourceReferenceStat machineResourceReferenceStat, WriteConcern writeConcern);

    /**
     * Reads and returns a MachineResourceReferenceStat object
     * @param id the unique id of the record
     * @return - found MachineResourceReferenceStat object, it can be null
     */
    public MachineResourceReferenceStat read(String id);

    /**
     * Updates a MachineResourceReferenceStat record
     * @param machineResourceReferenceStat the modified MachineResourceReferenceStat
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(MachineResourceReferenceStat machineResourceReferenceStat, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    public WriteResult delete(String id);

}
