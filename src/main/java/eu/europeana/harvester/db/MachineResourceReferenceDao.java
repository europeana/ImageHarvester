package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.MachineResourceReference;

import java.util.List;

/**
 * DAO for CRUD with processing_limits collection
 */
public interface MachineResourceReferenceDao {

    /**
     * Persists a MachineResourceReference object
     * @param machineResourceReference - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(MachineResourceReference machineResourceReference, WriteConcern writeConcern);

    /**
     * Modifies an existing MachineResourceReference record, if it doesn't exists then creates it.
     * @param machineResourceReference modified or new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     */
    public void createOrModify(MachineResourceReference machineResourceReference, WriteConcern writeConcern);

    /**
     * Reads and returns a MachineResourceReference object
     * @param id the unique id of the record
     * @return - found MachineResourceReference object, it can be null
     */
    public MachineResourceReference read(String id);

    /**
     * Updates a MachineResourceReference record
     * @param machineResourceReference the modified MachineResourceReference
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(MachineResourceReference machineResourceReference, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param machineResourceReference the unnecessary object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean delete(MachineResourceReference machineResourceReference, WriteConcern writeConcern);

    /**
     * Returns all the MachineResourceReferences from the DB
     * @return - list of MachineResourceReferences
     */
    public List<MachineResourceReference> getAllMachineResourceReferences();

}
