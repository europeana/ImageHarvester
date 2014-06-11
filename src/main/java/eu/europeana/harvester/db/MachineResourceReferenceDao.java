package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.MachineResourceReference;

import java.util.List;

/**
 * DAO for CRUD with processing_limits collection
 */
public interface MachineResourceReferenceDao {

    /**
     * Persists a MachineResourceReference object
     * @param machineResourceReference - a new object
     */
    public void create(MachineResourceReference machineResourceReference);

    /**
     * Reads and returns a MachineResourceReference object
     * @param id the unique id of the record
     * @return - found MachineResourceReference object, it can be null
     */
    public MachineResourceReference read(String id);

    /**
     * Updates a MachineResourceReference record
     * @param machineResourceReference the modified MachineResourceReference
     * @return - success or failure
     */
    public boolean update(MachineResourceReference machineResourceReference);

    /**
     * Deletes a record from DB
     * @param machineResourceReference the unnecessary object
     * @return - success or failure
     */
    public boolean delete(MachineResourceReference machineResourceReference);

    /**
     * Modifies an existing MachineResourceReference record, if it doesn't exists then creates it.
     * @param machineResourceReference modified or new object
     */
    public void createOrModify(MachineResourceReference machineResourceReference);

    /**
     * Returns all the MachineResourceReferences from the DB
     * @return - list of MachineResourceReferences
     */
    public List<MachineResourceReference> getAllMachineResourceReferences();

}
