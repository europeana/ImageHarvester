package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.MachineResourceReferenceStat;

public interface MachineResourceReferenceStatDao {

    /**
     * Persists a MachineResourceReferenceStat object
     * @param machineResourceReferenceStat - a new object
     */
    public void create(MachineResourceReferenceStat machineResourceReferenceStat);

    /**
     * Reads and returns a MachineResourceReferenceStat object
     * @param id the unique id of the record
     * @return - found MachineResourceReferenceStat object, it can be null
     */
    public MachineResourceReferenceStat read(String id);

    /**
     * Updates a MachineResourceReferenceStat record
     * @param machineResourceReferenceStat the modified MachineResourceReferenceStat
     * @return - success or failure
     */
    public boolean update(MachineResourceReferenceStat machineResourceReferenceStat);

    /**
     * Deletes a record from DB
     * @param machineResourceReferenceStat the unnecessary object
     * @return - success or failure
     */
    public boolean delete(MachineResourceReferenceStat machineResourceReferenceStat);

}
