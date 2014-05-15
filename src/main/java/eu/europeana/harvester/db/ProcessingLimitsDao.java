package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.ProcessingLimits;

/**
 * DAO for CRUD with processing_limits collection
 */
public interface ProcessingLimitsDao {

    public void create(ProcessingLimits processingLimits);

    public ProcessingLimits read(String id);

    public boolean update(ProcessingLimits processingLimits);

    public boolean delete(ProcessingLimits processingLimits);

    public void createOrModify(ProcessingLimits processingLimits);

    public ProcessingLimits findByCollectionId(Long id);

}
