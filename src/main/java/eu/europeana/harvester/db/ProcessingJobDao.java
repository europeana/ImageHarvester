package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.ProcessingJob;

import java.util.List;

/**
 * DAO for CRUD with processing_job collection
 */
public interface ProcessingJobDao {

    public void create(ProcessingJob processingJob);

    public ProcessingJob read(String id);

    public boolean update(ProcessingJob processingJob);

    public boolean delete(ProcessingJob processingJob);

    public List<ProcessingJob> getAllJobs();

}
