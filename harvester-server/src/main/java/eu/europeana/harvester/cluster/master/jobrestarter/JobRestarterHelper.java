package eu.europeana.harvester.cluster.master.jobrestarter;

import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceProcessingProfileDao;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobRestarterHelper {
    private static Logger LOG = LogManager.getLogger(JobRestarterHelper.class.getName());

    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private final ProcessingJobDao processingJobDao;
    private final SourceDocumentReferenceProcessingProfileDao processingProfileDao;

    public JobRestarterHelper (SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               ProcessingJobDao processingJobDao,
                               SourceDocumentReferenceProcessingProfileDao processingProfileDao) {
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.processingProfileDao = processingProfileDao;
        this.processingJobDao = processingJobDao;
    }

    public void reloadJobs() throws ExecutionException, IOException {
        final List<ProcessingJobTuple> newProcessingJobTuples = new ArrayList<>();

        for (final SourceDocumentReferenceProcessingProfile profile: processingProfileDao.getJobToBeEvaluated()) {
            newProcessingJobTuples.addAll(JobCreator.createJobs(profile.getReferenceOwner(),
                                                                sourceDocumentReferenceDao.read(profile.getSourceDocumentReferenceId()),
                                                                profile.getUrlSourceType(),
                                                                profile.getPriority(),
                                                                profile.getTaskType()
                                                               )
                                         );
        }

        for (final ProcessingJobTuple jobTuple: newProcessingJobTuples) {
            processingJobDao.createOrModify(jobTuple.getProcessingJob(), WriteConcern.ACKNOWLEDGED);
            for (final SourceDocumentReferenceProcessingProfile profile: jobTuple.getSourceDocumentReferenceProcessingProfiles()) {
                //should never happen
                if (!processingProfileDao.update(profile, WriteConcern.ACKNOWLEDGED)) {
                   LOG.error(profile.getId() + " " + profile.getSourceDocumentReferenceId() + " " + profile.getTaskType());
                }
            }
        }
    }
}
