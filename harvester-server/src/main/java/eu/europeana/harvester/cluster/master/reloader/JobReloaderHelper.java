package eu.europeana.harvester.cluster.master.reloader;

import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceProcessingProfileDao;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.joda.time.DateTime;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobReloaderHelper {
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private final ProcessingJobDao processingJobDao;
    private final SourceDocumentReferenceProcessingProfileDao processingProfileDao;

    public JobReloaderHelper (SourceDocumentReferenceDao sourceDocumentReferenceDao, ProcessingJobDao processingJobDao,
                              SourceDocumentReferenceProcessingProfileDao processingProfileDao) {
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.processingJobDao = processingJobDao;
        this.processingProfileDao = processingProfileDao;
    }

    public void reloadJobs() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> newProcessingJobTuples = new ArrayList<>();

        for (final SourceDocumentReferenceProcessingProfile profile: processingProfileDao.getJobToBeEvaluated()) {
            newProcessingJobTuples.addAll(JobCreator.createJobs(profile.getReferenceOwner(),
                                                                sourceDocumentReferenceDao.read(profile.getId()).getUrl(),
                                                                profile.getUrlSourceType(), profile.getPriority(),
                                                                profile.getTaskType()));
        }


        for (final ProcessingJobTuple jobTuple: newProcessingJobTuples) {
            processingJobDao.createOrModify(jobTuple.getProcessingJob(), WriteConcern.NORMAL);
            sourceDocumentReferenceDao.createOrModify(jobTuple.getSourceDocumentReference(), WriteConcern.NORMAL);
            processingProfileDao.createOrModify(jobTuple.getSourceDocumentReferenceProcessingProfiles(), WriteConcern.NONE);
        }
    }
}
