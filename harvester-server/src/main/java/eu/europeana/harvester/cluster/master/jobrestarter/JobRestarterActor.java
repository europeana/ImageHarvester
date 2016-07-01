package eu.europeana.harvester.cluster.master.jobrestarter;

import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.messages.ReloadJobs;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceProcessingProfileDao;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobRestarterActor extends UntypedActor {
    private Cancellable cancellable;

    private final JobRestarterConfig config;
    private final JobRestarterHelper helper;

    public JobRestarterActor (final JobRestarterConfig config,
                              final SourceDocumentReferenceDao documentReferenceDao,
                              final ProcessingJobDao processingJobDao,
                              final SourceDocumentReferenceProcessingProfileDao processingProfileDao)
    {
        if (null == config || null == processingProfileDao) {
            throw new IllegalArgumentException("JobRestarterActor: config and SourceDocumentReferenceDao cannot be null!");
        }

        this.config = config;
        helper = new JobRestarterHelper(documentReferenceDao, processingJobDao, processingProfileDao);
    }


    @Override
    public void preStart() {
        cancellable =scheduleOnce(/* two seconds delay */ 2);
    }

    @Override
    public void postStop() {
        if (null == cancellable) cancellable.cancel();
    }

    @Override
    public void onReceive (Object message) throws Exception {
        if (message instanceof ReloadJobs) {
            helper.reloadJobs();
            cancellable = scheduleOnce();
        }
    }

    private Cancellable scheduleOnce() {
        return scheduleOnce(config.getNumberOfSecondsBetweenRepetition().getStandardSeconds());
    }

    private Cancellable scheduleOnce(long delayInSeconds) {
        return getContext().system().scheduler().scheduleOnce(Duration.create(delayInSeconds, TimeUnit.SECONDS),
                getSelf(),
                new ReloadJobs(),
                getContext().dispatcher(),
                getSelf()
        );
    }
}
