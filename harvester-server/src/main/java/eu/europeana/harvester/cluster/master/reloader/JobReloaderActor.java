package eu.europeana.harvester.cluster.master.reloader;

import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.messages.ReloadJobs;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceProcessingProfileDao;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobReloaderActor extends UntypedActor {
    private Cancellable cancellable;

    private final JobReloaderConfig config;
    private final JobReloaderHelper helper;

    public JobReloaderActor (final JobReloaderConfig config,
                             final SourceDocumentReferenceProcessingProfileDao processingProfileDao)
    {
        if (null == config || null == processingProfileDao) {
            throw new IllegalArgumentException("JobReloaderActor: config and processingProfileDao cannot be null!");
        }

        this.config = config;
        helper = new JobReloaderHelper(sourceDocumentReferenceDao, processingJobDao, processingProfileDao);
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
       return scheduleOnce(config.getNumberOfSeconds().getStandardSeconds());
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
