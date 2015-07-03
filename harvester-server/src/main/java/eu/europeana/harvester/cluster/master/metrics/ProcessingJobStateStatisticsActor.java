package eu.europeana.harvester.cluster.master.metrics;

import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import com.codahale.metrics.Gauge;
import eu.europeana.harvester.cluster.domain.messages.GetProcessingJobStatistics;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by salexandru on 29.06.2015.
 */
public class ProcessingJobStateStatisticsActor extends UntypedActor {
    private Cancellable schedule;
    private final org.joda.time.Duration numberOfSecondsToDelay;
    private final ComputeProcessingJobStateStatistics computeProcessingJobStateStatistics;

    public ProcessingJobStateStatisticsActor (SourceDocumentProcessingStatisticsDao processingStatistics,
                                              org.joda.time.Duration numberOfSecondsToDelay) {
        if (null == processingStatistics) {
            throw new IllegalArgumentException("the dao for processingStatistics cannot be null");
        }

        this.numberOfSecondsToDelay = numberOfSecondsToDelay;
        computeProcessingJobStateStatistics = new ComputeProcessingJobStateStatistics(processingStatistics);

        MasterMetrics.Master.jobsPersistenceErrorCount.registerHandler(new Gauge<Long>() {

            @Override
            public Long getValue () {
                return computeProcessingJobStateStatistics.getNumberOfJobsWithError();
            }
        });

        MasterMetrics.Master.jobsPersistenceFinishedWithSuccessCount.registerHandler(new Gauge<Long>() {

            @Override
            public Long getValue () {
                return computeProcessingJobStateStatistics.getNumberOfJobsSuccessfullyFinished();
            }
        });

        MasterMetrics.Master.jobsPersistenceReadyCount.registerHandler(new Gauge<Long>() {

            @Override
            public Long getValue () {
                return computeProcessingJobStateStatistics.getNumberOfJobsReady();
            }
        });
    }


    @Override
    public void preStart() {
        schedule = scheduleOnce(2);
    }


    @Override
    public void postStop() {
        if (null != schedule) {
            schedule.cancel();
        }
    }

    @Override
    public void onReceive (Object message) throws Exception {
        if (message instanceof GetProcessingJobStatistics) {
           computeProcessingJobStateStatistics.run();
           schedule = scheduleOnce();
        }
    }

    private Cancellable scheduleOnce() {
       return scheduleOnce(numberOfSecondsToDelay.getStandardSeconds());
    }

    private Cancellable scheduleOnce(long delayInSeconds) {
        return getContext().system().scheduler().scheduleOnce(Duration.create(delayInSeconds, TimeUnit.SECONDS),
                                                              getSelf(),
                                                              new GetProcessingJobStatistics(),
                                                              getContext().dispatcher(),
                                                              getSelf()
                                                             );
    }
}
