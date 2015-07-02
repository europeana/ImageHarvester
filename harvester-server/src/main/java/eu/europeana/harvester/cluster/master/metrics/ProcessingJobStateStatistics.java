package eu.europeana.harvester.cluster.master.metrics;

import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import com.codahale.metrics.Gauge;
import eu.europeana.harvester.cluster.domain.messages.GetProcessingJobStatistics;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by salexandru on 29.06.2015.
 */
public class ProcessingJobStateStatistics extends UntypedActor {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private Cancellable schedule;
    private final int numberOfSecondsToDelay;
    private final ComputeProcessingJobStateStatistics computeProcessingJobStateStatistics;

    public ProcessingJobStateStatistics (SourceDocumentProcessingStatisticsDao processingStatistics,
                                         int numberOfSecondsToDelay) {
        if (numberOfSecondsToDelay <= 0) {
            throw new IllegalArgumentException("delay cannot be a negative number of zero");
        }

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
                return computeProcessingJobStateStatistics.getNumberOfJobsSuccessfulyFinished();
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
       return scheduleOnce(numberOfSecondsToDelay);
    }

    private Cancellable scheduleOnce(int delayInSeconds) {
        return getContext().system().scheduler().scheduleOnce(Duration.create(delayInSeconds, TimeUnit.MILLISECONDS),
                                                              getSelf(),
                                                              new GetProcessingJobStatistics(),
                                                              getContext().dispatcher(),
                                                              getSelf()
                                                             );
    }
}
