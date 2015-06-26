package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.GetTask;
import eu.europeana.harvester.cluster.domain.messages.inner.GetTaskStatesPerJob;
import eu.europeana.harvester.cluster.domain.messages.inner.MarkJobAsDone;
import eu.europeana.harvester.cluster.domain.messages.inner.RemoveJob;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReceiverJobDumperActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;




    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;





    public ReceiverJobDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                  final ActorRef accountantActor,
                                  final ProcessingJobDao processingJobDao){
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverJobDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.processingJobDao = processingJobDao;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if (message instanceof MarkJobAsDone) {
            String jobId = ((MarkJobAsDone)message).getJobID();
            markDone(jobId);

        }

        return;
    }



    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(String jobId) {
        final ProcessingJob processingJob = processingJobDao.read(jobId);
        final ProcessingJob newProcessingJob = processingJob.withState(JobState.FINISHED);
        processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());
    }


}
