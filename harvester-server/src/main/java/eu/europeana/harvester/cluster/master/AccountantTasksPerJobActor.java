package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AccountantTasksPerJobActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Maps each job with a list of tasks.
     */
    private final Map<String, List<String>> tasksPerJob = new HashMap<>();


    @Override
    public void onReceive(Object message) throws Exception {
        try {

            if (message instanceof GetTasksFromJob) {
                final String jobID = ((GetTasksFromJob) message).getJobID();
                List<String> tasks = tasksPerJob.get(jobID);
                if(tasks == null) {
                    tasks = new ArrayList<>();
                }

                getSender().tell(tasks, getSelf());
                return;
            }

            if (message instanceof GetTaskStatesPerJob) {
                final String jobID = ((GetTaskStatesPerJob) message).getJobID();

                final List<String> taskIDs = tasksPerJob.get(jobID);
                final List<TaskState> states = new ArrayList<>();

                if(taskIDs == null) {getSender().tell(new ArrayList<>(), getSelf()); return;}

                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                final ActorRef accountantAll = getContext().actorFor("../accountantAll");

                for(final String taskID : taskIDs) {
                    final Future<Object> future = Patterns.ask(accountantAll, new GetTaskState(taskID), timeout);
                    TaskState taskState = null;
                    try {
                        taskState = (TaskState) Await.result(future, timeout.duration());
                    } catch (Exception e) {
                        LOG.error("Error: {}", e);
                    }

                    states.add(taskState);

                }

                getSender().tell(states, getSelf());
                return;
            }

            if (message instanceof AddTasksToJob) {
                final String jobID = ((AddTasksToJob) message).getJobID();
                final List<String> tasksIDs = ((AddTasksToJob) message).getTaskIDs();

                tasksPerJob.put(jobID, tasksIDs);
                return;
            }

            if (message instanceof RemoveJob) {
                final String jobID = ((RemoveJob) message).getJobID();

                tasksPerJob.remove(jobID);
                return;
            }

            if (message instanceof Clean) {
                LOG.info("Clean map Tasks/Job accountant actor");
                tasksPerJob.clear();
                return;
            }

        } catch(Exception e) {
            //e.printStackTrace();
            LOG.error("Tasks/Job Accountant actor: {}", e.getMessage());
        }
    }

}
