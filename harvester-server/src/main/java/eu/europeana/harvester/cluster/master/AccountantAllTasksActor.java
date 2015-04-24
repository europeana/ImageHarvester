package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.CleanUp;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AccountantAllTasksActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);


    /**
     * Maps all tasks ids with a pair of task and its state.
     */
    private final Map<String, Pair<RetrieveUrl, TaskState>> allTasks = new HashMap<>();
    private final Map<String,Long> allTasksTimer = new HashMap<>();


    @Override
    public void onReceive(Object message) throws Exception {
        try {
            if (message instanceof GetNumberOfTasks) {
                final Integer nr = allTasks.size();

                getSender().tell(nr, getSelf());
                return;
            }

            if (message instanceof IsJobLoaded) {
                Boolean wasAnyJob = false;

                for (final Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
                    final RetrieveUrl current = entry.getValue().getKey();
                    if (current.getJobId().equals(((IsJobLoaded) message).getJobID())) {
                        wasAnyJob = true;
                        break;
                    }
                }

                getSender().tell(wasAnyJob, getSelf());
                return;
            }

            if (message instanceof GetTask) {
                final String taskID = ((GetTask) message).getTaskID();
                if (allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();

                    getSender().tell(retrieveUrl, getSelf());
                    return;
                }
                RetrieveUrl retrieveUrl = new RetrieveUrl("", "", null, "", "", null, null, "");
                getSender().tell(retrieveUrl, getSelf());
                return;
            }

            if (message instanceof GetConcreteTask) {
                final String taskID = ((GetConcreteTask) message).getTaskID();
                if (allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();

                    getSender().tell(retrieveUrl.getDocumentReferenceTask(), getSelf());
                    return;
                }

                final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                        new ProcessingJobTaskDocumentReference(null, "", null);
                getSender().tell(processingJobTaskDocumentReference, getSelf());
                return;
            }

            if (message instanceof GetTaskState) {
                final String taskID = ((GetTaskState) message).getTaskID();
                if (allTasks.containsKey(taskID)) {
                    final TaskState state = allTasks.get(taskID).getValue();

                    getSender().tell(state, getSelf());
                    return;
                }
                getSender().tell(TaskState.DONE, getSelf());
                return;
            }

            if (message instanceof GetNumberOfParallelDownloadsPerIP) {
                final String IP = ((GetNumberOfParallelDownloadsPerIP) message).getIP();

                List<String> tasksFromIP = null; // to be received

                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                final ActorRef accountantPerIP = getContext().actorFor("../accountantPerIP");

                final Future<Object> future = Patterns.ask(accountantPerIP, new GetTasksFromIP(IP), timeout);

                try {
                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
                } catch (Exception e) {
                    LOG.error("Error: {}", e);
                }


                if (tasksFromIP == null) {
                    getSender().tell(0l, getSelf());
                    return;
                }

                Integer nr = 0;
                for (final String taskID : tasksFromIP) {
                    if (allTasks.containsKey(taskID) && allTasks.get(taskID).getValue().equals(TaskState.DOWNLOADING)) {
                        nr += 1;
                    }
                }

                getSender().tell(nr, getSelf());
                return;
            }

            if (message instanceof ModifyState) {
                final String taskID = ((ModifyState) message).getTaskID();
                final TaskState state = ((ModifyState) message).getState();

                if (allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();
                    allTasks.put(taskID, new Pair<>(retrieveUrl, state));
                    if ( state==TaskState.DOWNLOADING || state==TaskState.PROCESSING)
                        allTasksTimer.put(taskID, new Long(System.currentTimeMillis()));
                }
                return;
            }

            if (message instanceof AddTask) {
                final String taskID = ((AddTask) message).getTaskID();
                final Pair<RetrieveUrl, TaskState> taskWithState = ((AddTask) message).getTaskWithState();

                allTasks.put(taskID, taskWithState);
                return;
            }

            if (message instanceof RemoveTask) {
                final String taskID = ((RemoveTask) message).getTaskID();

                allTasks.remove(taskID);
                allTasksTimer.remove(taskID);

                return;
            }

            if (message instanceof Monitor) {
                monitor();
                return;
            }


            if (message instanceof Clean) {
                LOG.info("Clean maps accountant actor");
                allTasks.clear();

                return;
            }

            if (message instanceof CleanUp) {
                LOG.info("Clean old entries with processing or downloading state accountant actor");
                cleanTasks();
                getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                        TimeUnit.MINUTES), getSelf(), new CleanUp(), getContext().system().dispatcher(), getSelf());


                return;
            }

            if (message instanceof PauseTasks) {
                final String jobID = ((PauseTasks) message).getJobID();
                pauseTasks(jobID);

                return;
            }

            if (message instanceof ResumeTasks) {
                final String jobID = ((ResumeTasks) message).getJobID();
                resumeTasks(jobID);

                return;
            }

            if (message instanceof GetRetrieveUrl) {

                GetRetrieveUrl m = (GetRetrieveUrl) message;


                final String IP = m.getIP();
                final boolean isException = m.isIPException();
                final Long defaultLimit = m.getDefaultLimit();
                final int exceptionLimit = m.getExceptionLimit();
                final String taskID = m.getTaskID();

                RetrieveUrl retrieveUrl = new RetrieveUrl("", "", null, "", "", null, null, "");
                List<String> tasksFromIP = null;

                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                final ActorRef accountantPerIP = getContext().actorFor("../accountantPerIP");

                final Future<Object> future = Patterns.ask(accountantPerIP, new GetTasksFromIP(IP), timeout);

                try {
                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
                } catch (Exception e) {
                    LOG.error("Error: {}", e);
                }


                try {


                    int nr = 0;

                    if (tasksFromIP != null) {
                        for (final String task : tasksFromIP) {
                            if (allTasks.containsKey(task) && allTasks.get(task).getValue().equals(TaskState.DOWNLOADING)) {
                                nr += 1;
                            }
                        }
                    }


                    if ((!isException) && (nr >= defaultLimit)) {
                        getSender().tell(retrieveUrl, getSelf());
                        return;
                    }


                    if (isException && (nr > exceptionLimit)) {
                        getSender().tell(retrieveUrl, getSelf());
                        return;
                    }


                    TaskState state = TaskState.DONE;


                    if (allTasks.containsKey(taskID))
                        state = allTasks.get(taskID).getValue();


                    if (state == TaskState.READY) {
                        retrieveUrl = allTasks.get(taskID).getKey();
                        allTasks.put(taskID, new Pair<>(retrieveUrl, TaskState.DOWNLOADING));
                    }


                    getSender().tell(retrieveUrl, getSelf());
                    return;
                } catch (Exception e) {
                    LOG.error("Accountant actor, GetRetrieveUrl: {}", e.getMessage());

                    getSender().tell(retrieveUrl, getSelf());

                    return;
                }
            }
        } catch (Exception e) {
            LOG.error("Accountant actor: {}", e.getMessage());
        }
    }


    private void pauseTasks(final String jobID) {
        final List<String> IDs = new ArrayList<>();
        for (Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
            final RetrieveUrl current = entry.getValue().getKey();
            final TaskState state = entry.getValue().getValue();

            if (current.getJobId().equals(jobID) && (!(TaskState.DONE).equals(state))) {
                IDs.add(entry.getKey());
            }
        }

        for (final String ID : IDs) {
            final RetrieveUrl retrieveUrl = allTasks.get(ID).getKey();

            allTasks.put(ID, new Pair<>(retrieveUrl, TaskState.PAUSE));
        }
    }

    private void resumeTasks(final String jobID) {
        final List<String> IDs = new ArrayList<>();
        for (Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
            final RetrieveUrl current = entry.getValue().getKey();
            final TaskState state = entry.getValue().getValue();

            if (current.getJobId().equals(jobID) && (TaskState.PAUSE).equals(state)) {
                IDs.add(entry.getKey());
            }
        }

        for (final String id : IDs) {
            final RetrieveUrl retrieveUrl = allTasks.get(id).getKey();
            allTasks.put(id, new Pair<>(retrieveUrl, TaskState.READY));
        }
    }


    // to be calles every x minutes to do the cleanup
    // marks old tasks that aren't done yet as Done so they can be removed
    private void cleanTasks () {
        long currentTime = System.currentTimeMillis();
        for ( String taskID: allTasksTimer.keySet()) {
            Long sinceWhen = allTasksTimer.get(taskID);
            if ( currentTime-sinceWhen.longValue() > 30*60*1000 ) {
                if (allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();
                    allTasks.remove(taskID);
                }
            }
        }
    }


    private void monitor() {
        LOG.info("Number of loaded tasks: {}", allTasks.size());
    }

}

