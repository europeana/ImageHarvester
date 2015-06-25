package eu.europeana.harvester.cluster.master.accountants;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.CleanUp;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.ProcessingJobLimits;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AccountantAllTasksActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());


    /**
     * Maps all tasks ids with a pair of task and its state.
     */
    private final Map<String, Pair<RetrieveUrl, TaskState>> allTasks = new HashMap<>();
    private final Map<String,Long> allTasksTimer = new HashMap<>();


    @Override
    public void onReceive(Object message) throws Exception {
        try {
            if (message instanceof GetNumberOfTasks) {
                //final Integer nr = allTasks.size();
                Integer nr = 0;
                for (final Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
                    final TaskState current = entry.getValue().getValue();
                    if (current.equals(TaskState.READY) || current.equals(TaskState.DOWNLOADING) )
                        nr++;

                }

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
                ReferenceOwner referenceOwner = null;
                if (allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();
                    referenceOwner = retrieveUrl.getReferenceOwner();
                    getSender().tell(retrieveUrl, getSelf());
                    return;
                }
                RetrieveUrl retrieveUrl = new RetrieveUrl("",  new ProcessingJobLimits(), null, "", "", null, null, "",referenceOwner);
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

//            if (message instanceof GetNumberOfParallelDownloadsPerIP) {
//                final String IP = ((GetNumberOfParallelDownloadsPerIP) message).getIP();
//
//                List<String> tasksFromIP = null; // to be received
//
//                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
//                final ActorRef accountantPerIP = getContext().actorFor("../accountantPerIP");
//
//                final Future<Object> future = Patterns.ask(accountantPerIP, new GetTasksFromIP(IP), timeout);
//
//                try {
//                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
//                } catch (Exception e) {
//                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
//                            "Error while waiting to receive tasks per IP.",e);
//                    // TODO : Evaluate if it is acceptable to hide the exception here.
//                }
//
//
//                if (tasksFromIP == null) {
//                    getSender().tell(0l, getSelf());
//                    return;
//                }
//
//                Integer nr = 0;
//                for (final String taskID : tasksFromIP) {
//                    if (allTasks.containsKey(taskID) && allTasks.get(taskID).getValue().equals(TaskState.DOWNLOADING)) {
//                        nr += 1;
//                    }
//                }
//
//                getSender().tell(nr, getSelf());
//                return;
//            }

            if (message instanceof ModifyState) {
                final String taskID = ((ModifyState) message).getTaskID();

                if (allTasks.containsKey(taskID)) {
                    final String jobID = ((ModifyState) message).getJobId();
                    final String IP = ((ModifyState) message).getIP();
                    final TaskState state = ((ModifyState) message).getState();
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();
                    allTasks.put(taskID, new Pair<>(retrieveUrl, state));
                    if ( state==TaskState.DOWNLOADING || state==TaskState.PROCESSING)
                        allTasksTimer.put(taskID, new Long(System.currentTimeMillis()));

                    if ( state==TaskState.DONE ) {
                        // we check if all tasks from that job are done
                        // if true, remove the job
                        List<String> tasks = new ArrayList<>();
                        final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                        final ActorRef accountantJobs = getContext().actorFor("../accountantPerJob");
                        Future<Object> future = Patterns.ask(accountantJobs, new GetTasksFromJob(jobID), timeout);
                        try {
                            tasks = (List<String>) Await.result(future, timeout.duration());
                        } catch (Exception e) {
                            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                                    "Error at markDone->ModifyState.", e);
                            // TODO : Investigate if it make sense to hide the exception here.
                        }

                        boolean foundTask = false ;
                        for ( String task: tasks) {
                            if ( allTasks.get(task).getValue() != TaskState.DONE )
                                foundTask = true;
                        }
                        // ModifyState with task status done should be called from a future, so we bang back the answer
                        getSender().tell(new Boolean(foundTask), ActorRef.noSender());
                        // at this moment, if foundTask is false, it means all tasks from that specific job are done
                        // we signal that back to the asker and remove the job and it's subsequent tasks
                        if (!foundTask) {
                            final ActorRef accountantIP = getContext().actorFor("../accountantPerJob");
                            accountantJobs.tell(new RemoveJob(jobID, ""), ActorRef.noSender());
                            accountantIP.tell(new RemoveTaskFromIP(taskID, IP), ActorRef.noSender());
                            for (String task : tasks) {
                                allTasks.remove(task);
                                allTasksTimer.remove(taskID);
                            }
                        }


                    }
                }
                return;
            }

            if (message instanceof AddTask) {
                final String taskID = ((AddTask) message).getTaskID();
                final Pair<RetrieveUrl, TaskState> taskWithState = ((AddTask) message).getTaskWithState();

                allTasks.put(taskID, taskWithState);
                final ActorRef accountantPerIP = getContext().actorFor("../accountantPerIP");
                final RetrieveUrl retrieveUrl = taskWithState.getKey();
                accountantPerIP.tell( new AddTasksToIP(retrieveUrl.getIpAddress(), retrieveUrl.getId()), ActorRef.noSender());

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
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
                        "Clean maps accountant actor");
                allTasks.clear();

                return;
            }

            if (message instanceof CleanUp) {
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
                        "Clean old entries with processing or downloading state accountant actor");

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

                RetrieveUrl retrieveUrl = new RetrieveUrl("", "", null, new ProcessingJobLimits(), "", "", null, null, "",
                        new ReferenceOwner("unknown","unknown","unknown"));
//                List<String> tasksFromIP = null;
//
//                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
//                final ActorRef accountantPerIP = getContext().actorFor("../accountantPerIP");
//
//                final Future<Object> future = Patterns.ask(accountantPerIP, new GetTasksFromIP(IP), timeout);
//
//                try {
//                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
//                } catch (Exception e) {
//                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
//                            "Exception while waiting for answer for getting tasks per IP.", e);
//                    // TODO : Evaluate if it is acceptable to hide the exception here.
//                }
//
//
//                try {
//
//
//                    int nr = 0;
//
//                    if (tasksFromIP != null) {
//                        for (final String task : tasksFromIP) {
//                            if (allTasks.containsKey(task) && allTasks.get(task).getValue().equals(TaskState.DOWNLOADING)) {
//                                nr += 1;
//                            }
//                        }
//                    }
//
//
//                    if ((!isException) && (nr >= defaultLimit)) {
//                        getSender().tell(retrieveUrl, getSelf());
//                        return;
//                    }
//
//
//                    if (isException && (nr > exceptionLimit)) {
//                        getSender().tell(retrieveUrl, getSelf());
//                        return;
//                    }

                try {
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
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
                            "Exception related to GetRetrieveUrl.", e);
                    getSender().tell(retrieveUrl, getSelf());

                    return;
                }
            }
        } catch (Exception e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
                    "General exception in accountant actor.", e);
            // TODO : Evaluate if it is acceptable to hide the exception here.;
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

