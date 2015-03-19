package eu.europeana.harvester.cluster.master;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccountantMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Maps each task with their allowed download speed.
     */
    private final Map<String, Long> speedPerTask = new HashMap<>();

    /**
     * Maps each job with a list of tasks.
     */
    private final Map<String, List<String>> tasksPerJob = new HashMap<>();

    /**
     * Maps all tasks ids with a pair of task and its state.
     */
    private final Map<String, Pair<RetrieveUrl, TaskState>> allTasks = new HashMap<>();

    /**
     * Map which stores a list of tasks for each IP address.
     */
    private final Map<String, List<String>> tasksPerIP = new HashMap<>();

    @Override
    public void onReceive(Object message) throws Exception {
        try {
            if (message instanceof GetNumberOfIPs) {
                final Integer nr = tasksPerIP.size();

                getSender().tell(nr, getSelf());
                return;
            }
            if (message instanceof GetNumberOfTasks) {
                final Integer nr = allTasks.size();

                getSender().tell(nr, getSelf());
                return;
            }
            if (message instanceof CheckIPsWithJobs) {
                final Double percentage = checkIPsWithJobs(((CheckIPsWithJobs) message).getIpsWithJobs());

                getSender().tell(percentage, getSelf());
                return;
            }
            if (message instanceof IsJobLoaded) {
                Boolean wasAnyJob = false;

                for(final Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
                    final RetrieveUrl current = entry.getValue().getKey();
                    if(current.getJobId().equals(((IsJobLoaded) message).getJobID())) {
                        wasAnyJob = true;
                        break;
                    }
                }

                getSender().tell(wasAnyJob, getSelf());
                return;
            }
            if (message instanceof GetTask) {
                final String taskID = ((GetTask) message).getTaskID();
                if(allTasks.containsKey(taskID)) {
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
                if(allTasks.containsKey(taskID)) {
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
                if(allTasks.containsKey(taskID)) {
                    final TaskState state = allTasks.get(taskID).getValue();

                    getSender().tell(state, getSelf());
                    return;
                }
                getSender().tell(TaskState.DONE, getSelf());
                return;
            }
            if (message instanceof GetTasksFromIP) {
                final String IP = ((GetTasksFromIP) message).getIP();
                List<String> tasks = tasksPerIP.get(IP);
                if(tasks == null) {
                    tasks = new ArrayList<>();
                }

                getSender().tell(tasks, getSelf());
                return;
            }
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
                for(final String taskID : taskIDs) {
                    if(allTasks.containsKey(taskID)) {
                        final TaskState requestState = allTasks.get(taskID).getValue();
                        states.add(requestState);
                    }
                }

                getSender().tell(states, getSelf());
                return;
            }
            if (message instanceof GetDownloadSpeed) {
                final String taskID = ((GetDownloadSpeed) message).getTaskID();

                if(speedPerTask.containsKey(taskID)) {
                    final Long speed = speedPerTask.get(taskID);

                    getSender().tell(speed, getSelf());
                } else {
                    getSender().tell(0l, getSelf());
                }
                return;
            }
            if (message instanceof GetNumberOfParallelDownloadsPerIP) {
                final String IP = ((GetNumberOfParallelDownloadsPerIP) message).getIP();
                final List<String> tasksFromIP = tasksPerIP.get(IP);

                if(tasksFromIP == null) {getSender().tell(0l, getSelf()); return;}

                Integer nr = 0;
                for(final String taskID : tasksFromIP) {
                    if(allTasks.containsKey(taskID) && allTasks.get(taskID).getValue().equals(TaskState.DOWNLOADING)) {
                        nr += 1;
                    }
                }

                getSender().tell(nr, getSelf());
                return;
            }
            if (message instanceof ModifyState) {
                final String taskID = ((ModifyState) message).getTaskID();
                final TaskState state = ((ModifyState) message).getState();

                if(allTasks.containsKey(taskID)) {
                    final RetrieveUrl retrieveUrl = allTasks.get(taskID).getKey();
                    allTasks.put(taskID, new Pair<>(retrieveUrl, state));
                }
                return;
            }
            if (message instanceof AddDownloadSpeed) {
                final String taskID = ((AddDownloadSpeed) message).getTaskID();
                final Long speed = ((AddDownloadSpeed) message).getSpeed();

                speedPerTask.put(taskID, speed);
                return;
            }
            if (message instanceof AddTask) {
                final String taskID = ((AddTask) message).getTaskID();
                final Pair<RetrieveUrl, TaskState> taskWithState = ((AddTask) message).getTaskWithState();

                allTasks.put(taskID, taskWithState);
                return;
            }
            if (message instanceof AddTasksToJob) {
                final String jobID = ((AddTasksToJob) message).getJobID();
                final List<String> tasksIDs = ((AddTasksToJob) message).getTaskIDs();

                tasksPerJob.put(jobID, tasksIDs);
                return;
            }
            if (message instanceof AddTasksToIP) {
                final String IP = ((AddTasksToIP) message).getIP();
                final List<String> taskIDs = ((AddTasksToIP) message).getTasks();

                tasksPerIP.put(IP, taskIDs);
                return;
            }
            if (message instanceof RemoveDownloadSpeed) {
                final String taskID = ((RemoveDownloadSpeed) message).getTaskID();

                speedPerTask.remove(taskID);
                return;
            }
            if (message instanceof RemoveJob) {
                final String jobID = ((RemoveJob) message).getJobID();

                tasksPerJob.remove(jobID);
                return;
            }
            if (message instanceof RemoveTask) {
                final String taskID = ((RemoveTask) message).getTaskID();

                allTasks.remove(taskID);
                return;
            }
            if (message instanceof RemoveTaskFromIP) {
                final String taskID = ((RemoveTaskFromIP) message).getTaskID();
                final String IP = ((RemoveTaskFromIP) message).getIP();

                if(!tasksPerIP.containsKey(IP)) {return;}
                final List<String> taskFromIP = tasksPerIP.get(IP);
                taskFromIP.remove(taskID);

                tasksPerIP.put(IP, taskFromIP);
                return;
            }
            if (message instanceof Monitor) {
                monitor();
                return;
            }
            if (message instanceof CleanIPs) {
                cleanIPs();

                return;
            }
            if (message instanceof Clean) {
                LOG.info("Clean maps accountant actor");
                speedPerTask.clear();
                tasksPerJob.clear();
                allTasks.clear();
                tasksPerIP.clear();

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
        } catch(Exception e) {
            e.printStackTrace();
            LOG.error("Accountant actor: {}", e.getMessage());
        }
    }

    /**
     * Checks how much percent of IPs has loaded tasks.
     * @return the calculated percentage
     */
    private Double checkIPsWithJobs(HashMap<String, Boolean> ipsWithJobs) {
        final int nrOfIPs = tasksPerIP.size();
        int ipsWithoutOngoingRequest = 0;
        if(ipsWithJobs == null) {return 0.0;}

        for (final String IP : ipsWithJobs.keySet()) {
            int ready = 0;
            int downloading  = 0;
            int processing = 0;

            final List<String> taskIDs = tasksPerIP.get(IP);
            if(taskIDs == null) {continue;}

            for (final String taskID : taskIDs) {
                if(!allTasks.containsKey(taskID)) {continue;}
                final TaskState taskState = allTasks.get(taskID).getValue();

                if (taskState.equals(TaskState.READY)) {
                    ready += 1;
                }
                if (taskState.equals(TaskState.DOWNLOADING)) {
                    downloading += 1;
                }
                if (taskState.equals(TaskState.PROCESSING)) {
                    processing += 1;
                }
            }
            if ((ready + downloading + processing)== 0) {
                ipsWithoutOngoingRequest++;
            }
        }

        if (nrOfIPs == 0) {return 0.0;}

        return  100.0 - (100.0 * ipsWithoutOngoingRequest / nrOfIPs);
    }

    private void cleanIPs() {
        final List<String> IPs = new ArrayList<>();
        for (final Map.Entry<String, List<String>> task : tasksPerIP.entrySet()) {
            int ready = 0;
            int downloading = 0;
            int processing = 0;

            for (final String taskID : task.getValue()) {
                if(!allTasks.containsKey(taskID)) {continue;}
                final TaskState taskState = allTasks.get(taskID).getValue();

                if (taskState.equals(TaskState.READY)) {
                    ready += 1;
                }
                if (taskState.equals(TaskState.DOWNLOADING)) {
                    downloading += 1;
                }
                if (taskState.equals(TaskState.PROCESSING)) {
                    processing += 1;
                }
            }

            if (ready+downloading+processing == 0) {
                IPs.add(task.getKey());
            }
        }

        for(final String IP : IPs) {
            tasksPerIP.remove(IP);
        }
    }

    private void pauseTasks(final String jobID) {
        final List<String> IDs = new ArrayList<>();
        for(Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
            final RetrieveUrl current = entry.getValue().getKey();
            final TaskState state = entry.getValue().getValue();

            if(current.getJobId().equals(jobID) && (!(TaskState.DONE).equals(state))) {
                IDs.add(entry.getKey());
            }
        }

        for(final String ID : IDs) {
            final RetrieveUrl retrieveUrl = allTasks.get(ID).getKey();

            speedPerTask.remove(ID);
            allTasks.put(ID, new Pair<>(retrieveUrl, TaskState.PAUSE));
        }
    }

    private void resumeTasks(final String jobID) {
        final List<String> IDs = new ArrayList<>();
        for(Map.Entry<String, Pair<RetrieveUrl, TaskState>> entry : allTasks.entrySet()) {
            final RetrieveUrl current = entry.getValue().getKey();
            final TaskState state = entry.getValue().getValue();

            if(current.getJobId().equals(jobID) && (TaskState.PAUSE).equals(state)) {
                IDs.add(entry.getKey());
            }
        }

        for(final String id : IDs) {
            final RetrieveUrl retrieveUrl = allTasks.get(id).getKey();
            allTasks.put(id, new Pair<>(retrieveUrl, TaskState.READY));
        }
    }

    private void monitor() {
        LOG.info("Nr of tasks per ip: ");
        int ipsWithoutLoadedTasks = 0;
        int ipsDownloading = 0;
        int ipsProcessing = 0;

        for (final Map.Entry<String, List<String>> task : tasksPerIP.entrySet()) {
            int ready = 0;
            int downloading = 0;
            int processing = 0;

            for (final String taskID : task.getValue()) {
                final TaskState taskState = allTasks.get(taskID).getValue();

                if (taskState.equals(TaskState.READY)) {
                    ready += 1;
                }
                if (taskState.equals(TaskState.DOWNLOADING)) {
                    downloading += 1;
                }
                if (taskState.equals(TaskState.PROCESSING)) {
                    processing += 1;
                }
            }

            if (ready == 0) {
                ipsWithoutLoadedTasks += 1;
            }
            if (downloading != 0) {
                ipsDownloading += 1;
            }
            if (processing != 0) {
                ipsProcessing += 1;
            }

            LOG.info("{} : ready: {}, downloading: {}, processing: {}", task.getKey(), ready, downloading, processing);
        }
        LOG.info("IPS without loaded tasks: {}", ipsWithoutLoadedTasks);
        LOG.info("IPS downloading tasks: {}", ipsDownloading);
        LOG.info("IPS processing tasks: {}", ipsProcessing);

        LOG.info("Number of IPs: {}", tasksPerIP.size());
        LOG.info("Number of loaded tasks: {}", allTasks.size());
    }
}
