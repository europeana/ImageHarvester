package eu.europeana.harvester.cluster.master.accountants;

import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.BagOfTasks;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.AddTask;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.JobPriority;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by nutz on 11.08.2015.
 */
public class AccountantActorHelper {


    private class MapWrapper {

        private final Map<String, RetrieveUrl> waitingTasks = new HashMap<>();
        private final Map<String, List<String>> tasksPerIP = new HashMap<>();

        public int getSize() { return waitingTasks.size(); }

        public void addTask (RetrieveUrl retrieveUrl) {
            waitingTasks.put(retrieveUrl.getId(),retrieveUrl);
            final String IP = retrieveUrl.getIpAddress();
            List<String> tasks = tasksPerIP.get(IP);
            if (tasks ==null)
                tasks = new ArrayList<>();
            tasks.add(retrieveUrl.getId());
            tasksPerIP.put(IP, tasks);
        }

        public ArrayList<String> getOverloadedIPs(int threshold ) {
            ArrayList<String> IPs = new ArrayList<>();
            for (final Map.Entry<String, List<String>> task : tasksPerIP.entrySet())
                if (task.getValue().size() > threshold)
                    IPs.add(task.getKey());
            return IPs;

        }

        public List<RetrieveUrl> getListOfTasksWithRoundRobinStrategy(int maxToSend ) {

            ArrayList<RetrieveUrl> tasksToSend = new ArrayList<>();

            boolean foundTasks = true;

            while (foundTasks && tasksToSend.size()<maxToSend) {

                List<String> ips = new ArrayList<>(tasksPerIP.keySet());
                foundTasks = false;
                for (String ip : ips) {

                    List<String> tasks = tasksPerIP.get(ip);

                    String task = tasks.remove(0);
                    if (tasks.size() == 0)
                        tasksPerIP.remove(ip);
                    else
                        tasksPerIP.put(ip, tasks);


                    RetrieveUrl retrieveUrl = waitingTasks.remove(task);
            
                    if(retrieveUrl!=null)
                        tasksToSend.add(retrieveUrl);

                    foundTasks = true;

                    if (tasksToSend.size() == maxToSend)
                        break;
                }
            }

            return tasksToSend;

        }

    }
    /**
     * Maps all tasks ids
     */
    private final Map<String, RetrieveUrl> allStartedTasks = new HashMap<>();
    private final Map<String, DateTime> allStartedTaskStartTime = new HashMap<>();
    private final MapWrapper normalLane = new MapWrapper();
    private final MapWrapper fastLane = new MapWrapper();

    private final DefaultLimits defaultLimits;


    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());


    public AccountantActorHelper(DefaultLimits defaultLimits) {
        this.defaultLimits = defaultLimits;
    }

    public Integer getNumberOfTasks() {
        Integer nr = normalLane.getSize() +fastLane.getSize()+allStartedTasks.size();
        return nr;
    }


    public void addTask ( AddTask message ) {
        //read the message payload into local variables
        final String taskID = message.getTaskID();
        final Pair<RetrieveUrl, TaskState> taskWithState = message.getTaskWithState();
        final JobPriority prio = JobPriority.fromPriority(message.getJobPriority());

        if ( prio == JobPriority.FASTLANE)
            fastLane.addTask(taskWithState.getKey());
        else
            normalLane.addTask(taskWithState.getKey());

        return;
    }

    public void doneTask ( DoneProcessing message ) {

        final String taskID = message.getTaskID();
        final String jobID = message.getJobId();
        final String IP = message.getSourceIp();
        allStartedTasks.remove(taskID);
        allStartedTaskStartTime.remove(taskID);

        return;
    }

    public void monitor () {
        return;
    }

    public void clean() {
        DateTime minDateTime = DateTime.now().minus(defaultLimits.getMaxJobProcessingDuration());
        ArrayList<String> tasksToRestart = new ArrayList<>();

        for ( String taskID : allStartedTaskStartTime.keySet())
            if (allStartedTaskStartTime.get(taskID).isBefore(minDateTime))
                tasksToRestart.add(taskID);

        for ( String taskID : tasksToRestart ) {
            allStartedTaskStartTime.remove(taskID);
            RetrieveUrl retrieveUrl = allStartedTasks.remove(taskID);
            fastLane.addTask(retrieveUrl);
        }

        return;
    }


    public ArrayList<String> getIPsWithTooManyTasks(int threshold) {

        ArrayList<String> IPs = new ArrayList<>();
        IPs.addAll(normalLane.getOverloadedIPs(threshold));
        IPs.addAll(fastLane.getOverloadedIPs(threshold));
        return IPs;
    }


    public BagOfTasks getBagOfTasks() {

        List<RetrieveUrl> tasksToSend = startTasks();
        //LOG.info ("taskstosend size"+tasksToSend.size());
        final BagOfTasks bagOfTasks = new BagOfTasks(tasksToSend);
        return bagOfTasks;

    }


    /**
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private List<RetrieveUrl> startTasks() {

        List<RetrieveUrl> tasksToSend = new ArrayList<>();

        final int maxToSend = defaultLimits.getTaskBatchSize();

        // first we go through the fastlane tasks
        List<RetrieveUrl> fastLaneTasks = fastLane.getListOfTasksWithRoundRobinStrategy(maxToSend);

        List<RetrieveUrl> normalLaneTasks = ( tasksToSend.size() < maxToSend ) ?
                normalLane.getListOfTasksWithRoundRobinStrategy(maxToSend-fastLaneTasks.size()) : new ArrayList<RetrieveUrl>();
        tasksToSend.addAll(fastLaneTasks);
        tasksToSend.addAll(normalLaneTasks);
        for ( RetrieveUrl task : tasksToSend ) {
            allStartedTasks.put(task.getId(), task);
            allStartedTaskStartTime.put(task.getId(), DateTime.now());
        }

        return tasksToSend;
    }


}
