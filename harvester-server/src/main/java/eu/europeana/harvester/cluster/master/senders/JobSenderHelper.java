package eu.europeana.harvester.cluster.master.senders;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.BagOfTasks;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.CheckIPsWithJobs;
import eu.europeana.harvester.cluster.domain.messages.inner.GetRetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.GetTasksFromIP;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobSenderHelper  {


    /**
     * Handles the request for new tasks. Sends a predefined number of tasks.
     * @param sender sender actor.
     */
    public static void handleRequest(ActorRef sender, ActorRef accountantActor, ActorRef receiverActor, ActorRef jobLoaderActor,
                               DefaultLimits defaultLimits, HashMap<String, Boolean> ipsWithJobs, Logger LOG, IPExceptions ipExceptions) {

        final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new CheckIPsWithJobs(ipsWithJobs), timeout);
        Double percentage;
        try {
            percentage = (Double) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                    "Exception while responding to get tasks request",e);

            percentage = 0.0;
        }

        //LOG.info ("creating bag of tasks");

        List<RetrieveUrl> tasksToSend = startTasks(defaultLimits, ipsWithJobs, accountantActor, ipExceptions, LOG);

        //LOG.info ("taskstosend size"+tasksToSend.size());


        final BagOfTasks bagOfTasks = new BagOfTasks(tasksToSend);
        sender.tell(bagOfTasks, receiverActor);

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                "Percentage of IPs which has loaded requests: {}% load when it's below: {}",
                percentage, defaultLimits.getMinTasksPerIPPercentage());

        if(percentage < defaultLimits.getMinTasksPerIPPercentage()) {
            //accountantActor.tell(new CleanIPs(), getSelf());
            jobLoaderActor.tell(new LoadJobs(), ActorRef.noSender());
        }
    }

    /**
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private static List<RetrieveUrl> startTasks(DefaultLimits defaultLimits, HashMap<String, Boolean> ipsWithJobs, ActorRef accountantActor,
                            IPExceptions ipExceptions, Logger LOG) {
        List<RetrieveUrl> tasksToSend = new ArrayList<>();
        final int maxToSend = defaultLimits.getTaskBatchSize();
        try {
            // Each server is a different case. We treat them different.

            List<String> IPs = new ArrayList(ipsWithJobs.keySet());
            Collections.shuffle(IPs);
            //LOG.info ("IPs with jobs size "+IPs.size());

            for (final String IP : IPs) {


                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                final Future<Object> future = Patterns.ask(accountantActor, new GetTasksFromIP(IP), timeout);

                List<String> tasksFromIP;
                try {
                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                            "Error at startTasks->getTasksFromIP.", e);

                    continue;
                }


                if (tasksFromIP == null || tasksFromIP.size()==0) {
                    //LOG.info ("tasksfromip is null or 0 elems for IP "+IP);
                    continue;
                }




                // Starts tasks until we have resources or there are tasks to start. (mainly bandwidth)

                for (final String taskID : tasksFromIP) {
                    RetrieveUrl r = startOneDownload(taskID, IP, ipExceptions, defaultLimits, accountantActor, LOG);

                    if (r != null && (!r.getUrl().equals(""))) {
                        //LOG.info("started one download for IP: " + IP + " and URL " + r.getUrl()+" for task ID "+taskID);
                        tasksToSend.add(r);
                        if (tasksToSend.size() >= maxToSend)
                            break;
                    }
                }


                if ( tasksToSend.size() >= maxToSend ) break;


            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return tasksToSend;
    }

    /**
     * Starts one download
     * @param taskID a list of requests
     * @return - at success true at failure false
     */
    private static RetrieveUrl startOneDownload(String taskID, final String IP, IPExceptions ipExceptions,
                                     DefaultLimits defaultLimits, ActorRef accountantActor, Logger LOG) {
        RetrieveUrl retrieveUrl = null;

        if (! ipExceptions.getIgnoredIPs().contains(IP)) {

            final boolean isException = ipExceptions.getIps().contains(IP);
            final Long defaultLimit = defaultLimits.getDefaultMaxConcurrentConnectionsLimit();
            final int exceptionLimit = ipExceptions.getMaxConcurrentConnectionsLimit();


            final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
            Future<Object> future;


            GetRetrieveUrl message = new GetRetrieveUrl(taskID, IP, isException, defaultLimit, exceptionLimit);


            future = Patterns.ask(accountantActor, message , timeout);
            try {
                retrieveUrl = (RetrieveUrl) Await.result(future, timeout.duration());
            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                        "Error at startOneDownload -> getTask", e);

            }


        }

        //LOG.info ("started one download returning "+retrieveUrl.getUrl()+ " for taski ID "+ taskID);

        return retrieveUrl ;
    }


    /**
     * Checks for tasks which was not acknowledged by any slave so they will be reloaded.
     */
//    private void checkForMissedTasks() {
//        final DateTime currentTime = new DateTime();
//
//        List<String> tasksToRemove = new ArrayList<>();
//
//        try {
//            final Map<String, DateTime> tasks = new HashMap<>(tasksPerTime);
//
//            for (final String task : tasks.keySet()) {
//                final DateTime timeout =
//                        tasks.get(task).plusMillis(clusterMasterConfig.getResponseTimeoutFromSlaveInMillis());
//                if (timeout.isBefore(currentTime)) {
//                    tasksToRemove.add(task);
//
//                    accountantActor.tell(new ModifyState(task, TaskState.READY), getSelf());
//                }
//            }
//
//            for(final String task : tasksToRemove) {
//                tasksPerTime.remove(task);
//            }
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//        }
//
//        final int period = clusterMasterConfig.getResponseTimeoutFromSlaveInMillis()/1000;
//        getContext().system().scheduler().scheduleOnce(Duration.create(period,
//                TimeUnit.SECONDS), getSelf(), new CheckForTaskTimeout(), getContext().system().dispatcher(), getSelf());
//    }


    /**
     * Recovers the tasks if an actor system crashes.
     * @param address the address of the actor system.
     */
//    private void recoverTasks(final Address address) {
//        final HashSet<String> tasks = tasksPerAddress.get(address);
//        if(tasks != null) {
//            for (final String taskID : tasks) {
//                accountantActor.tell(new ModifyState(taskID, TaskState.READY), getSelf());
//                accountantActor.tell(new RemoveDownloadSpeed(taskID), getSelf());
//            }
//        }
//        tasksPerAddress.remove(address);
//    }




}
