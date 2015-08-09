package eu.europeana.harvester.cluster.master.senders;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.BagOfTasks;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.GetListOfIPs;
import eu.europeana.harvester.cluster.domain.messages.inner.GetRetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.GetTasksFromIP;
import eu.europeana.harvester.domain.JobPriority;
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


        // we send a loadjobs message to the loader reagrdless and let him deal with it.

        jobLoaderActor.tell(new LoadJobs(), ActorRef.noSender());


        List<RetrieveUrl> tasksToSend = startTasks(defaultLimits, ipsWithJobs, accountantActor, ipExceptions, LOG);

        //LOG.info ("taskstosend size"+tasksToSend.size());


        final BagOfTasks bagOfTasks = new BagOfTasks(tasksToSend);
        sender.tell(bagOfTasks, receiverActor);

    }

    /**
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private static List<RetrieveUrl> startTasks(DefaultLimits defaultLimits, HashMap<String, Boolean> ipsWithJobs, ActorRef accountantActor,
                            IPExceptions ipExceptions, Logger LOG) {
        List<RetrieveUrl> tasksToSend = new ArrayList<>();

        final int maxToSend = defaultLimits.getTaskBatchSize();

        try {

            // first send from the fastlane
            final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
            final Future<Object> future = Patterns.ask(accountantActor, new GetListOfIPs(), timeout);

            List<String> fastLaneIPs=null;
            try {
                fastLaneIPs = (List<String>) Await.result(future, timeout.duration());
            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                        "Error at startTasks->getListOfIPs.", e);

            }

            for (final String IP : fastLaneIPs) {

                final Future<Object> future2 = Patterns.ask(accountantActor, new GetTasksFromIP(IP, JobPriority.FASTLANE ), timeout);

                List<String> tasksFromIP;
                try {
                    tasksFromIP = (List<String>) Await.result(future2, timeout.duration());

                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                            "Error at startTasks->getTasksFromIP.", e);

                    continue;
                }


                if (tasksFromIP == null || tasksFromIP.size()==0) {
                    //LOG.info ("tasksfromip is null or 0 elems for IP "+IP);
                    continue;
                }


                for (final String taskID : tasksFromIP) {
                    RetrieveUrl r = startOneDownload(JobPriority.FASTLANE, taskID, IP, ipExceptions, defaultLimits, accountantActor, LOG);

                    if (r != null && (!r.getUrl().equals(""))) {
                        //LOG.info("started one download for IP: " + IP + " and URL " + r.getUrl()+" for task ID "+taskID);
                        tasksToSend.add(r);
                        if (tasksToSend.size() >= maxToSend)
                            break;
                    }
                }


                if ( tasksToSend.size() >= maxToSend ) break;


            }
            // if we didn't reached the maximum size from the fastlane, we continue with normal loading
            if ( tasksToSend.size() < maxToSend ) {
                // Each server is a different case. We treat them different.

                List<String> IPs = new ArrayList(ipsWithJobs.keySet());
                Collections.shuffle(IPs);
                //LOG.info ("IPs with jobs size "+IPs.size());
                for (final String IP : IPs) {


                    final Future<Object> future3 = Patterns.ask(accountantActor, new GetTasksFromIP(IP, JobPriority.NORMAL), timeout);

                    List<String> tasksFromIP;
                    try {
                        tasksFromIP = (List<String>) Await.result(future3, timeout.duration());

                    } catch (Exception e) {
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                                "Error at startTasks->getTasksFromIP.", e);

                        continue;
                    }


                    if (tasksFromIP == null || tasksFromIP.size() == 0) {
                        //LOG.info ("tasksfromip is null or 0 elems for IP "+IP);
                        continue;
                    }


                    for (final String taskID : tasksFromIP) {
                        RetrieveUrl r = startOneDownload(JobPriority.NORMAL, taskID, IP, ipExceptions, defaultLimits, accountantActor, LOG);

                        if (r != null && (!r.getUrl().equals(""))) {
                            //LOG.info("started one download for IP: " + IP + " and URL " + r.getUrl()+" for task ID "+taskID);
                            tasksToSend.add(r);
                            if (tasksToSend.size() >= maxToSend)
                                break;
                        }
                    }


                    if (tasksToSend.size() >= maxToSend) break;
                }

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
    private static RetrieveUrl startOneDownload(JobPriority jobPriority, String taskID, final String IP, IPExceptions ipExceptions,
                                     DefaultLimits defaultLimits, ActorRef accountantActor, Logger LOG) {
        RetrieveUrl retrieveUrl = null;

        if (! ipExceptions.getIgnoredIPs().contains(IP)) {

            final boolean isException = ipExceptions.getIps().contains(IP);
            final Long defaultLimit = defaultLimits.getDefaultMaxConcurrentConnectionsLimit();
            final int exceptionLimit = ipExceptions.getMaxConcurrentConnectionsLimit();


            final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
            Future<Object> future;


            GetRetrieveUrl message = new GetRetrieveUrl(jobPriority, taskID, IP, isException, defaultLimit, exceptionLimit);


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






}
