package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.ResponseHeader;
import org.joda.time.Duration;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ClusterMasterActor extends UntypedActor {

    private int SAVE = 0;
    private int UPDATE = 0;

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * The routers reference. We send all the messages to the router actor and then he decides the next step.
     */
    private final ActorRef routerActor;

    /**
     * A map with all jobs which maps each job with an other map. The inner map contains all the links with their state.
     */
    private final HashMap<String, HashMap<String,JobState>> allJobs = new HashMap<String, HashMap<String, JobState>>();

    /**
     * Map of urls and their task type(link check, conditional or unconditional download).
     */
    private final HashMap<String, DocumentReferenceTaskType> taskTypeOfDoc =
            new HashMap<String, DocumentReferenceTaskType>();

    /**
     * Map which stores a list of jobs for each ipAddress.
     */
    private final HashMap<String, List<Pair<String,String>>> linksPerIpAddress =
            new HashMap<String, List<Pair<String, String>>>();

    /**
     * Maps each url with an other map. The inner map maps jobs with speed.
     * (It is necessary because many jobs can have the same url in their task list)
     */
    private final HashMap<String, HashMap<String, Long>> speedPerUrl = new HashMap<String, HashMap<String, Long>>();

    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;

    /**
     * MachineResourceReference DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

    /**
     * SourceDocumentProcessingStatistics DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    /**
     * SourceDocumentReference DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    /**
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    /**
     * LinkCheckLimits DAO object which lets us to read and store data to and from the database.
     */
    private final LinkCheckLimitsDao linkCheckLimitsDao;

    /**
     * The default limit on download speed if it has not been provided.
     */
    private final Long defaultBandwidthLimitReadInBytesPerSec;

    /**
     * The default limit on concurrent number of downloads if it has not been provided.
     */
    private final Long defaultMaxConcurrentConnectionsLimit;

    public ClusterMasterActor(ClusterMasterConfig clusterMasterConfig, ProcessingJobDao processingJobDao,
                              MachineResourceReferenceDao machineResourceReferenceDao,
                              SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                              SourceDocumentReferenceDao sourceDocumentReferenceDao,
                              SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                              LinkCheckLimitsDao linkCheckLimitsDao, ActorRef routerActor,
                              Long defaultBandwidthLimitReadInBytesPerSec, Long defaultMaxConcurrentConnectionsLimit) {
        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
        this.routerActor = routerActor;
        this.defaultBandwidthLimitReadInBytesPerSec = defaultBandwidthLimitReadInBytesPerSec;
        this.defaultMaxConcurrentConnectionsLimit = defaultMaxConcurrentConnectionsLimit;
    }

    @Override
    public void preStart() throws Exception {
        log.debug("Started");

        getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(
                clusterMasterConfig.getReceiveTimeoutInterval().getStandardSeconds(), TimeUnit.SECONDS));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof  ActorStart) {
            start();
        }
        if(message instanceof RetrieveUrl) {
            routerActor.tell(message, getSelf());
        } else
        if(message instanceof StartedUrl) {
            log.info("From " + getSender() + " to master: " +
                    ((StartedUrl)message).getUrl() + " started.");
        } else
        if(message instanceof DoneDownload) {
            log.info("From " + getSender() + " to master: url: " + ((DoneDownload)message).getUrl() +
                    " from job: " + ((DoneDownload)message).getJobId() + " done.");

            markDone((DoneDownload) message);
        }
    }

    /**
     * Starts monitoring the database, checking for new jobs and then starts them if we have free resources.
     */
    private void start() {
        log.info("====================== Looking for new jobs =================================");
        updateLists();
        startTasks();

        final int period = (int)clusterMasterConfig.getJobsPollingInterval().getStandardSeconds();
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(period,
                TimeUnit.SECONDS), getSelf(), new ActorStart(), getContext().system().dispatcher(), getSelf());
    }

    /**
     * Updates the list of jobs.
     */
    private void updateLists() {
        final List<ProcessingJob> all = processingJobDao.getAllJobs();

        for(final ProcessingJob job : all) {
            if(!allJobs.containsKey(job.getId())) {// && !job.getState().equals(JobState.FINISHED)) {
                final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();
                final HashMap<String, JobState> urlsWithState = new HashMap<String, JobState>();

                for(ProcessingJobTaskDocumentReference task : tasks) {
                    final String sourceDocId = task.getSourceDocumentReference();

                    final SourceDocumentReference sourceDocumentReference =
                            sourceDocumentReferenceDao.read(sourceDocId);

                    String ipAddress = sourceDocumentReference.getIpAddress();
                    if(ipAddress == null) {
                        try {
                            final InetAddress address =
                                    InetAddress.getByName(new URL(sourceDocumentReference.getUrl()).getHost());
                            ipAddress = address.getHostAddress();
                            final SourceDocumentReference updatedSourceDocumentReference =
                                    sourceDocumentReference.withIPAddress(ipAddress);
                            sourceDocumentReferenceDao.update(updatedSourceDocumentReference);
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                            continue;
                        } catch (MalformedURLException e) {
                            e.printStackTrace();
                        }
                    }

                    List<Pair<String, String>> links = linksPerIpAddress.get(ipAddress);
                    if(links == null) {
                        links = new ArrayList<Pair<String, String>>();
                    }
                    links.add(new Pair<String, String>(job.getId(), sourceDocId));

                    linksPerIpAddress.put(ipAddress, links);

                    urlsWithState.put(sourceDocId, JobState.READY);
                    taskTypeOfDoc.put(task.getSourceDocumentReference(), task.getTaskType());
                }

                allJobs.put(job.getId(), urlsWithState);
            }
        }
    }

    /**
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private void startTasks() {
        int finished = 0;
        // Each server is a different case. We treat them different.
        for (final Map.Entry entry : linksPerIpAddress.entrySet()) {
            MachineResourceReference limit = machineResourceReferenceDao.read((String) entry.getKey());
            if(limit == null) {
                limit = new MachineResourceReference((String) entry.getKey(), null,
                        null, defaultBandwidthLimitReadInBytesPerSec, defaultMaxConcurrentConnectionsLimit);
            }
            List<ReferenceOwner> referenceOwners = limit.getReferenceOwnerList();
            if(referenceOwners == null) {
                referenceOwners = new ArrayList<ReferenceOwner>();
            }

            int runningDownloads = 0;
            Long consumedBandwidth = 0l;

            final List<Pair<String, String>> linkPerJob = (List<Pair<String, String>>) entry.getValue();

            // Calculates the occupied bandwidth.
            for (final Pair<String, String> job : linkPerJob) {
                JobState jobsLinkState = allJobs.get(job.getKey()).get(job.getValue());
                if(jobsLinkState.equals(JobState.RUNNING)) {
                    runningDownloads++;
                    consumedBandwidth += speedPerUrl.get(job.getValue()).get(job.getKey());
                }
                if(jobsLinkState.equals(JobState.FINISHED) || jobsLinkState.equals(JobState.ERROR)) {
                    finished++;
                }
                log.info(job.getValue() + ":" + jobsLinkState);
            }

            // Maximum speed per link
            final Long speedLimitPerLink =
                    limit.getBandwidthLimitReadInBytesPerSec()/limit.getMaxConcurrentConnectionsLimit();

            log.info(limit.getMaxConcurrentConnectionsLimit() + ":" + runningDownloads);
            log.info(limit.getBandwidthLimitReadInBytesPerSec() + ":" + consumedBandwidth);

            // Starts tasks until we have resources. (mainly bandwidth)
            while(runningDownloads < limit.getBandwidthLimitReadInBytesPerSec() &&
                    ((limit.getBandwidthLimitReadInBytesPerSec() - consumedBandwidth) >= speedLimitPerLink)) {

                final ReferenceOwner referenceOwner = startOneDownload(linkPerJob, speedLimitPerLink);
                if(referenceOwner == null) {
                    break;
                }

                if(!referenceOwners.contains(referenceOwner)) {
                    referenceOwners.add(referenceOwner);
                }

                runningDownloads++;
                consumedBandwidth += speedLimitPerLink;
            }

            limit = limit.withReferenceOwnerList(referenceOwners);
            machineResourceReferenceDao.update(limit);

            log.info(limit.getMaxConcurrentConnectionsLimit() + ":" + runningDownloads);
            log.info(limit.getBandwidthLimitReadInBytesPerSec() + ":" + consumedBandwidth);
        }
        System.out.println("Finished: " + finished);
    }

    /**
     * Starts one download
     * @param linkPerJob a map of links with jobs
     * @param speed allowed maximum speed
     * @return - at success a ReferenceOwner object at failure null
     */
    private ReferenceOwner startOneDownload(List<Pair<String, String>> linkPerJob, Long speed) {
        for (final Pair<String, String> job : linkPerJob) {
            final String jobId = job.getKey();

            if(allJobs.get(jobId).get(job.getValue()).equals(JobState.READY)) {
                final String sourceDocId = job.getValue();
                allJobs.get(jobId).put(sourceDocId, JobState.RUNNING);

                final ProcessingJob processingJob = processingJobDao.read(jobId);
                final ProcessingJob newProcessingJob = processingJob.withState(JobState.RUNNING);

                processingJobDao.update(newProcessingJob);

                final SourceDocumentReference newDoc = sourceDocumentReferenceDao.read(sourceDocId);

                HashMap<String, Long> speeds = speedPerUrl.get(sourceDocId);
                if(speeds == null) {
                    speeds = new HashMap<String, Long>();
                }

                final DocumentReferenceTaskType documentReferenceTaskType = taskTypeOfDoc.get(sourceDocId);

                List<ResponseHeader> headers = null;
                if(documentReferenceTaskType.equals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD)) {
                    final String statisticsID = newDoc.getLastStatsId();
                    final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                            sourceDocumentProcessingStatisticsDao.read(statisticsID);
                    try {
                        headers = sourceDocumentProcessingStatistics.getHttpResponseHeaders();
                    } catch (Exception e) {
                        headers = new ArrayList<ResponseHeader>();
                        e.printStackTrace();
                    }
                }

                final HttpRetrieveConfig httpRetrieveConfig =
                        new HttpRetrieveConfig(Duration.millis(100), speed, speed, Duration.ZERO, 0l, true,
                                documentReferenceTaskType);
                getSelf().tell(new RetrieveUrl(newDoc.getUrl(), httpRetrieveConfig, jobId, newDoc.getId(), headers,
                        documentReferenceTaskType), getSelf());

                speeds.put(jobId, speed);

                speedPerUrl.put(sourceDocId, speeds);

                log.info("Starting: " + newDoc.getUrl());

                return newDoc.getReferenceOwner();
            }
        }

        return null;
    }

    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(DoneDownload msg) {
        final ProcessingJob processingJob = processingJobDao.read(msg.getJobId());
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(msg.getUrl().toString());

        String temp1;
        String temp2;
        try {
            temp1 = msg.getJobId();
        } catch (Exception e) {
            log.info("\nERROR: missing job id\n");
            return;
        }
        try {
            temp2 = finishedDocument.getId();
        } catch (Exception e) {
            log.info("\nERROR: missing doc id: " + msg.getUrl().toString() + "\n");
            return;
        }

        final String jobId = temp1;
        final String docId = temp2;

        try {
            allJobs.get(jobId).put(docId, JobState.FINISHED);
        } catch(Exception e) {
            e.printStackTrace();
            return;
        }

        // CreateOrModify statistics
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(docId, jobId);

        if(sourceDocumentProcessingStatistics == null) {
            sourceDocumentProcessingStatistics =
                    new SourceDocumentProcessingStatistics(new Date(), new Date(), msg.getProcessingState(),
                            finishedDocument.getReferenceOwner(), docId, processingJob.getId(),
                            msg.getHttpResponseCode(), msg.getHttpResponseContentType(),
                            msg.getHttpResponseContentSizeInBytes(),
                            msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                            msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                            msg.getSourceIp(), msg.getHttpResponseHeaders());

            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

            System.out.println("Save: " + SAVE++);
        } else {
            SourceDocumentProcessingStatistics updatedSourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatistics.withUpdate(new Date(), msg.getProcessingState(), msg.getJobId(),
                            msg.getHttpResponseCode(), msg.getHttpResponseContentSizeInBytes(),
                            msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                            msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                            msg.getHttpResponseHeaders());

            sourceDocumentProcessingStatisticsDao.update(updatedSourceDocumentProcessingStatistics);

            System.out.println("Update: " + UPDATE++);
        }

        final DocumentReferenceTaskType documentReferenceTaskType = taskTypeOfDoc.get(docId);
        if(!documentReferenceTaskType.equals(DocumentReferenceTaskType.CHECK_LINK)) {
            final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfoFromDB =
                    sourceDocumentReferenceMetaInfoDao.findBySourceDocumentReferenceId(docId);

            if(sourceDocumentReferenceMetaInfoFromDB != null) {
                final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                        new SourceDocumentReferenceMetaInfo(sourceDocumentReferenceMetaInfoFromDB.getId(),
                                docId, msg.getImageMetaInfo(),
                                msg.getAudioMetaInfo(), msg.getVideoMetaInfo());
                sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo);
            } else {
                final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                        new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                                msg.getAudioMetaInfo(), msg.getVideoMetaInfo());
                sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo);
            }
        }

        final HashMap<String, JobState> links = allJobs.get(jobId);

        SourceDocumentReference updatedDocument =
                finishedDocument.withLastStatsId(sourceDocumentProcessingStatistics.getId());
        updatedDocument = updatedDocument.withRedirectionPath(msg.getRedirectionPath());

        sourceDocumentReferenceDao.update(updatedDocument);

        boolean allDone = true;
        for (final Map.Entry link : links.entrySet()) {
            if(link.getValue() != JobState.FINISHED && link.getValue() != JobState.ERROR) {
                allDone = false;
                break;
            }
        }
        if(allDone) {
            final ProcessingJob newProcessingJob = processingJob.withState(JobState.FINISHED);

            processingJobDao.update(newProcessingJob);
        }
    }

}
