package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import org.joda.time.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ClusterMasterActor extends UntypedActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final ClusterMasterConfig clusterMasterConfig;

    private final ActorRef routerActor;

    private final HashMap<String, HashMap<String,JobState>> allJobs = new HashMap<String, HashMap<String, JobState>>();

    private final HashMap<Long, List<String>> jobsPerCollection = new HashMap<Long, List<String>>();

    private final HashMap<String, HashMap<String, Long>> speedPerUrl = new HashMap<String, HashMap<String, Long>>();

    private final ProcessingJobDao processingJobDao;

    private final ProcessingLimitsDao processingLimitsDao;

    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    private final LinkCheckLimitsDao linkCheckLimitsDao;

    public ClusterMasterActor(ClusterMasterConfig clusterMasterConfig, ProcessingJobDao processingJobDao,
                              ProcessingLimitsDao processingLimitsDao,
                              SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                              SourceDocumentReferenceDao sourceDocumentReferenceDao,
                              LinkCheckLimitsDao linkCheckLimitsDao, ActorRef routerActor) {
        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.processingLimitsDao = processingLimitsDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
        this.routerActor = routerActor;
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
        if(message instanceof SendResponse) {
            log.info("From " + getSender() + " to master: " +
                    ((SendResponse)message).getHttpRetrieveResponse().getUrl() + " " +
                    ((SendResponse)message).getHttpRetrieveResponse().getContentSizeInBytes() + " done.");
        } else
        if(message instanceof DoneDownload) {
            log.info("From " + getSender() + " to master: url: " + ((DoneDownload)message).getUrl() +
                    " from job: " + ((DoneDownload)message).getJobId() + " done.");

            markDone((DoneDownload)message);
        }
    }

    private void start() {
        final int delay = 0;
        final int period = (int)clusterMasterConfig.getJobsPollingInterval().getStandardSeconds() * 1000;

        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                log.info("Looking for new jobs");
                updateLists();
                startTasks();
            }
        }, delay, period);
    }

    private void updateLists() {
        final List<ProcessingJob> all = processingJobDao.getAllJobs();

        for(final ProcessingJob job : all) {
            if(!allJobs.containsKey(job.getId())) {
                final List<String> urls = job.getSourceDocumentReferences();
                final HashMap<String, JobState> urlsWithState = new HashMap<String, JobState>();

                for(final String url : urls) {
                    urlsWithState.put(url, JobState.READY);
                }
                allJobs.put(job.getId(), urlsWithState);

                List<String> jobs = jobsPerCollection.get(job.getCollectionId());
                if(jobs == null) {
                    jobs = new ArrayList<String>();
                }
                jobs.add(job.getId());

                jobsPerCollection.put(job.getCollectionId(), jobs);
            }
        }
    }

    private void startTasks() {
        for (final Map.Entry entry : jobsPerCollection.entrySet()) {
            log.info("Check job: " + entry.getKey());

            final ProcessingLimits limit = processingLimitsDao.findByCollectionId((Long) entry.getKey());

            int runningDownloads = 0;
            Long consumedBandwidth = 0l;

            final List<String> jobs = (List<String>)entry.getValue();

            for(final String job : jobs) {
                final HashMap<String, JobState> links = allJobs.get(job);
                for (final Map.Entry link : links.entrySet()) {
                    if(link.getValue().equals(JobState.RUNNING)) {
                        runningDownloads++;
                        consumedBandwidth += speedPerUrl.get(link.getKey()).get(job);
                    }
                    log.info(link.getKey() + ":" + link.getValue());
                }
            }

            final Long speedLimitPerLink =
                    limit.getBandwidthLimitReadInBytesPerSec()/limit.getMaxConcurrentConnectionsLimit();

            log.info(limit.getMaxConcurrentConnectionsLimit() + ":" + runningDownloads);
            log.info(limit.getBandwidthLimitReadInBytesPerSec() + ":" + consumedBandwidth);

            while(runningDownloads < limit.getBandwidthLimitReadInBytesPerSec() &&
                    ((limit.getBandwidthLimitReadInBytesPerSec() - consumedBandwidth) >= speedLimitPerLink)) {
                if(!startOneDownload(jobs, speedLimitPerLink)) {
                    break;
                }
                runningDownloads++;
                consumedBandwidth += speedLimitPerLink;
            }

            log.info(limit.getMaxConcurrentConnectionsLimit() + ":" + runningDownloads);
            log.info(limit.getBandwidthLimitReadInBytesPerSec() + ":" + consumedBandwidth);
        }
    }

    private boolean startOneDownload(List<String> jobs, Long speed) {
        for(final String job : jobs) {
            final HashMap<String, JobState> links = allJobs.get(job);
            for (final Map.Entry link : links.entrySet()) {
                if(link.getValue().equals(JobState.READY)) {
                    links.put((String) link.getKey(), JobState.RUNNING);

                    HashMap<String, Long> speeds = speedPerUrl.get(link.getKey());
                    if(speeds == null) {
                        speeds = new HashMap<String, Long>();
                    }
                    speeds.put(job, speed);
                    speedPerUrl.put((String) link.getKey(), speeds);
                    SourceDocumentReference newDoc = sourceDocumentReferenceDao.read((String)link.getKey());

                    final HttpRetrieveConfig httpRetrieveConfig =
                            new HttpRetrieveConfig(Duration.millis(100), speed, speed, Duration.ZERO, 0l, true);
                    getSelf().tell(new RetrieveUrl(newDoc.getUrl(), httpRetrieveConfig, job), getSelf());

                    log.info("Starting: " + newDoc.getUrl());
                    return true;
                }
            }
        }

        return false;
    }

    private void markDone(DoneDownload msg) {
        final ProcessingJob processingJob = processingJobDao.read(msg.getJobId());
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(msg.getUrl().toString());

        allJobs.get(msg.getJobId()).put(finishedDocument.getId(), JobState.FINISHED);

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), ProcessingState.SUCCESS,
                        finishedDocument.getProviderId(), finishedDocument.getCollectionId(),
                        finishedDocument.getRecordId(), finishedDocument.getId(), processingJob.getId(),
                        msg.getHttpResponseCode(), msg.getHttpResponseContentType(),
                        msg.getHttpResponseContentSizeInBytes(), msg.getRetrievalDurationInSecs(),
                        msg.getCheckingDurationInSecs(), msg.getSourceIp(), msg.getHttpResponseHeaders());

        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);
    }
}
