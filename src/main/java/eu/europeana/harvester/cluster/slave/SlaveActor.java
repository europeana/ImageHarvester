package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.UntypedActorWithStash;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpClient;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseDiskStorage;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;

import gr.ntua.image.mediachecker.*;
import org.im4java.core.InfoException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SlaveActor extends UntypedActorWithStash {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    private final HttpRetrieveResponseFactory httpRetrieveResponseFactory;

    /**
     * Response type: memory or disk storage.
     */
    private final ResponseType responseType;

    /**
     * If the response type is disk storage then the absolute path on disk where
     * the content of the download will be saved.
     */
    private final String pathToSave;

    public SlaveActor(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer,
                      HttpRetrieveResponseFactory httpRetrieveResponseFactory, ResponseType responseType,
                      String pathToSave) {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.responseType = responseType;
        this.pathToSave = pathToSave;
        log.info("Create");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        System.out.println("ON receive");
        final ActorRef sender = getSender();

        if(message instanceof RetrieveUrl) {
            final RetrieveUrl task = (RetrieveUrl) message;
            final StartedUrl startedUrl = new StartedUrl(task.getUrl());

            sender.tell(startedUrl, getSelf());

            HttpRetrieveResponse httpRetrieveResponse = downloadTask(task);

            if(httpRetrieveResponse.getHttpResponseCode() == -1) {
                final DoneDownload doneDownload = new DoneDownload(new URL(task.getUrl()),
                        task.getJobId(), httpRetrieveResponse.getHttpResponseCode(),
                        httpRetrieveResponse.getHttpResponseContentType(), httpRetrieveResponse.getContentSizeInBytes(),
                        httpRetrieveResponse.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        httpRetrieveResponse.getRetrievalDurationInMilliSecs(),
                        httpRetrieveResponse.getCheckingDurationInMilliSecs(),
                        httpRetrieveResponse.getSourceIp(), httpRetrieveResponse.getHttpResponseHeaders(),
                        httpRetrieveResponse.getRedirectionPath(), null, null, null, ProcessingState.ERROR);
                sender.tell(doneDownload, getSelf());
                return;
            }

            if(task.getDocumentReferenceTaskType().equals(DocumentReferenceTaskType.CHECK_LINK)) {
                final DoneDownload doneDownload = new DoneDownload(new URL(task.getUrl()),
                        task.getJobId(), httpRetrieveResponse.getHttpResponseCode(),
                        httpRetrieveResponse.getHttpResponseContentType(), httpRetrieveResponse.getContentSizeInBytes(),
                        httpRetrieveResponse.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        httpRetrieveResponse.getRetrievalDurationInMilliSecs(),
                        httpRetrieveResponse.getCheckingDurationInMilliSecs(),
                        httpRetrieveResponse.getSourceIp(), httpRetrieveResponse.getHttpResponseHeaders(),
                        httpRetrieveResponse.getRedirectionPath(), null, null, null, ProcessingState.SUCCESS);
                sender.tell(doneDownload, getSelf());
                return;
            }

            final ContentType contentType = classifyUrl(task);

            ImageMetaInfo imageMetaInfo = null;
            AudioMetaInfo audioMetaInfo = null;
            VideoMetaInfo videoMetaInfo = null;

            if(responseType.equals(ResponseType.DISK_STORAGE)) {
                switch (contentType) {
                    case TEXT:
                        break;
                    case IMAGE:
                        imageMetaInfo = extractImageMetadata(httpRetrieveResponse);
                        break;
                    case VIDEO:
                        videoMetaInfo = extractVideoMetaData(httpRetrieveResponse);
                        break;
                    case AUDIO:
                        audioMetaInfo = extractAudioMetadata(httpRetrieveResponse);
                        break;
                    case UNKNOWN:
                        break;
                }
            }

            final DoneDownload doneDownload = new DoneDownload(new URL(task.getUrl()),
                    task.getJobId(), httpRetrieveResponse.getHttpResponseCode(),
                    httpRetrieveResponse.getHttpResponseContentType(), httpRetrieveResponse.getContentSizeInBytes(),
                    httpRetrieveResponse.getSocketConnectToDownloadStartDurationInMilliSecs(),
                    httpRetrieveResponse.getRetrievalDurationInMilliSecs(),
                    httpRetrieveResponse.getCheckingDurationInMilliSecs(),
                    httpRetrieveResponse.getSourceIp(), httpRetrieveResponse.getHttpResponseHeaders(),
                    httpRetrieveResponse.getRedirectionPath(), imageMetaInfo, audioMetaInfo, videoMetaInfo,
                    ProcessingState.SUCCESS);
            sender.tell(doneDownload, getSelf());
        } else
        if(message instanceof StartPing) {
            StartPing startPing = (StartPing)message;
            final DonePing donePing =
                    startPinging(startPing.getIp(), startPing.getNrOfPings(), startPing.getPingTimeout());

            sender.tell(donePing, getSelf());
        }
    }

    /**
     * Starts one download
     * @param task contains all the information for this task
     * @return - finished job response with all the collected information
     */
    private HttpRetrieveResponse downloadTask(RetrieveUrl task) {
        System.out.println("Download :D");
        HttpRetrieveResponse httpRetrieveResponse = null;
        final String path = pathToSave + "/" + task.getReferenceId();

        try {
            httpRetrieveResponse =
                    httpRetrieveResponseFactory.create(responseType, path);
            String url = task.getUrl();

            int depth = -1;
            boolean redirect;

            // Downloads until we get the final link,
            // if it's redirect then download the link given in the header Location field
            do {
                depth++; redirect = false;

                try {
                    httpRetrieveResponse.setUrl(new URL(url));
                } catch (java.net.MalformedURLException e) {
                    httpRetrieveResponse.setHttpResponseCode(-1);
                    return httpRetrieveResponse;
                }
                final HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer,
                        task.getHttpRetrieveConfig(), task.getHeaders(), httpRetrieveResponse,
                        HttpGET.build(httpRetrieveResponse.getUrl()));
                if(httpRetrieveResponse.getHttpResponseCode() == -1) {
                    return httpRetrieveResponse;
                }

                ExecutorService httpPool = Executors.newFixedThreadPool(1);
                final Future<HttpRetrieveResponse> res = httpPool.submit(httpClient);

                httpRetrieveResponse = res.get();
                // blocks until we get the response
                //httpRetrieveResponse = httpClient.call();


                // checks if the link was a redirect link
                if(httpRetrieveResponse.getRedirectionPath().size() == depth+1) {
                    url = httpRetrieveResponse.getRedirectionPath().get(depth);
                    httpRetrieveResponse = clearDataAndKeepRedirects(httpRetrieveResponse, path);
                    redirect = true;
                }
            } while(redirect);

            httpRetrieveResponse.setUrl(new URL(task.getUrl()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return httpRetrieveResponse;
    }

    /**
     * Classifies the downloaded content in one of the existent categories.
     * @param task contains all the information for this task
     * @return - the matching category
     */
    private ContentType classifyUrl(RetrieveUrl task) {
        final String path = pathToSave + "/" + task.getReferenceId();
        try {
            String type = MediaChecker.getMimeType(path);
            if(type.startsWith("image")) {
                return ContentType.IMAGE;
            }
            if(type.startsWith("audio")) {
                return ContentType.AUDIO;
            }
            if(type.startsWith("video")) {
                return ContentType.VIDEO;
            }
            if(type.startsWith("text")) {
                return ContentType.TEXT;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return ContentType.UNKNOWN;
    }

    /**
     * If we received redirect than we clear the response object and keep only the list of redirect links.
     * @param oldHttpRetrieveResponse - the current HttpRetrieveResponse object
     * @return - a new HttpRetrieveResponse object
     */
    private HttpRetrieveResponse clearDataAndKeepRedirects(HttpRetrieveResponse oldHttpRetrieveResponse, String path) {
        final List<String> temp = oldHttpRetrieveResponse.getRedirectionPath();
        HttpRetrieveResponse newHttpRetrieveResponse = null;
        try {
            newHttpRetrieveResponse =
                    httpRetrieveResponseFactory.create(responseType, path);
            newHttpRetrieveResponse.setRedirectionPath(temp);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return newHttpRetrieveResponse;
    }

    /**
     * Extracts image meta data
     * @param httpRetrieveResponse finished job response with all the collected information
     * @return - an object with all the meta info
     */
    private ImageMetaInfo extractImageMetadata(HttpRetrieveResponse httpRetrieveResponse) {
        final String filePath = ((HttpRetrieveResponseDiskStorage)httpRetrieveResponse).getAbsolutePath();

        ImageMetaInfo imageMetaInfo = null;

        try {
            final ImageInfo imageInfo = MediaChecker.getImageInfo(filePath);

            imageMetaInfo =
                    new ImageMetaInfo(imageInfo.getWidth(), imageInfo.getHeight(), imageInfo.getMimeType(),
                            imageInfo.getFileFormat(), imageInfo.getColorSpace());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InfoException e) {
            e.printStackTrace();
        }

        return imageMetaInfo;
    }

    /**
     * Extracts audio meta data
     * @param httpRetrieveResponse finished job response with all the collected information
     * @return - an object with all the meta info
     */
    private AudioMetaInfo extractAudioMetadata(HttpRetrieveResponse httpRetrieveResponse) {
        final String filePath = ((HttpRetrieveResponseDiskStorage)httpRetrieveResponse).getAbsolutePath();

        AudioMetaInfo audioMetaInfo = null;

        try {
            final AudioInfo audioInfo = MediaChecker.getAudioInfo(filePath);

            audioMetaInfo =
                    new AudioMetaInfo(audioInfo.getSampleRate(), audioInfo.getBitRate(), audioInfo.getDuration(),
                            audioInfo.getMimeType(), audioInfo.getFileFormat());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return audioMetaInfo;
    }

    /**
     * Extracts video meta data
     * @param httpRetrieveResponse finished job response with all the collected information
     * @return - an object with all the meta info
     */
    private VideoMetaInfo extractVideoMetaData(HttpRetrieveResponse httpRetrieveResponse) {
        final String filePath = ((HttpRetrieveResponseDiskStorage)httpRetrieveResponse).getAbsolutePath();

        VideoMetaInfo videoMetaInfo = null;

        try {
            final VideoInfo videoInfo = MediaChecker.getVideoInfo(filePath);

            videoMetaInfo =
                    new VideoMetaInfo(videoInfo.getWidth(), videoInfo.getHeight(), videoInfo.getDuration(),
                            videoInfo.getMimeType(), videoInfo.getFileFormat(), videoInfo.getFrameRate());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return videoMetaInfo;
    }

    /**
     * Starts pinging a machine
     * @param ip the machines ip address
     * @param nrOfPings how many times you want to ping it
     * @param pingTimeout timeout in milliseconds
     * @return - all the information about this job in one object
     */
    private DonePing startPinging(String ip, int nrOfPings, int pingTimeout) {
        List<Long> responses = new ArrayList<Long>();
        long temp;

        for(int i=0; i<nrOfPings; i++) {
            log.info("Pinging #" + (i+1) + ": " + ip);
            try {
                final InetAddress address = InetAddress.getByName(ip);

                final long currentTime = System.currentTimeMillis();
                boolean success = address.isReachable(pingTimeout);
                temp = (System.currentTimeMillis() - currentTime);

                if(success) {
                    responses.add(temp);
                    System.out.println("Response in: " + temp + " ms");
                } else {
                    System.out.println("Timeout(" + (i+1) + ") at: " + ip);
                }


            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(responses);

        if(responses.size() != 0) {
            final long avg = sum(responses) / responses.size();
            final long min = Collections.min(responses);
            final long max = Collections.max(responses);
            final long median = median(responses);

            final DonePing donePing = new DonePing(ip, avg, min, max, median);

            return donePing;
        } else {
            return new DonePing();
        }
    }

    /**
     * Calculates the sum of a list of numbers
     * @param list list of numbers
     * @return - the result
     */
    public Long sum(List<Long> list) {
        Long sum= 0l;

        for (Long i:list)
            sum = sum + i;
        return sum;
    }

    /**
     * Calculates the median of a list of numbers
     * @param list list of numbers
     * @return - the result
     */
    public long median(List<Long> list){
        int middle = list.size()/2;

        if (list.size() % 2 == 1) {
            return list.get(middle);
        } else {
            return (list.get(middle-1) + list.get(middle)) / 2;
        }
    }

}
