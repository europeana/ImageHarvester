package eu.europeana.harvester.cluster.slave;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.messages.DonePing;
import eu.europeana.harvester.cluster.domain.messages.StartPing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This type of actor pings a server and creates statistics on its response type.
 */
public class PingerSlaveActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof StartPing) {
            final StartPing startPing = (StartPing)message;
            final DonePing donePing =
                    startPinging(startPing.getIp(), startPing.getNrOfPings(), startPing.getPingTimeout());

            getSender().tell(donePing, getSelf());

            return;
        }
    }

    /**
     * Starts pinging a machine
     * @param ip the machines ip address
     * @param nrOfPings how many times you want to ping it
     * @param pingTimeout timeout in milliseconds
     * @return - all the information about this job in one object
     */
    private DonePing startPinging(String ip, int nrOfPings, int pingTimeout) {
        final List<Long> responses = new ArrayList<Long>();
        long temp;

        for(int i=0; i<nrOfPings; i++) {
            LOG.debug("Pinging #{} : {}", (i + 1), ip);
            try {
                final InetAddress address = InetAddress.getByName(ip);

                final long currentTime = System.currentTimeMillis();
                boolean success = address.isReachable(pingTimeout);
                temp = (System.currentTimeMillis() - currentTime);

                if(success) {
                    responses.add(temp);
                    LOG.debug("Response in: {} ms", temp);
                } else {
                    LOG.debug("Timeout({}) at: ", (i+1), ip);
                }
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage());
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
        Collections.sort(responses);

        if(responses.size() != 0) {
            final double avg = sum(responses) / responses.size();
            final long min = Collections.min(responses);
            final long max = Collections.max(responses);
            final double median = median(responses);

            return new DonePing(ip, avg, min, max, median);
        } else {
            return new DonePing();
        }
    }

    /**
     * Calculates the sum of a list of numbers
     * @param list list of numbers
     * @return - the result
     */
    private Long sum(List<Long> list) {
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
    private double median(List<Long> list){
        int middle = list.size()/2;

        if (list.size() % 2 == 1) {
            return list.get(middle);
        } else {
            return (list.get(middle-1) + list.get(middle)) / 2;
        }
    }
}
