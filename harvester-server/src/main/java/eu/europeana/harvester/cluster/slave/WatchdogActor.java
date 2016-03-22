package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.Slave;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

public class WatchdogActor extends UntypedActor {
    private final Slave slave;
    private static final Long MIN_DONE_TASKS_PER_HOUR = 100l;
    private static final FiniteDuration INTERVAL = scala.concurrent.duration.Duration.create(1, TimeUnit.HOURS);
    private Long previousDoneProcessingTotalCounter = 0l;

    private class CheckActivity {}

    public static ActorRef createActor(final ActorSystem system, final Slave slave) {
        return system.actorOf(Props.create(WatchdogActor.class, slave));
    }

    public WatchdogActor(final Slave slave) {
        this.slave = slave;
    }

    @Override
    public void preStart() throws Exception {
        previousDoneProcessingTotalCounter = 0l;
        schedule();
    }

        @Override
    public void onReceive(Object message) throws Exception {
            if(message instanceof CheckActivity) {
                executeCheckActivity();
                schedule();
                return ;
            }
        }

    private void schedule() {
        getContext().system().scheduler().scheduleOnce(INTERVAL, getSelf(), new CheckActivity(), getContext().system().dispatcher(), getSelf());
    }

    private void executeCheckActivity() {
        if (SlaveMetrics.Worker.Master.doneProcessingTotalCounter.getCount() - previousDoneProcessingTotalCounter < MIN_DONE_TASKS_PER_HOUR) {
            slave.restart();
        }
        previousDoneProcessingTotalCounter = SlaveMetrics.Worker.Master.doneProcessingTotalCounter.getCount();
    }

}
