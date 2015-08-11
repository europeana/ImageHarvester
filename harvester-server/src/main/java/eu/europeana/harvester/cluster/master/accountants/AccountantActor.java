package eu.europeana.harvester.cluster.master.accountants;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RequestTasks;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class AccountantActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private AccountantActorHelper accountantActorHelper;

    private ActorRef masterReceiver;



    public AccountantActor(DefaultLimits defaultLimits){
        accountantActorHelper = new AccountantActorHelper(defaultLimits);
        masterReceiver = getContext().actorFor("../receiver");
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                TimeUnit.MINUTES), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {

            if (message instanceof GetNumberOfTasks) {
                getSender().tell(accountantActorHelper.getNumberOfTasks(), ActorRef.noSender());
                return;
            } else

            if ( message instanceof GetOverLoadedIPs) {
                getSender().tell(accountantActorHelper.getIPsWithTooManyTasks(1000), ActorRef.noSender());
                return;
            } else

            if(message instanceof RequestTasks) {

                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_SENDER),
                        "Received request for tasks from "+getSender());

                MasterMetrics.Master.sendJobSetToSlaveCounter.inc();
                final com.codahale.metrics.Timer.Context context = MasterMetrics.Master.sendJobSetToSlaveDuration.time();
                getSender().tell(accountantActorHelper.getBagOfTasks(),masterReceiver);
                context.stop();
                return;
            } else

            if (message instanceof AddTask) {
                accountantActorHelper.addTask((AddTask) message);
                return;
            } else

            if (message instanceof DoneProcessing) {
                accountantActorHelper.doneTask((DoneProcessing) message);
                return;
            } else

            if (message instanceof Monitor) {
                accountantActorHelper.monitor();
                return;
            } else

            if (message instanceof Clean) {
                accountantActorHelper.clean();
                getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                        TimeUnit.MINUTES), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());
                return;
            } else


            {
                unhandled(message);
            }
    }


}
