package eu.europeana.harvester.cluster.master.accountants;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.messages.inner.GetListOfIPs;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.domain.JobPriority;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountantDispatcherActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private ActorRef accountantFastLaneActor;
    private ActorRef accountantActor;

    public AccountantDispatcherActor(){

        this.accountantActor = getContext().system().actorOf(Props.create(AccountantMasterActor.class), "accountantMaster");
        this.accountantFastLaneActor = getContext().system().actorOf(Props.create(AccountantMasterActor.class), "accountantFastLane");

    }

    @Override
    public void onReceive(Object message) throws Exception {
        try {

            if ( message instanceof GetListOfIPs) {
                accountantFastLaneActor.forward(message, getContext());
                return;
            }



            if (message instanceof GetNumberOfTasks) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof CheckIPsWithJobs) {
                accountantActor.forward(message,getContext());
                return;
            }

            if (message instanceof IsJobLoaded) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTask) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetConcreteTask) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTaskState) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTasksFromIP) {
                final JobPriority prio = ((GetTasksFromIP)message).getJobPriority();
                if ( prio == JobPriority.NORMAL )
                    accountantActor.forward(message,getContext());
                else
                    accountantFastLaneActor.forward(message,getContext());
                return;

            }

            if (message instanceof GetTasksFromJob) {
                accountantActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTaskStatesPerJob) {
                accountantActor.forward(message, getContext());
                return;
            }


            if (message instanceof ModifyState) {
                accountantActor.forward(message,getContext());
                return;
            }

            if (message instanceof AddTask) {
                final JobPriority prio = ((AddTask)message).getJobPriority();
                if ( prio == JobPriority.NORMAL )
                    accountantActor.forward(message,getContext());
                else
                    accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof AddTasksToJob) {
                JobPriority prio = ((AddTasksToJob) message).getJobPriority();
                if ( prio == JobPriority.NORMAL )
                    accountantActor.forward(message,getContext());
                else
                    accountantFastLaneActor.forward(message,getContext());
                return;
            }



            if (message instanceof RemoveJob) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof RemoveTask) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof RemoveTaskFromIP) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof Monitor) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof CleanIPs) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());
                return;
            }

            if (message instanceof Clean) {
                accountantActor.forward(message,getContext());
                accountantFastLaneActor.forward(message,getContext());

                return;
            }

            if (message instanceof PauseTasks) {
                accountantActor.forward(message,getContext());
                return;
            }

            if (message instanceof ResumeTasks) {
                accountantActor.forward(message,getContext());
                return;
            }

            if (message instanceof GetRetrieveUrl) {
                final JobPriority prio = ((GetRetrieveUrl)message).getJobPriority();
                if ( prio == JobPriority.NORMAL )
                    accountantActor.forward(message,getContext());
                else
                    accountantFastLaneActor.forward(message,getContext());
                return;


            }

            if ( message instanceof GetOverLoadedIPs) {
                accountantActor.forward(message,getContext());
                return;
            }
        } catch(Exception e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_ACCOUNTANT),
                    "General exception in accountant actor.", e);
            // TODO : Evaluate if it is acceptable to hide the exception here.;
        }
    }


}
