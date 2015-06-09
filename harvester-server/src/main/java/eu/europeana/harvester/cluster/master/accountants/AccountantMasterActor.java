package eu.europeana.harvester.cluster.master.accountants;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.messages.inner.*;

public class AccountantMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private ActorRef accountantAllTasksActor;
    private ActorRef accountantTasksPerIPActor;
    private ActorRef accountantTasksPerJobActor;




    public AccountantMasterActor (){
        this.accountantAllTasksActor = getContext().system().actorOf(Props.create(AccountantAllTasksActor.class), "accountantAll");
        this.accountantTasksPerIPActor = getContext().system().actorOf(Props.create(AccountantTaskPerIPActor.class), "accountantPerIP");
        this.accountantTasksPerJobActor = getContext().system().actorOf(Props.create(AccountantTasksPerJobActor.class), "accountantPerJob");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        try {

            if (message instanceof GetNumberOfIPs) {
                accountantTasksPerIPActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetNumberOfTasks) {
                accountantAllTasksActor.forward(message, getContext());
                return;
            }

            if (message instanceof CheckIPsWithJobs) {
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }

            if (message instanceof IsJobLoaded) {
                accountantAllTasksActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTask) {
                accountantAllTasksActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetConcreteTask) {
                accountantAllTasksActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTaskState) {
                accountantAllTasksActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTasksFromIP) {
                accountantTasksPerIPActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTasksFromJob) {
                accountantTasksPerJobActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetTaskStatesPerJob) {
                accountantTasksPerJobActor.forward(message, getContext());
                return;
            }

            if (message instanceof GetNumberOfParallelDownloadsPerIP) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof ModifyState) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof AddTask) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof AddTasksToJob) {
                accountantTasksPerJobActor.forward(message,getContext());
                return;
            }

            if (message instanceof AddTasksToIP) {
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }

            if (message instanceof RemoveJob) {
                accountantTasksPerJobActor.forward(message,getContext());
                return;
            }

            if (message instanceof RemoveTask) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof RemoveTaskFromIP) {
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }

            if (message instanceof Monitor) {
                accountantAllTasksActor.forward(message,getContext());
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }

            if (message instanceof CleanIPs) {
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }

            if (message instanceof Clean) {
                accountantAllTasksActor.forward(message,getContext());
                accountantTasksPerIPActor.forward(message,getContext());
                accountantTasksPerJobActor.forward(message,getContext());

                return;
            }

            if (message instanceof PauseTasks) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof ResumeTasks) {
                accountantAllTasksActor.forward(message,getContext());
                return;
            }

            if (message instanceof GetRetrieveUrl) {
                accountantAllTasksActor.forward(message,getContext());
                return;

            }

            if ( message instanceof GetOverLoadedIPs) {
                accountantTasksPerIPActor.forward(message,getContext());
                return;
            }
        } catch(Exception e) {
            //e.printStackTrace();
            LOG.error("Accountant master actor: {}", e.getMessage());
        }
    }


}
