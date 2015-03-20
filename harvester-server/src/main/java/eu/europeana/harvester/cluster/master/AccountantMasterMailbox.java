package eu.europeana.harvester.cluster.master;

import akka.actor.ActorSystem;
import akka.dispatch.Envelope;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedPriorityMailbox;
import com.typesafe.config.Config;
import eu.europeana.harvester.cluster.domain.messages.inner.*;

import java.util.Comparator;

public class AccountantMasterMailbox extends UnboundedPriorityMailbox {

   private final static PriorityGenerator PRIO_GENERATOR = new PriorityGenerator() {
        @Override
        public int gen(Object message) {
            if ((message instanceof GetNumberOfTasks) ||
                    (message instanceof GetTask) ||
                    (message instanceof GetConcreteTask) ||
                    (message instanceof GetTaskState) ||
                    (message instanceof GetTasksFromIP) ||
                    (message instanceof GetTasksFromJob) ||
                    (message instanceof GetTaskStatesPerJob))
                return 1; // HIGH PIORITY
            else
                return 0; // LOW PRIORITY (default)
        }
    };

    public AccountantMasterMailbox(ActorSystem.Settings settings, Config config) {
        super(PRIO_GENERATOR);
    }

    @Override
    public Comparator<Envelope> cmp() {
        return new Comparator<Envelope>() {
            @Override
            public int compare(Envelope t1, Envelope t2) {
                return PRIO_GENERATOR.compare(t1,t2);
            }
        };
    }

    @Override
    public int initialCapacity() {
        return 1000;
    }
}
