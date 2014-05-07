package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;

import java.util.Arrays;
import java.util.List;

public class MasterMain {

    public static void main(String[] args) {
        final List<String> downloadLinks = Arrays.asList(
                "http://edition.cnn.com",
                "http://download.thinkbroadband.com/5MB.zip",
                "http://download.thinkbroadband.com/10MB.zip",
                "http://download.thinkbroadband.com/20MB.zip",
                "http://download.thinkbroadband.com/50MB.zip",
                "http://download.thinkbroadband.com/100MB.zip",
                "http://jwst.nasa.gov/images3/flightmirrorarrive1.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive2.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive3.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive4.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive5.jpg"
        );

        final Config config = ConfigFactory.parseString(
                "akka.cluster.roles = [clusterMaster]").withFallback(
                ConfigFactory.load("cluster"));

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.log().info("Will start when 1 backend members in the cluster.");

        //#registerOnUp
        Cluster.get(system).registerOnMemberUp(new Runnable() {
            @Override
            public void run() {
                System.out.println("Joined");
                ActorRef clusterMaster =
                        system.actorOf(Props.create(ClusterMasterActor.class, downloadLinks),
                        "clusterMaster");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                clusterMaster.tell("start", ActorRef.noSender());
            }
        });

    }
}
