package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.routing.AdaptiveLoadBalancingPool;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.cluster.routing.SystemLoadAverageMetricsSelector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.cluster.master.MasterActor;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterMain {

    public static void main(String[] args) {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        List<String> downloadLinks = Arrays.asList(
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

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0").
                withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]")).
                withFallback(ConfigFactory.load("master"));

        final ActorSystem system = ActorSystem.create("master", config);

        int totalInstances = 100;
        int maxInstancesPerNode = 10;
        boolean allowLocalRoutees = false;
        String useRole = "slave";

        ActorRef slaveRouter = system.actorOf(
                new ClusterRouterPool(new AdaptiveLoadBalancingPool(
                        SystemLoadAverageMetricsSelector.getInstance(), 0),
                        new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRole)).
                        props(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer)), "slaveRouter");

        final ActorRef master = system.actorOf(Props.create(MasterActor.class, downloadLinks, slaveRouter), "master");

        master.tell("start", ActorRef.noSender());
    }
}
