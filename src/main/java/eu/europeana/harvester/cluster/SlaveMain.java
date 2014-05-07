package eu.europeana.harvester.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.cluster.master.NodeMasterActor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SlaveMain {

    public static void main(String[] args) {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        int nrOfSlaves = 3;

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 2551).
                withFallback(ConfigFactory.parseString("akka.cluster.roles = [nodeMaster]")).
                withFallback(ConfigFactory.load("remote"));

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.actorOf(Props.create(NodeMasterActor.class, channelFactory, hashedWheelTimer, nrOfSlaves),
                "nodeMaster");

        system.actorOf(Props.create(MetricsListener.class), "metricsListener");
    }
}
