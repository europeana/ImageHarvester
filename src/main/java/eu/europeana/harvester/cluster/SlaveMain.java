package eu.europeana.harvester.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SlaveMain {

    public static void main(String[] args) {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        int nrOfSlaves = 5;

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0").
                withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [ \"akka.tcp://master@127.0.0.1:39876\"]")).
                withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]")).
                withFallback(ConfigFactory.load("slave"));


        final ActorSystem system = ActorSystem.create("slave", config);

        for(int i = 0; i < nrOfSlaves; i++) {
            String slaveName = "slave" + UUID.randomUUID().toString().replaceAll("-", "");

            System.out.println("Create: " + slaveName);
            system.actorOf(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer), slaveName);
        }

    }
}
