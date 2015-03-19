package eu.europeana.harvester.cluster;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SlaveDaemon implements Daemon {

    private static final Logger LOG = LogManager.getLogger(SlaveDaemon.class.getName());

    private Slave slave;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.info("Initializing slave");

        String[] args = daemonContext.getArguments();
        slave = new Slave(args);
        slave.init(slave);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting slave");

        slave.start();
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping slave");

        slave.getActorSystem().shutdown();
    }

    @Override
    public void destroy() {
        LOG.info("Destroying slave");
    }
}
