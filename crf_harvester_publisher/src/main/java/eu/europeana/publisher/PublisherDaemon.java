package eu.europeana.publisher;

import com.typesafe.config.*;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;

public class PublisherDaemon implements Daemon {

    private static final Logger LOG = LogManager.getLogger(PublisherDaemon.class.getName());

    private Publisher publisher;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.info("Initializing publisher");

        String[] args = daemonContext.getArguments();

        String configFilePath;
        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/publishing.conf";
        } else if(1 == args.length) {
            configFilePath = args[0];
        }
        else {
            configFilePath = args[0];

        }

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        }
        catch (ConfigException.Null e) {
            LOG.info("startTimestamp is null");
        }


        String startTimestampFile = null;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (StringUtils.isEmpty(file)) {
                    LOG.info("File is empty => startTimestamp is null");
                }
                else {
                    startTimestamp = DateTime.parse(file.trim());
                    LOG.info("startTimestamp loaded from file is "+startTimestamp);
                }
            }
            else {
                LOG.info("startTimestampFile is null");
            }
        }
        catch (ConfigException.Null e) {
            LOG.info("startTimestampFile is null");
        }
        catch (FileNotFoundException e) {
            LOG.info("Timestamp file doesn't exist => startTimestampFile is null");
        }

        publisher = new Publisher(startTimestamp,startTimestampFile,config);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting publisher");

        publisher.start();
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping publisher");

        publisher.stop();
    }

    @Override
    public void destroy() {
        LOG.info("Destroying publisher");
    }
}
