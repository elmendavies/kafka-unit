package elmendavies.kafka.unit;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

public class KafkaUnit {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);
    
    private KafkaServerStartable broker;

    private TestingServer zookeeper;
    private final String zookeeperString;
    private final String brokerString;
    private int zkPort;
    private int brokerPort;
    private Properties kafkaBrokerConfig = new Properties();

    public KafkaUnit() throws IOException {
        this(getEphemeralPort(), getEphemeralPort());
    }
   
    public KafkaUnit(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    private static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }


    public void startup() throws Exception {
    	LOGGER.info("Starting up Apache Zookeeper...");
    	
//        zookeeper = new TestableZooKeeperServerMain(zkPort);
//        zookeeper.startup();
        
        zookeeper = new TestingServer(zkPort);

        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        }));
        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
        kafkaBrokerConfig.setProperty("advertised.host.name", "localhost");
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("broker.id", "0");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        
    	LOGGER.info("Starting up Apache Kafka...");
    	
        broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
        broker.startup();
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperString;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());

        // run
        LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.createTopic(zkUtils, opts);
    }

    public void shutdown() throws IOException {
        if (broker != null) {
        	LOGGER.info("Shutting down Apache Kafka");
        	broker.shutdown();
        }
        if (zookeeper != null) {
        	LOGGER.info("Shutting down Apache Zookeeper");
        	zookeeper.close();
        }
    }   


    /**
     * Set custom broker configuration.
     * See avaliable config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    
}
