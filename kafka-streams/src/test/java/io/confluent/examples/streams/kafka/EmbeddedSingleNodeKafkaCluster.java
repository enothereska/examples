package io.confluent.examples.streams.kafka;

import org.apache.curator.test.InstanceSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import io.confluent.examples.streams.RestApp;
import io.confluent.examples.streams.zookeeper.ZooKeeperEmbedded;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedSingleNodeKafkaCluster {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);
  private static final String KAFKASTORE_TOPIC = "_schemas";
  private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

  private final ZooKeeperEmbedded zookeeper;
  private final KafkaEmbedded broker;
  private final RestApp schemaRegistry;

  /**
   * Creates and starts a Kafka cluster.
   */
  public EmbeddedSingleNodeKafkaCluster() throws Exception {
    this(new Properties());
  }

  /**
   * Creates and starts a Kafka cluster.
   *
   * @param brokerConfig Additional broker configuration settings.
   */
  public EmbeddedSingleNodeKafkaCluster(Properties brokerConfig) throws Exception {
    log.debug("Initiating embedded Kafka cluster startup");
    int zkPort = InstanceSpec.getRandomPort();
    log.debug("Starting a ZooKeeper instance on port {} ...", zkPort);
    zookeeper = new ZooKeeperEmbedded(zkPort);
    log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

    Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
    log.debug("Starting a Kafka instance on port {} ...", effectiveBrokerConfig.getProperty("port"));
    broker = new KafkaEmbedded(effectiveBrokerConfig);
    broker.start();
    log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
        broker.brokerList(), broker.zookeeperConnect());

    schemaRegistry = new RestApp(InstanceSpec.getRandomPort(), zookeeperConnect(), KAFKASTORE_TOPIC, AVRO_COMPATIBILITY_TYPE);
    schemaRegistry.start();
  }

  private Properties effectiveBrokerConfigFrom(Properties brokerConfig, ZooKeeperEmbedded zookeeper) {
    Properties effectiveConfig = new Properties();
    effectiveConfig.put("zookeeper.connect", zookeeper.connectString());
    int brokerPort = InstanceSpec.getRandomPort();
    effectiveConfig.put("port", String.valueOf(brokerPort));
    effectiveConfig.putAll(brokerConfig);
    return effectiveConfig;
  }

  /**
   * Stop the Kafka cluster.
   */
  public void stop() throws Exception {
    schemaRegistry.stop();
    broker.stop();
    zookeeper.stop();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka producers how to connect to this cluster.
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * You can use this to e.g. tell Kafka consumers how to connect to this cluster.
   */
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * The "schema.registry.url" setting of this schema registry instance.
   */
  public String schemaRegistryUrl() {
    return schemaRegistry.restConnect;
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(String topic) {
    createTopic(topic, 1, 1, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(String topic, int partitions, int replication) {
    createTopic(topic, partitions, replication, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(String topic,
                          int partitions,
                          int replication,
                          Properties topicConfig) {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

}