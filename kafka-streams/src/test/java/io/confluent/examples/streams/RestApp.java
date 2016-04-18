package io.confluent.examples.streams;

import org.eclipse.jetty.server.Server;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity;

/**
 * Copied verbatim from `https://github.com/confluentinc/schema-registry/blob/107abb02927f5cc9608a3823b2a33bbc22d881eb/core/src/test/java/io/confluent/kafka/schemaregistry/RestApp.java`.
 */
public class RestApp {

  public final Properties prop;
  public RestService restClient;
  public SchemaRegistryRestApplication restApp;
  public Server restServer;
  public String restConnect;

  public RestApp(int port, String zkConnect, String kafkaTopic) {
    this(port, zkConnect, kafkaTopic, AvroCompatibilityLevel.NONE.name);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic, String compatibilityType) {
    this(port, zkConnect, kafkaTopic, compatibilityType, true);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic,
                 String compatibilityType, boolean masterEligibility) {
    prop = new Properties();
    prop.setProperty(SchemaRegistryConfig.PORT_CONFIG, ((Integer) port).toString());
    prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic);
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, compatibilityType);
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility);
  }

  public void start() throws Exception {
    restApp = new SchemaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
    restConnect = restServer.getURI().toString();
    if (restConnect.endsWith("/"))
      restConnect = restConnect.substring(0, restConnect.length() - 1);
    restClient = new RestService(restConnect);
  }

  public void stop() throws Exception {
    restClient = null;
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }

  public boolean isMaster() {
    return restApp.schemaRegistry().isMaster();
  }

  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    restApp.schemaRegistry().setMaster(schemaRegistryIdentity);
  }

  public SchemaRegistryIdentity myIdentity() {
    return restApp.schemaRegistry().myIdentity();
  }

  public SchemaRegistryIdentity masterIdentity() {
    return restApp.schemaRegistry().masterIdentity();
  }

  public SchemaRegistry schemaRegistry() {
    return restApp.schemaRegistry();
  }

}
