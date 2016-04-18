/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to work on Specific Avro data.
 */
public class SpecificAvroIntegrationTest {

  private static EmbeddedSingleNodeKafkaCluster cluster = null;
  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    cluster = new EmbeddedSingleNodeKafkaCluster();
    cluster.createTopic(inputTopic);
    cluster.createTopic(outputTopic);
  }

  @AfterClass
  public static void stopKafkaCluster() throws Exception {
    if (cluster != null) {
      cluster.stop();
    }
  }

  @Test
  public void shouldRoundTripSpecificAvroDataThroughKafka() throws Exception {
    List<WikiFeed> inputValues = Arrays.asList(
        WikiFeed.newBuilder().setUser("alice").setIsNew(true).setContent("lorem ipsum").build()
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    KStreamBuilder builder = new KStreamBuilder();

    SchemaRegistryClient client = new CachedSchemaRegistryClient(cluster.schemaRegistryUrl(), 1000);
    Properties avroSerdeConfig = new Properties();
    avroSerdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
    SpecificAvroSerde<WikiFeed> specificAvroSerde = new SpecificAvroSerde<>(client, (Map) avroSerdeConfig);

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "specific-avro-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zookeeperConnect());
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, specificAvroSerde.getClass().getName());

    // Write the input data as-is to the output topic.
    builder.stream(inputTopic).to(outputTopic);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
    // of the input data we produce below).
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(5000);

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

    // Give the stream processing application some time to do its work.
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(10000);
    streams.close();

    //
    // Step 3: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-avro-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
    List<WikiFeed> actualValues = IntegrationTestUtils.readValues(outputTopic, consumerConfig, inputValues.size());
    assertThat(actualValues).isEqualTo(inputValues);
  }

}