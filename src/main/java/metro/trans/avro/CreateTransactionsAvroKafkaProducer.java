package metro.trans.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTransactionsAvroKafkaProducer {

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic, final Properties cloudConfig)
    {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        }
        catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Please provide command line arguments: configPath topic");
            System.exit(1);
        }

        /*
            Load properties from a local configuration file
            Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
            to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
            Follow these instructions to create this file: https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
         */
        final Properties props = loadConfig(args[0]);

        // Create topic if needed
        final String topic = args[1];
        createTopic(topic, props);

        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Producer<String, Transactions> producer = new KafkaProducer<String, Transactions>(props);

        String transactionId = "3";
        String accountId = "IBAN0000000031673629402517";
        String msgType = "PAC0009";
        String date = "2021-05-12T14:59:27";
        double amount = -1009.23;

        // Produce sample data
        Transactions record = getTransaction(transactionId, accountId, msgType, date, amount);

        producer.send(new ProducerRecord<String, Transactions>(topic, record), new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                }
            }
        });
        producer.flush();
        System.out.printf("<M>essages were produced to topic %s%n", topic);
        producer.close();

    }

    private static Transactions getTransaction(String transactionId, String accountId, String msgType, String date, double amount) {
        BusinessKey businessKey = new BusinessKey();
        businessKey.setTxId( transactionId );

        MetaData md = new MetaData();
        md.setMsgType(msgType);
        md.setMdInsertDtTm(date);

        Details d = new Details();
        d.setDate(date);
        d.setAmount(amount);

        Transactions t = new Transactions();
        t.setAccountId( accountId );
        t.setTranactionId(businessKey.getTxId());
        t.setDataPayLoad(d);
        t.setBusinessKey(businessKey);
        t.setMetadata(md);
        return t;
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
