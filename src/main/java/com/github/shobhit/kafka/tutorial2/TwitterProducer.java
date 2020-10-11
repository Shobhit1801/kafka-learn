package com.github.shobhit.kafka.tutorial2;

import com.github.shobhit.kafka.tutorial1.ProducerDemoWithCallBack;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        new TwitterProducer().run();
    }

    public void run() throws InterruptedException, IOException {
        // get a twitter client
        Client client = getTwitterClient(msgQueue);

        //get kafka producer
        KafkaProducer<String, String> producer = getKafkaProducer();

        while(!client.isDone()) {
            String topic = "first_topic";
            String value = msgQueue.take();
            //create a producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n"+
                                "Record: " + record.toString() + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Some error occurred:",e);
                    }
                }
            });
        }

        client.stop();
        producer.flush();
        producer.close();

    }

    public Properties getTwitterAPIKeys() throws IOException {
        Properties prop=new Properties();
        FileInputStream ip= new FileInputStream("C:/kafka-learn/twitter_config.properties");
        prop.load(ip);
        return prop;
    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue) throws IOException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(5678L, 566888L);
        List<String> terms = Lists.newArrayList("india", "politics", "cricket");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // fetch twitter config properties
        Properties twitterConfigProps = getTwitterAPIKeys();

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(twitterConfigProps.getProperty("APIKey"), twitterConfigProps.getProperty("APISecretKey"), twitterConfigProps.getProperty("AccessToken"), twitterConfigProps.getProperty("AccessSecretToken") );

        Logger logger = LoggerFactory.getLogger(TwitterTest.class.getName());

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        hosebirdClient.connect();
        return hosebirdClient;
    }

    public KafkaProducer<String,String> getKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // batching and compression (inc throughput but dec latency)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 Kb

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        logger.info("Exit producer func");
        return producer;
    }
}
