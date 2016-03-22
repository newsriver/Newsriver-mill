package ch.newsriver.mill;

import ch.newsriver.dao.ElasticsearchPoolUtil;
import ch.newsriver.data.content.Article;
import ch.newsriver.data.html.HTML;
import ch.newsriver.executable.BatchInterruptibleWithinExecutorPool;
import ch.newsriver.mill.extractor.GanderArticleExtractor;
import ch.newsriver.util.http.HttpClientPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.Properties;

/**
 * Created by eliapalme on 11/03/16.
 */
public class Mill extends BatchInterruptibleWithinExecutorPool implements Runnable {

    private static final Logger logger = LogManager.getLogger(Mill.class);
    private boolean run = false;
    private static final int BATCH_SIZE = 250;
    private static final int POOL_SIZE = 50;
    private static final int QUEUE_SIZE = 500;

    private static final ObjectMapper mapper = new ObjectMapper();
    Consumer<String, String> consumer;
    Producer<String, String> producer;


    public Mill() throws IOException {

        super(POOL_SIZE, QUEUE_SIZE);
        run = true;

        try {
            HttpClientPool.initialize();
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Unable to initialize http connection pool", e);
            run = false;
            return;
        } catch (KeyStoreException e) {
            logger.error("Unable to initialize http connection pool", e);
            run = false;
            return;
        } catch (KeyManagementException e) {
            logger.error("Unable to initialize http connection pool", e);
            run = false;
            return;
        }

        Properties props = new Properties();
        InputStream inputStream = null;
        try {

            String propFileName = "kafka.properties";
            inputStream = Mill.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {
            logger.error("Unable to load kafka properties", e);
        } finally {
            try {
                inputStream.close();
            } catch (Exception e) {
            }
        }


        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("raw-html"));
        producer = new KafkaProducer(props);


    }

    public void stop() {
        run = false;
        HttpClientPool.shutdown();
        this.shutdown();
        consumer.close();
        producer.close();
    }


    public void run() {

        while (run) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                MillMain.addMetric("HTMLs in", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    this.waitFreeBatchExecutors(BATCH_SIZE);

                    supplyAsyncInterruptWithin(() -> {
                        HTML html = null;
                        try {
                            html = mapper.readValue(record.value(), HTML.class);
                        } catch (IOException e) {
                            logger.error("Error deserializing BaseURL", e);
                            return null;
                        }

                        GanderArticleExtractor extractor = new GanderArticleExtractor();
                        Article article = extractor.extract(html);

                        if (article != null) {


                            Client client = null;
                            client = ElasticsearchPoolUtil.getInstance().getClient();

                            try {
                                IndexRequest indexRequest = new IndexRequest("newsriver", "article");
                                indexRequest.source(mapper.writeValueAsString(article));
                                IndexResponse response = client.index(indexRequest).actionGet();

                                if (response.isCreated()) {
                                    article.setId(response.getId());
                                    String json = null;
                                    try {
                                        json = mapper.writeValueAsString(article);
                                    } catch (IOException e) {
                                        logger.fatal("Unable to serialize mill result", e);
                                        return null;
                                    }
                                    producer.send(new ProducerRecord<String, String>("raw-article", html.getReferral().getNormalizeURL(), json));
                                    MillMain.addMetric("Articles out", records.count());
                                }
                            } catch (Exception e) {
                                logger.error("Unable to save article in elasticsearch", e);
                            } finally {
                            }

                            try {
                                IndexRequest indexRequest = new IndexRequest("newsriver-publisher", "publisher", article.getPublisher().getDomainName());
                                indexRequest.source(mapper.writeValueAsString(article.getPublisher()));
                                client.index(indexRequest).actionGet();
                            } catch (Exception e) {
                                logger.error("Unable to save publisher", e);
                            } finally {
                            }

                        }

                        return null;
                    }, Duration.ofSeconds(60), this)
                            .exceptionally(throwable -> {
                                logger.error("HTMLFetcher unrecoverable error.", throwable);
                                return null;
                            });

                }
            } catch (InterruptedException ex) {
                logger.warn("Miner job interrupted", ex);
                run = false;
                return;
            } catch (BatchSizeException ex) {
                logger.fatal("Requested a batch size bigger than pool capability.");
            }
            continue;
        }


    }

}
