package ch.newsriver.mill;

import ch.newsriver.dao.ElasticsearchPoolUtil;
import ch.newsriver.data.content.Article;
import ch.newsriver.data.html.HTML;
import ch.newsriver.data.url.BaseURL;
import ch.newsriver.data.url.ManualURL;
import ch.newsriver.executable.Main;
import ch.newsriver.executable.poolExecution.BatchInterruptibleWithinExecutorPool;
import ch.newsriver.mill.extractor.GanderArticleExtractor;
import ch.newsriver.performance.MetricsLogger;
import ch.newsriver.processor.Output;
import ch.newsriver.processor.Processor;
import ch.newsriver.processor.StreamProcessor;
import ch.newsriver.util.http.HttpClientPool;
import ch.newsriver.data.website.WebSite;
import ch.newsriver.data.website.WebSiteFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

import java.util.Properties;

/**
 * Created by eliapalme on 11/03/16.
 */
public class Mill implements StreamProcessor<HTML, Article> {

    private static final Logger logger = LogManager.getLogger(Mill.class);
    private static final MetricsLogger metrics = MetricsLogger.getLogger(Mill.class, Main.getInstance().getInstanceName());

    private static final ObjectMapper mapper = new ObjectMapper();



    public Mill()  {



        try {
            HttpClientPool.initialize();
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Unable to initialize http connection pool", e);

            return;
        } catch (KeyStoreException e) {
            logger.error("Unable to initialize http connection pool", e);

            return;
        } catch (KeyManagementException e) {
            logger.error("Unable to initialize http connection pool", e);

            return;
        }


    }

    public void close() {

        HttpClientPool.shutdown();


    }

    /*
    public void run() {
        metrics.logMetric("start", null);
        while (run) {
            try {
                this.waitFreeBatchExecutors(batchSize);
                //TODO: need to decide if keep this.
                //metrics.logMetric("processing batch");

                ConsumerRecords<String, String> records;
                if(this.isPriority()){
                    records = consumer.poll(250);
                }else{
                    records = consumer.poll(60000);
                }

                MillMain.addMetric("HTMLs in", records.count());
                for (ConsumerRecord<String, String> record : records) {

                    supplyAsyncInterruptExecutionWithin(() -> {

                        Output<HTML, Article> output = this.process(record.value());
                        if (output.isSuccess()) {

                            String json = null;
                            try {
                                json = mapper.writeValueAsString(output.getOutput());
                            } catch (IOException e) {
                                logger.fatal("Unable to serialize mill result", e);
                                return null;
                            }
                            if (output.getIntput().getReferral() instanceof ManualURL) {
                                producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), "Content extraction completed."));
                                producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), json));
                            }


                                producer.send(new ProducerRecord<String, String>("raw-article"+priorityPostFix, output.getOutput().getUrl(), json));


                            if (output.getOutput().getWebsite() == null) {
                                URI articleURI = null;
                                try {
                                    articleURI = new URI(output.getOutput().getUrl());
                                } catch (URISyntaxException e) {
                                }
                                //The website is unknow instruct Intell to getter informarion about the website and update the article
                                producer.send(new ProducerRecord<String, String>("website-url", articleURI.getScheme() + "://" + articleURI.getHost(), output.getOutput().getId()));
                                metrics.logMetric("submitted website-url", null);
                            }
                        } else {
                            if (output.getIntput().getReferral() instanceof ManualURL) {
                                producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), "Error: unable to extract main content."));
                            }
                        }
                        return null;

                    }, this)
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

*/

    @Override
    public KeyValue<String, Article> process(String key, HTML html) {



        Article article = null;


        String urlHash = "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] hash = digest.digest(html.getUrl().getBytes(StandardCharsets.UTF_8));
            urlHash = Base64.encodeBase64URLSafeString(hash);
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Unable to compute URL hash", e);
            return null;
        }

        Client client = null;
        client = ElasticsearchPoolUtil.getInstance().getClient();


        try {
            GetResponse response = client.prepareGet("newsriver", "article", urlHash).execute().actionGet();
            if (response.isExists()) {
                try {
                    article = mapper.readValue(response.getSourceAsString(), Article.class);
                } catch (IOException e) {
                    logger.fatal("Unable to deserialize article", e);
                    return null;
                }
            }
        } catch (Exception e) {
            logger.error("Unable to get article from elasticsearch", e);
            return null;
        }


        if (article != null) {
            //Check if the article already contains this
            final BaseURL referral = html.getReferral();
            if (referral != null) {
                boolean notFound = article.getReferrals().stream().noneMatch(baseURL -> baseURL.getReferralURL() != null && baseURL.getReferralURL().equals(referral.getReferralURL()));
                if (!notFound) {
                    logger.warn("Found a duplicate referral to the same article, Newsriver-Scout should prevent this.");
                } else {
                    article.getReferrals().add(html.getReferral());
                }
            }
        } else {
            GanderArticleExtractor extractor = new GanderArticleExtractor();
            article = extractor.extract(html);
            if (article == null) {
                logger.warn("Gander was unable to extract the content for:" + html.getUrl());
                MillMain.addMetric("Articles missed", 1);
            }
        }

        if (article == null && html.isAlreadyFetched()) {
            logger.warn("An article that was supposed to have already been fetched has not been found.");
            return null;
        }

        if (article != null) {
            URI articleURI = null;
            if (article.getWebsite() == null) {
                try {
                    articleURI = new URI(article.getUrl());
                    WebSite webSite = WebSiteFactory.getInstance().getWebsite(articleURI.getHost().toLowerCase());
                    article.setWebsite(webSite);
                } catch (URISyntaxException e) {

                }
            }
            try {

                IndexRequest indexRequest = new IndexRequest("newsriver", "article", urlHash);
                indexRequest.source(mapper.writeValueAsString(article));
                IndexResponse response = client.index(indexRequest).actionGet();
                if (response != null && response.getId() != null && !response.getId().isEmpty()) {
                    article.setId(response.getId());
                    MillMain.addMetric("Articles out", 1);
                    if (response.isCreated()) {
                        metrics.logMetric("submitted raw-article", html.getReferral());

                    } else {
                        metrics.logMetric("submitted raw-article update", html.getReferral());

                    }
                }
                return new KeyValue(key,article);

            } catch (Exception e) {
                logger.error("Unable to save article in elasticsearch", e);

                return new KeyValue(key,article);
            }

        }
        return null;


    }

}
