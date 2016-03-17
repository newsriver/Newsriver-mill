package ch.newsriver.mill;

import ch.newsriver.data.html.HTML;
import ch.newsriver.data.url.BaseURL;
import ch.newsriver.executable.BatchInterruptibleWithinExecutorPool;
import ch.newsriver.util.http.HttpClientPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.intenthq.gander.Gander;
import com.intenthq.gander.PageInfo;
import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import scala.Option;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
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

    private  LanguageDetector languageDetector;

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

        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();

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
                for (ConsumerRecord<String, String> record : records) {
                    this.waitFreeBatchExecutors(BATCH_SIZE);
                    MillMain.addMetric("URLs in", 1);

                    supplyAsyncInterruptWithin(() -> {
                        HTML html = null;
                        try {
                            html = mapper.readValue(record.value(), HTML.class);
                        } catch (IOException e) {
                            logger.error("Error deserialising BaseURL", e);
                            return null;
                        }
                        String pageText = Jsoup.parseBodyFragment(html.getRawHTML()).body().text();
                        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
                        TextObject textObject = textObjectFactory.forText(pageText);
                        List<DetectedLanguage> langList = languageDetector.getProbabilities(textObject);
                        if(langList.size()==0){
                            
                        }

                        languageDetector.detect(pageText);
                        try {
                            PageInfo pageInfo = Gander.extract(html.getRawHTML(), "it").get();

                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.print(e.getMessage());
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
