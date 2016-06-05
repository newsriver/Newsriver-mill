package ch.newsriver.mill;

import ch.newsriver.data.content.Article;
import ch.newsriver.data.html.HTML;
import ch.newsriver.data.url.BaseURL;
import ch.newsriver.data.url.ManualURL;
import ch.newsriver.data.url.SeedURL;
import ch.newsriver.executable.Main;
import ch.newsriver.executable.StreamExecutor;
import ch.newsriver.executable.poolExecution.MainWithPoolExecutorOptions;
import ch.newsriver.performance.MetricsLogger;
import ch.newsriver.util.JSONSerde;
import com.google.common.base.Predicates;
import org.apache.commons.cli.Options;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Option;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by eliapalme on 11/03/16.
 */
public class MillMain extends StreamExecutor implements Thread.UncaughtExceptionHandler {

    private static final int DEFAUTL_PORT = 9098;
    private static final Logger logger = LogManager.getLogger(MillMain.class);
    private static MetricsLogger metrics;

    public int getDefaultPort(){
        return DEFAUTL_PORT;
    }

    static Mill mill;
    private KafkaStreams streams;

    public MillMain(String[] args){
        super(args,true);

    }

    public static void main(String[] args){
        new MillMain(args);
    }

    public void shutdown(){

        if(mill!=null)mill.close();
    }

    public void start(){

        metrics = MetricsLogger.getLogger(Mill.class, Main.getInstance().getInstanceName());


        mill = new Mill();


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
        } catch (java.lang.Exception e) {
            logger.error("Unable to load kafka properties", e);
        } finally {
            try {
                inputStream.close();
            } catch (java.lang.Exception e) {
            }
        }

        props.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "" + this.getPoolSize());


        final Serde<String> stringSerde = Serdes.String();


        final Serde<HTML> htmlSerde = new JSONSerde<>(HTML.class);
        final Serde<Article> articleSerde = new JSONSerde<>(Article.class);




        KStreamBuilder builder = new KStreamBuilder();


        KStream<String, HTML> urls = builder.stream(stringSerde, htmlSerde, "raw-html");


        KStream<String, Article> htmls = urls.map((url, html) -> {

            KeyValue<String, Article> output = mill.process(url, html);

            if (output!=null && output.value!=null) {


                if (output.value.getReferrals() instanceof ManualURL) {
                    //producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), "Content extraction completed."));
                    //producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), json));
                }

                //producer.send(new ProducerRecord<String, String>("raw-article"+priorityPostFix, output.getOutput().getUrl(), json));
                return output;
            } else {
                //if (output.value.getReferrals() instanceof ManualURL) {
                    //producer.send(new ProducerRecord<String, String>("processing-status", ((ManualURL) output.getIntput().getReferral()).getSessionId(), "Error: unable to extract main content."));
                //}
                return new KeyValue(url,null);
            }

        });

        Predicate<String, Article> hasArticle = (k, v) -> v != null;
        Predicate<String, Article> noWebsite = (k, v) -> v != null && v.getWebsite()==null;
        KStream<String, Article>[] split = htmls.branch(hasArticle, noWebsite);

        split[0].to(stringSerde, articleSerde, "raw-article");
        KStream<String, String> fetchWebsite = split[1].map( (k,v)-> {
            URI articleURI = null;
            try {
                articleURI = new URI(v.getUrl());
            } catch (URISyntaxException e) {
            }
            return new KeyValue<String,String>(articleURI.getScheme() + "://" + articleURI.getHost(), v.getId());
        });
        fetchWebsite.to("website-url");

        streams = new KafkaStreams(builder, props);
        streams.setUncaughtExceptionHandler(this);


        streams.start();

    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {

        logger.fatal("Mill uncaughtException", e);
    }

}
