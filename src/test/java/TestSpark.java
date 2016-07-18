import ch.newsriver.dao.JDBCPoolUtil;
import ch.newsriver.data.content.Article;
import ch.newsriver.data.content.ArticleFactory;
import ch.newsriver.data.content.ArticleRequest;
import ch.newsriver.ml.classifier.news.category.ArticleTrainingSet;
import ch.newsriver.ml.classifier.news.category.TrainingDataHandler;
import ch.newsriver.util.http.HttpClientPool;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intenthq.gander.text.StopWords;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by eliapalme on 10/05/16.
 */
public class TestSpark {

    /*
    private static final ObjectMapper mapper = new ObjectMapper();
    private static JavaSparkContext sc;
    @Before
    public void initialize() throws Exception {

//       SparkConf conf = new SparkConf().setAppName("classifier").setMaster("local").set("spark.cores.max", "10");
//        sc = new JavaSparkContext(conf);




        Tech feeds

        //http://www.ajc.com/feeds/categories/technology/
        //http://www.washingtontimes.com/atom/headlines/culture/technology/
        //http://www.chron.com/rss/feed/AP-Technology-and-Science-266.php
        //http://technorumors.com/rss
        //http://mf.feeds.reuters.com/reuters/technologyNews
        //http://rss.nytimes.com/services/xml/rss/nyt/Technology.xml



    }

    @After
    public void shutdown() throws Exception {

    }



    @Test
    public void trainModel() throws URISyntaxException, Exception {

        SparkConf conf = new SparkConf().setAppName("classifier").setMaster("local").set("spark.cores.max", "10");
        sc = new JavaSparkContext(conf);

        HashingTF tf = new HashingTF();
        IDF idf = new IDF();

        List<String> categories = Arrays.asList(new String[]{"sport","business","technology","international","entertainment","life","motors","culture","science","usa","united kingdom","republic of ireland","canada","wine & dine","politics","tv & movies","travel","health","fashion"});

        JavaRDD<LabeledPoint> features = sc.emptyRDD();
        try {
            TrainingDataHandler handler = new TrainingDataHandler();

            Map<Integer,Map<Integer,ArticleTrainingSet>> trainingSet = handler.loadData();
            Map<Integer,ArticleTrainingSet> lang = trainingSet.get(3);


                for(ArticleTrainingSet set : lang.values()){

                    if(!categories.contains(set.getCategory().toLowerCase())) continue;


                    JavaRDD<List<String>> articlesKeywords = urlTokens(set.getArticles());
                    //articlesKeywords = articlesKeywords.union( textTokens(set.getArticles()));
                    final double label;
                    if(set.getCategory().equalsIgnoreCase("business")){
                        label=1d;
                    }else{
                        label=0d;
                    }
                    features = features.union(articlesKeywords.map(keywords ->new LabeledPoint(label, tf.transform(keywords))));

                }


        }catch (Exception e){
            throw e;
        }



        JavaRDD<Vector>  x = features.map( f -> f.features());
        IDFModel idfModel = idf.fit(x);

        features = features.map(lablePoint ->{ idfModel.transform(lablePoint.features()); return  lablePoint;} );

        features.cache();

        JavaRDD<LabeledPoint>[] tainingAndTest = features.randomSplit(new double[]{0.5,0.5,0});

        NaiveBayesModel model = NaiveBayes.train(tainingAndTest[0].rdd());




        JavaRDD<Tuple2<Object,Object>> predictionLabel = tainingAndTest[0].map(point -> {
            return new Tuple2(model.predict(point.features()),point.label());
        });
        predictionLabel.cache();


        MulticlassMetrics metricTrain = new MulticlassMetrics(predictionLabel.rdd());



        //predictionLabel = tainingAndTest[1].map(point -> {

          //  double predicted = model.predict(point.features());
           // if(predicted!=point.label()) {
             //   point
            //}
            //return new Tuple2(predicted,point.label());
        //});

        predictionLabel.cache();


        MulticlassMetrics  metricTest = new MulticlassMetrics(predictionLabel.rdd());
        metricTrain.precision(); //compute
        metricTest.precision(); //compute

        System.out.println("\n\n--> Training");
        System.out.println("Precision:"+metricTrain.precision());
        System.out.println("Recall:"+metricTrain.recall());
        System.out.println("F:"+metricTrain.fMeasure());
        System.out.println("\n\n--> Test");
        System.out.println("Precision:"+metricTest.precision());
        System.out.println("Recall:"+metricTest.recall());
        System.out.println("F:"+metricTest.fMeasure());




    }



    @Test
    public void trainModel2() throws URISyntaxException, Exception {

        new TrainModel().train();

    }

    @Test
    public void download() throws URISyntaxException, Exception {

        TrainingDataHandler handler = new TrainingDataHandler();
        handler.downloadNewDataSet();
    }


    @Test
    public void trainModel3() throws URISyntaxException, Exception {

        new TrainModelPipeline().train();

    }





    private JavaRDD<List<String>> textTokens(List<ArticleTrainingSet.Article> articles){

        final Pattern punctuationPattern = Pattern.compile("[^\\p{Ll}\\p{Lu}\\p{Lt}\\p{Lo}\\p{Nd}\\p{Pc}\\s]");

        List<String> articlesUrls = new LinkedList<>();
        articles.forEach(article -> articlesUrls.add(article.getText()));
        JavaRDD<String> texts = sc.parallelize(articlesUrls);

        JavaRDD<List<String>> articlesWordsIn = texts.map(article ->  {

                    List<String> allWords = Arrays.asList(article.split(" "));
                    List<String> words = new LinkedList<>();
                    for(String w: allWords){
                        String word = punctuationPattern.matcher(w).replaceAll("");//.toLowerCase();
                        if(!StopWords.stopWords("en").contains(word.toLowerCase()) && word.length()>2){
                            words.add(word);
                        }
                    }
                    return  words;
                }

        );
        return  articlesWordsIn;

    }

    private JavaRDD<List<String>> urlTokens(List<ArticleTrainingSet.Article> articles){

        final Pattern punctuationPattern = Pattern.compile("[^\\p{Ll}\\p{Lu}\\p{Lt}\\p{Lo}\\p{Pc}\\s]");

        List<String> articlesUrls = new LinkedList<>();
        articles.forEach(article -> articlesUrls.add(article.getUrl()));
        JavaRDD<String> urls = sc.parallelize(articlesUrls);

        JavaRDD<List<String>> articlesWordsIn = urls.map(url ->  {

                    URI uri = new URI(url);
                    List<String> words = new LinkedList<>();
                    for(String w: uri.getPath().split("[/\\p{P}]")){
                        String word = punctuationPattern.matcher(w).replaceAll("").toLowerCase();
                        if(word.length()==0)continue;
                        words.add("/"+word);
                    }
                    return  words;
                }

        );
        return  articlesWordsIn;

    }

*/





}
