import ch.newsriver.dao.JDBCPoolUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intenthq.gander.text.StopWords;

import scala.Tuple2;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by eliapalme on 13/05/16.
 */
public class TrainModel implements Serializable{
/*
    private static final ObjectMapper mapper = new ObjectMapper();
    private static JavaSparkContext sc = null;


    public TrainModel(){
        SparkConf conf = new SparkConf().setAppName("classifier").setMaster("local").set("spark.cores.max", "10");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryoserializer.buffer","24m");

        sc = new JavaSparkContext(conf);
    }




    public void train(){

        HashingTF tf = new HashingTF(10);
        IDF idf = new IDF();



        JavaRDD<ArticleTrainingData> articles = null;
        //try {
        //    articles = sc.objectFile("/Users/eliapalme/articleTraninga.rdd");
        //}catch (Exception e){
            articles = sc.emptyRDD();
        //}

        JavaRDD<ArticleTrainingData> freshdata = sc.parallelize(downloadTreiningSet());
        freshdata.cache();

        //Text features extraction
        freshdata = freshdata.map(article ->{

            JavaRDD<String> articlesKeywords = urlTokens(article.getUrl());
            articlesKeywords.union( textTokens(article.getText()));
            articlesKeywords.union( textTokens(article.getTitle()));

            Vector vector = tf.transform(articlesKeywords.collect());
            //article.setFeatures(vector);

            return article;
        });

        //articles = articles.union(freshdata);
        articles=freshdata;


        //remove articles with duplicate
        //articles = articles.mapToPair(e -> new Tuple2<String, ArticleTrainingData>(e.getUrl(), e))
        //                   .reduceByKey((value1, value2) -> (value1)).values();


        //cache the union of the old data with the new data
        articles = articles.cache();

        //update the corpus with all extracted keywords of the whole data
        //JavaRDD<Vector> corpus = articles.map(article -> article.getFeatures());
        //corpus.saveAsTextFile("/Users/eliapalme/Newsriver/ML/CategoryClassification/data/test.rdd");

       // IDFModel idfModel = idf.fit(corpus);

        // applaying idf to features
       // articles = articles.map(article ->{
      //      article.setFeatures(idfModel.transform(article.getFeatures()));
       //     return article;
       // });



        //save the new traning and test data.
        //articles.saveAsObjectFile("/Users/eliapalme/Newsriver/ML/CategoryClassification/data/articles");

        //articles.saveAsTextFile("/Users/eliapalme/Newsriver/ML/CategoryClassification/data/articles.rdd");

        //JavaRDD<LabeledPoint> labeledSet = articles.map(article -> new LabeledPoint(1d, article.getFeatures()));
        //JavaRDD<LabeledPoint>[] labeledSets = labeledSet.randomSplit(new double[]{0.4,0.6});
       // NaiveBayesModel model = NaiveBayes.train(labeledSets[0].rdd());


        //model.save(sc.sc(), "/Users/eliapalme/Newsriver/ML/CategoryClassification/classifier.model");

        //JavaRDD<Tuple2<Object,Object>> predictions = labeledSets[1].map(point -> {
        //    return new Tuple2(model.predict(point.features()),point.label());
        //});


        //MulticlassMetrics metric = new MulticlassMetrics(predictions.rdd());

        //System.out.println("Precision:"+metric.precision());
        //System.out.println("Recall:"+metric.recall());
        //System.out.println("F:"+metric.fMeasure());



    }


    private JavaRDD<String> textTokens(String text){

        final Pattern punctuationPattern = Pattern.compile("[^\\p{Ll}\\p{Lu}\\p{Lt}\\p{Lo}\\p{Nd}\\p{Pc}\\s]");



        List<String> allWords = Arrays.asList(text.split(" "));
        List<String> words = new LinkedList<>();
        for(String w: allWords){
            String word = punctuationPattern.matcher(w).replaceAll("");//.toLowerCase();
            if(!StopWords.stopWords("en").contains(word.toLowerCase()) && word.length()>2){
                words.add(word);
            }
        }
        return  sc.parallelize(words);

    }

    private JavaRDD<String> urlTokens(String url) {

        final Pattern punctuationPattern = Pattern.compile("[^\\p{Ll}\\p{Lu}\\p{Lt}\\p{Lo}\\p{Nd}\\p{Pc}\\s]");

        List<String> words = new LinkedList<>();

        try {
            URI uri = new URI(url);

            for (String w : uri.getPath().split("[/-]")) {
                String word = punctuationPattern.matcher(w).replaceAll("").toLowerCase();

                words.add(word);
            }
        }catch (URISyntaxException e){}
        return  sc.parallelize(words);


    }

    private List<ArticleTrainingData> downloadTreiningSet(){

        List<ArticleTrainingData> data = new ArrayList<>();


        String sql = "Select title,snippet,URL,categoryID,languageID from NewscronContent.article where status=5 order by id desc limit 10 ";
        try (Connection conn = JDBCPoolUtil.getInstance().getConnection(JDBCPoolUtil.DATABASES.Sources); PreparedStatement stmt = conn.prepareStatement(sql);) {
            int cnt=0;
            try (ResultSet rs = stmt.executeQuery();) {
                while (rs.next()) {
                    ArticleTrainingData a = new ArticleTrainingData();
                    a.setText(rs.getString("snippet"));
                    a.setTitle(rs.getString("title"));
                    a.setUrl(rs.getString("URL"));
                    //a.setCategory(rs.getInt("categoryID"));
                    //a.setLanguage(rs.getInt("languageID"));
                    data.add(a);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        return  data;
    }
*/
}
