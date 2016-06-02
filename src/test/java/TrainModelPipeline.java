import ch.newsriver.data.content.Article;
import ch.newsriver.ml.classifier.news.category.ArticleTrainingSet;
import ch.newsriver.ml.classifier.news.category.TrainingDataHandler;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by eliapalme on 13/05/16.
 */
public class TrainModelPipeline  implements Serializable{

    private static final ObjectMapper mapper = new ObjectMapper();
    private static JavaSparkContext jsc = null;
    private static SQLContext sqlContext = null;

    public TrainModelPipeline(){
        SparkConf conf = new SparkConf().setAppName("classifier").setMaster("local").set("spark.cores.max", "10");
        jsc = new JavaSparkContext(conf);
        sqlContext = new org.apache.spark.sql.SQLContext(jsc);
    }

    public void train(){



        int numClasses = 0;
        List<Row> rows = new ArrayList<>();
        try {
            TrainingDataHandler handler = new TrainingDataHandler();

            Map<Integer,Map<Integer,ArticleTrainingSet>> trainingSet = handler.loadData();
            Map<Integer,ArticleTrainingSet> lang = trainingSet.get(3);

                for(ArticleTrainingSet set : lang.values()){
                    numClasses++;
                    for(ArticleTrainingSet.Article a : set.getArticles()){
                        rows.add( RowFactory.create(set.getLanguage() + "_" + set.getCategory(), a.getUrl(), a.getTitle(), a.getText(),  a.getUrl() + " " +  a.getTitle() + " " + a.getText()));
                    }
                }


        }catch (Exception e){}


        JavaRDD<Row> jrdd = jsc.parallelize(rows);


        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.StringType, false, Metadata.empty()),
                new StructField("url", DataTypes.StringType, false, Metadata.empty()),
                new StructField("title", DataTypes.StringType, false, Metadata.empty()),
                new StructField("body", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });

        DataFrame data = sqlContext.createDataFrame(jrdd, schema);



        //Tokenizer tokenizerURL = new Tokenizer().setInputCol("url").setOutputCol("url_tokens");
        //Tokenizer tokenizerTitlte = new Tokenizer().setInputCol("title").setOutputCol("title_tokens");
        //Tokenizer tokenizerBody = new Tokenizer().setInputCol("body").setOutputCol("body_tokens");
        Tokenizer tokenizerText = new Tokenizer().setInputCol("text").setOutputCol("text_tokens");



        //data = tokenizerText.transform(tokenizerBody.transform(tokenizerTitlte.transform(tokenizerURL.transform(data))));

        data = tokenizerText.transform(data);



        Integer numFeatures = 20000;
        //HashingTF hashingTFURL = new HashingTF().setInputCol("url_tokens").setOutputCol("url_features").setNumFeatures(numFeatures);
        //HashingTF hashingTFTitle = new HashingTF().setInputCol("title_tokens").setOutputCol("title_features").setNumFeatures(numFeatures);
        //HashingTF hashingTFBody = new HashingTF().setInputCol("body_tokens").setOutputCol("body_features").setNumFeatures(numFeatures);
        HashingTF hashingTFText = new HashingTF().setInputCol("text_tokens").setOutputCol("text_features").setNumFeatures(numFeatures);

        data= hashingTFText.transform(data);

        //data= hashingTFURL.transform(data);

        //VectorAssembler assembler = new VectorAssembler()
        //        .setInputCols(new String[]{"url_features", "title_features", "body_features"})
        //        .setOutputCol("raw_features");

        //VectorAssembler assembler = new VectorAssembler()
        //        .setInputCols(new String[]{"url_features"})
        //        .setOutputCol("raw_features");

        //data = assembler.transform(data);


        IDF idf = new IDF().setInputCol("text_features").setOutputCol("features");
        IDFModel idfModel = idf.fit(data);
        data = idfModel.transform(data);

        DataFrame[] splits = data.randomSplit(new double[] {0.7, 0.3});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);


        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        /*GBTClassifier gbt = new GBTClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxIter(1);*/


        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        dt.setMaxMemoryInMB(2048);

        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());



        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {labelIndexer, featureIndexer,dt,labelConverter});


        PipelineModel model = pipeline.fit(trainingData);


        DataFrame predictionsTrain = model.transform(trainingData);
        DataFrame predictionsTest = model.transform(testData);


        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("precision");


        double accuracyTrain = evaluator.evaluate(predictionsTrain);
        double accuracyTest = evaluator.evaluate(predictionsTest);




        System.out.println("Precision Train = " + accuracyTrain);
        System.out.println("Precision Test = " + accuracyTest);




    }


}
