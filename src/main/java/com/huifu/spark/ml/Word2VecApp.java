package com.huifu.spark.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Word2VecApp {
  public static void main(String[] args) {
    // create spark context
    SparkConf scf = new SparkConf().setMaster("local").setAppName("Word2VecApp");
    JavaSparkContext jsc = new JavaSparkContext(scf);
    SQLContext sqlContext = new SQLContext(jsc);
    // Connect to Hbase table BD_PAGE_REPOSITORY
    Configuration conf = HBaseConfiguration.create();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("URL"), Bytes.toBytes("filteredPlainText"));
    scan.setCaching(100);
    scan.setMaxResultSize(2000);
    JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd =
        hbaseContext.hbaseRDD(TableName.valueOf("BD_PAGE_REPOSITORY"), scan);
    JavaRDD<Row> jrdd = javaRdd.map(new ScanConvertFunction());
    // Input data: Each row is a bag of words from a sentence or document.
    // JavaRDD<Row> jrdd = jsc.parallelize(
    // Arrays.asList(RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
    // RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
    // RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))));
    //
    StructType schema = new StructType(new StructField[] {new StructField("text",
        new ArrayType(DataTypes.StringType, true), false, Metadata.empty())});

    DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);
    // Learn a mapping from words to Vectors.
    Word2Vec word2Vec =
        new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0);

    Word2VecModel model = word2Vec.fit(documentDF);
    DataFrame result = model.transform(documentDF);
    for (Row row : result.collectAsList()) {
      List<String> text = row.getList(0);
      Vector vector = (Vector) row.get(1);
      System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
    }
  }

  private static class ScanConvertFunction
      implements Function<Tuple2<ImmutableBytesWritable, Result>, Row> {
    @Override
    public Row call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
      String page = Bytes.toString(tuple._2().value());
      return null;
    }

  }

}
