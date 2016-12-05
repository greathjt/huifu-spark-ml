package com.huifu.spark.ml;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.huaban.analysis.jieba.JiebaSegmenter;

import scala.Tuple2;

public class Word2VecApp {
  public static void main(String[] args) {
    // create spark context
    System.setProperty("hadoop.home.dir", "c:\\\\winutil\\\\");
    SparkConf scf = new SparkConf().setMaster("local").setAppName("Word2VecApp");
    JavaSparkContext jsc = new JavaSparkContext(scf);
    SQLContext sqlContext = new SQLContext(jsc);
    // Connect to Hbase table BD_PAGE_REPOSITORY
    String coreSiteXml = "classpath:core-site.xml";
    String hbaseSiteXml = "classpath:hbase-site.xml";
    String hdfsSiteXml = "classpath:hdfs-site.xml";
    String yarnSiteXml = "classpath:ssl-client.xml";
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(coreSiteXml);
    conf.addResource(hbaseSiteXml);
    conf.addResource(hdfsSiteXml);
    conf.addResource(yarnSiteXml);

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("URL"), Bytes.toBytes("filteredPlainText"));
    scan.setCaching(100);
    scan.setMaxVersions(1);
    // scan.setFilter(new PageFilter(2000));
    JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd =
        hbaseContext.hbaseRDD(TableName.valueOf("BD_PAGE_REPOSITORY"), scan);
    JavaRDD<Row> jrdd = javaRdd.map(new ScanConvertFunction());
    //
    // List<Row> jrdd =
    // Arrays.asList(RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
    // RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
    // RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" "))));


    StructType schema = new StructType(new StructField[] {new StructField("text",
        new ArrayType(DataTypes.StringType, true), false, Metadata.empty())});

    DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);
    // Learn a mapping from words to Vectors.
    Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(400)
        .setMinCount(0).setWindowSize(5);

    Word2VecModel model = word2Vec.fit(documentDF);
    // DataFrame result = model.transform(documentDF);
    // for (Row row : result.collectAsList()) {
    // List<String> text = row.getList(0);
    // Vector vector = (Vector) row.get(1);
    // System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
    // }
    String[] keyWords =
        "无耻,诈骗,骗子,垃圾,跑路,失联,维权,欺骗,造假,无法取现,不让取现,提现困难,坑爹,黑名单,曝光,自融,作死,坑,忽悠,警惕,疑似,小心,泄露,恶意,撤销,忠告,不能提现,危险,提现,谎称,信息披露,嫌疑,虚假,涉嫌,卑鄙,血汗钱,预警,奇葩,澄清,亏空,待收,证据,失信,忽悠,逾期,人去楼空,虚假标的,揭发,电话打不通,不兑现,圈套,破案,案件,还我钱,被抓,风险,取保候审,小心,非法,集资,警察,立案"
            .split(",");
    // String[] keyWords = "Logistic,regression,models,are,neat".split(",");
    for (String keyWord : keyWords) {
      try {
        List<Row> result = model.findSynonyms(keyWord, 25).collectAsList();
        // System.out.println("Text: " + keyWord result.get(0).getString(0) );
        // System.out.println("Text: " + result );
        System.out.println("keyword:" + keyWord + ",synonyms:" + result.toString());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static class ScanConvertFunction
      implements Function<Tuple2<ImmutableBytesWritable, Result>, Row> {
    private static JiebaSegmenter segmenter = new JiebaSegmenter();
    private static String regEx = "[^\u4e00-\u9fa5]+";// 只保留中文
    // private static String regEx =
    // "[’（）〈〉：．!#$%&\'()*+,-./:;～<=>?@，。?↓★、�…【】《》？©“”▪►‘’！•[\\]^_`{|}~]+|^(a-zA-Z0-9)]";
    private static Pattern p = Pattern.compile(regEx);

    @Override
    public Row call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
      // 去掉\r,\n,\t,space
      String page = Bytes.toString(tuple._2().value()).replace("\n", "").replace("\r", "")
          .replace("\t", "").replace(" ", "");
      Matcher m = p.matcher(page);
      // 对page去噪、分词，page2words
      return RowFactory.create(segmenter.sentenceProcess(m.replaceAll("").trim()));
    }
  }

}
