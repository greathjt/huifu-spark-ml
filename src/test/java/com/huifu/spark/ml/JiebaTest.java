package com.huifu.spark.ml;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.huaban.analysis.jieba.JiebaSegmenter;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class JiebaTest extends TestCase {
  JiebaSegmenter segmenter = new JiebaSegmenter();
  // 
  // String regEx = "[’（）〈〉：．!#$%&\'()*+,-./:;～<=>?@，。?↓★、�…【】《》？©“”▪►‘’！•[\\]^_`{|}~]+|^(a-zA-Z0-9)]";
  String regEx = "[^\u4e00-\u9fa5]+";

  Pattern p = Pattern.compile(regEx);

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public JiebaTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(JiebaTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp() {
    String[] sentences = new String[] {"这是一个伸手不见五指的黑12345夜。我叫孙悟空，我爱北京，我爱Python和C++。", "我不喜欢日本和服。",
        "雷猴回归人间。", "工信处女干事每月经过下属科室=>?@，。?↓★都要亲口交代24口交换机等技术性器件的安装工作", "结果婚的和尚未结过婚的"};
    for (String sentence : sentences) {
      Matcher m = p.matcher(sentence);
      System.out.println(segmenter.sentenceProcess(m.replaceAll("").trim()).toString());
    }
  }

  public void testRegEx1() {
    String regEx = "[^\u4e00-\u9fa5]+";
    Pattern p = Pattern.compile(regEx);
    String page = "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。";
    Matcher m = p.matcher(page);
    System.out.println(m.replaceAll(""));
  }

  public void testRegEx2() {
    String regEx = "[’（）〈〉：．!#$%&\'()*+,-./:;～<=>?@，。?↓★、�…【】《》？©“”▪►‘’！•[\\]^_`{|}~]+|^(a-zA-Z0-9)]";
    Pattern p = Pattern.compile(regEx);
    String page = "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。";
    Matcher m = p.matcher(page);
    System.out.println(m.replaceAll(""));
  }

  public void testRegEx3() {
    String regEx = "[`~!@#$%^&*()+=|{}':;',//[//].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？^(a-zA-Z0-9)]";
    Pattern p = Pattern.compile(regEx);
    String page = "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。";
    Matcher m = p.matcher(page);
    System.out.println(m.replaceAll(""));
  }
}
