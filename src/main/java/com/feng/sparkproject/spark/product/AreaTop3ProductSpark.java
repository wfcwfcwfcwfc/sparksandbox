package com.feng.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.feng.sparkproject.conf.ConfigurationManager;
import com.feng.sparkproject.constant.Constants;
import com.feng.sparkproject.dao.IAreaTop3ProductDAO;
import com.feng.sparkproject.dao.ITaskDAO;
import com.feng.sparkproject.dao.factory.DAOFactory;
import com.feng.sparkproject.domain.AreaTop3Product;
import com.feng.sparkproject.domain.Task;
import com.feng.sparkproject.util.ParamUtils;
import com.feng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 各区域top3热门商品统计Spark作业
 *
 * @author Administrator
 */
public class AreaTop3ProductSpark {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("AreaTop3ProductSpark");
    SparkUtils.setMaster(conf);

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

    sqlContext.udf().register("concat_long_string",
        new ConcatLongStringUDF(), DataTypes.StringType);
    sqlContext.udf().register("get_json_object",
        new GetJsonObjectUDF(), DataTypes.StringType);
    sqlContext.udf().register("random_prefix",
        new RandomPrefixUDF(), DataTypes.StringType);
    sqlContext.udf().register("remove_random_prefix",
        new RemoveRandomPrefixUDF(), DataTypes.StringType);
    sqlContext.udf().register("group_concat_distinct",
        new GroupConcatDistinctUDAF());

    SparkUtils.mockData(sc, sqlContext);

    ITaskDAO taskDAO = DAOFactory.getTaskDAO();

    long taskid = ParamUtils.getTaskIdFromArgs(args,
        Constants.SPARK_LOCAL_TASKID_PRODUCT);
    Task task = taskDAO.findById(taskid);

    JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
    String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
    String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

    JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(
        sqlContext, startDate, endDate);
    System.out.println("cityid2clickActionRDD: " + cityid2clickActionRDD.count());

    JavaPairRDD<Long, Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
    System.out.println("cityid2cityInfoRDD: " + cityid2cityInfoRDD.count());

    generateTempClickProductBasicTable(sqlContext,
        cityid2clickActionRDD, cityid2cityInfoRDD);

    generateTempAreaPrdocutClickCountTable(sqlContext);

    generateTempAreaFullProductClickCountTable(sqlContext);

    JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
    System.out.println("areaTop3ProductRDD: " + areaTop3ProductRDD.count());

    List<Row> rows = areaTop3ProductRDD.collect();
    System.out.println("rows: " + rows.size());
    persistAreaTop3Product(taskid, rows);

    sc.close();
  }

  /**
   * 查询指定日期范围内的点击行为数据
   *
   * @param sqlContext
   * @param startDate  起始日期
   * @param endDate    截止日期
   * @return 点击行为数据
   */
  private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(
      SQLContext sqlContext, String startDate, String endDate) {

    String sql =
        "SELECT "
            + "city_id,"
            + "click_product_id product_id "
            + "FROM user_visit_action "
            + "WHERE click_product_id IS NOT NULL "
            + "AND date>='" + startDate + "' "
            + "AND date<='" + endDate + "'";

    DataFrame clickActionDF = sqlContext.sql(sql);

    JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

    JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(

        new PairFunction<Row, Long, Row>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<Long, Row> call(Row row) throws Exception {
            Long cityid = row.getLong(0);
            return new Tuple2<Long, Row>(cityid, row);
          }

        });

    return cityid2clickActionRDD;
  }

  /**
   * 使用Spark SQL从MySQL中查询城市信息
   *
   * @param sqlContext SQLContext
   * @return
   */
  private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
    String url = null;
    String user = null;
    String password = null;
    boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

    if (local) {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL);
      user = ConfigurationManager.getProperty(Constants.JDBC_USER);
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
    } else {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
    }

    Map<String, String> options = new HashMap<String, String>();
    options.put("url", url);
    options.put("dbtable", "city_info");
    options.put("user", user);
    options.put("password", password);

    DataFrame cityInfoDF = sqlContext.read().format("jdbc")
        .options(options).load();

    JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

    JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(

        new PairFunction<Row, Long, Row>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<Long, Row> call(Row row) throws Exception {
            long cityid = Long.valueOf(String.valueOf(row.get(0)));
            return new Tuple2<Long, Row>(cityid, row);
          }

        });

    return cityid2cityInfoRDD;
  }

  /**
   * 生成点击商品基础信息临时表
   *
   * @param sqlContext
   * @param cityid2clickActionRDD
   * @param cityid2cityInfoRDD
   */
  private static void generateTempClickProductBasicTable(
      SQLContext sqlContext,
      JavaPairRDD<Long, Row> cityid2clickActionRDD,
      JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
    JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
        cityid2clickActionRDD.join(cityid2cityInfoRDD);

    JavaRDD<Row> mappedRDD = joinedRDD.map(

        new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple)
              throws Exception {
            long cityid = tuple._1;
            Row clickAction = tuple._2._1;
            Row cityInfo = tuple._2._2;

            long productid = clickAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String area = cityInfo.getString(2);

            return RowFactory.create(cityid, cityName, area, productid);
          }

        });

    List<StructField> structFields = new ArrayList<StructField>();
    structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
    structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));


    StructType schema = DataTypes.createStructType(structFields);

    DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
    System.out.println("tmp_click_product_basic: " + df.count());

    df.registerTempTable("tmp_click_product_basic");
  }

  /**
   * 生成各区域各商品点击次数临时表
   *
   * @param sqlContext
   */
  private static void generateTempAreaPrdocutClickCountTable(
      SQLContext sqlContext) {
    String sql =
        "SELECT "
            + "area,"
            + "product_id,"
            + "count(*) click_count, "
            + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
            + "FROM tmp_click_product_basic "
            + "GROUP BY area,product_id ";

    /**
     * 双重group by
     */


    DataFrame df = sqlContext.sql(sql);

    System.out.println("tmp_area_product_click_count: " + df.count());

    df.registerTempTable("tmp_area_product_click_count");
  }

  /**
   * 生成区域商品点击次数临时表（包含了商品的完整信息）
   *
   * @param sqlContext
   */
  private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {


    String sql =
        "SELECT "
            + "tapcc.area,"
            + "tapcc.product_id,"
            + "tapcc.click_count,"
            + "tapcc.city_infos,"
            + "pi.product_name,"
            + "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status "
            + "FROM tmp_area_product_click_count tapcc "
            + "JOIN product_info pi ON tapcc.product_id=pi.product_id ";


    DataFrame df = sqlContext.sql(sql);

    System.out.println("tmp_area_fullprod_click_count: " + df.count());

    df.registerTempTable("tmp_area_fullprod_click_count");
  }

  /**
   * 获取各区域top3热门商品
   *
   * @param sqlContext
   * @return
   */
  private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {


    String sql =
        "SELECT "
            + "area,"
            + "CASE "
            + "WHEN area='China North' OR area='China East' THEN 'A Level' "
            + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
            + "WHEN area='West North' OR area='West South' THEN 'C Level' "
            + "ELSE 'D Level' "
            + "END area_level,"
            + "product_id,"
            + "click_count,"
            + "city_infos,"
            + "product_name,"
            + "product_status "
            + "FROM ("
            + "SELECT "
            + "area,"
            + "product_id,"
            + "click_count,"
            + "city_infos,"
            + "product_name,"
            + "product_status,"
            + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
            + "FROM tmp_area_fullprod_click_count "
            + ") t "
            + "WHERE rank<=3";

    DataFrame df = sqlContext.sql(sql);

    return df.javaRDD();
  }

  /**
   * 将计算出来的各区域top3热门商品写入MySQL中
   *
   * @param rows
   */
  private static void persistAreaTop3Product(long taskid, List<Row> rows) {
    List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

    for (Row row : rows) {
      AreaTop3Product areaTop3Product = new AreaTop3Product();
      areaTop3Product.setTaskid(taskid);
      areaTop3Product.setArea(row.getString(0));
      areaTop3Product.setAreaLevel(row.getString(1));
      areaTop3Product.setProductid(row.getLong(2));
      areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
      areaTop3Product.setCityInfos(row.getString(4));
      areaTop3Product.setProductName(row.getString(5));
      areaTop3Product.setProductStatus(row.getString(6));
      areaTop3Products.add(areaTop3Product);
    }

    IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
    areTop3ProductDAO.insertBatch(areaTop3Products);
  }

}
