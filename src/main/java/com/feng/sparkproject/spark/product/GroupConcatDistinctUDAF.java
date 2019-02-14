package com.feng.sparkproject.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数（group_concat_distinct()）
 * <p>
 * 技术点4：自定义UDAF聚合函数
 *
 * @author Administrator
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

  private static final long serialVersionUID = -2510776241322950505L;

  private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
      DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
  private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
      DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
  private DataType dataType = DataTypes.StringType;
  private boolean deterministic = true;

  @Override
  public StructType inputSchema() {
    return inputSchema;
  }

  @Override
  public StructType bufferSchema() {
    return bufferSchema;
  }

  @Override
  public DataType dataType() {
    return dataType;
  }

  @Override
  public boolean deterministic() {
    return deterministic;
  }

  /**
   * 初始化
   * 可以认为是，你自己在内部指定一个初始的值
   */
  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, "");
  }

  /**
   * 更新
   * 可以认为是，一个一个地将组内的字段值传递进来
   * 实现拼接的逻辑
   */
  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    String bufferCityInfo = buffer.getString(0);
    String cityInfo = input.getString(0);

    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo)) {
        bufferCityInfo += cityInfo;
      } else {
        bufferCityInfo += "," + cityInfo;
      }

      buffer.update(0, bufferCityInfo);
    }
  }

  /**
   * 合并
   * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
   * 但是可能一个分组内的数据，会分布在多个节点上处理
   * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
   */
  @Override
  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    String bufferCityInfo1 = buffer1.getString(0);
    String bufferCityInfo2 = buffer2.getString(0);

    for (String cityInfo : bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo;
        } else {
          bufferCityInfo1 += "," + cityInfo;
        }
      }
    }

    buffer1.update(0, bufferCityInfo1);
  }

  @Override
  public Object evaluate(Row row) {
    return row.getString(0);
  }

}
