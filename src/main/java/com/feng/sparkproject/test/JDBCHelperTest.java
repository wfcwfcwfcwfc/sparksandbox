package com.feng.sparkproject.test;

import com.feng.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * JDBC辅助组件测试类
 *
 * @author Administrator
 */
@SuppressWarnings("unused")
public class JDBCHelperTest {

  public static void main(String[] args) throws Exception {
    JDBCHelper jdbcHelper = JDBCHelper.getInstance();


    String sql = "insert into test_user(name,age) values(?,?)";

    List<Object[]> paramsList = new ArrayList<Object[]>();
    paramsList.add(new Object[]{"麻子", 30});
    paramsList.add(new Object[]{"王五", 35});

    jdbcHelper.executeBatch(sql, paramsList);
  }

}
