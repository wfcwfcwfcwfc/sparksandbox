package com.feng.sparkproject.test;

import java.sql.*;

/**
 * JDBC增删改查示范类
 * <p>
 * JDBC只是java程序操作数据库中最原始和最基础的一种方式
 * <p>
 * 实际上，在项目中，我们一般不会直接使用JDBC；而是会使用J2EE的一些开源框架，比如MyBatis，也可以是Hibernate
 * 而且为了方便框架的整合使用，我们通常都会在spark作业中，使用Spring开源框架，进行各种技术的整合
 * 比如Kafka、Redis、ZooKeeper、Thrift
 * <p>
 * MyBatis/Hibernate这种操作数据库的框架，其实底层也是基于JDBC进行封装的，只不过提供了更加方便快捷的使用
 * 大大提升了我们的开发效率
 * <p>
 * 但是在本套系类课程中，是不会使用这些开源框架来操作数据库的，而只会使用JDBC
 * 因为直接用JDBC最简单，也最基础，不需要其他技术的基础
 * 主要同学们有基本的java编程基础、最最基础的hadoop基础知识，以及扎实的Spark技术功底
 * 我们就要保证同学们就可以学习这套高端、负责的Spark大型大数据项目实战课程
 *
 * @author Administrator
 */
@SuppressWarnings("unused")
public class JdbcCRUD {

  public static void main(String[] args) {
    preparedStatement();
  }

  /**
   * 测试插入数据
   */
  private static void insert() {

    Connection conn = null;

    Statement stmt = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark_project",
          "root",
          "root");

      stmt = conn.createStatement();

      String sql = "insert into test_user(name,age) values('李四',26)";
      int rtn = stmt.executeUpdate(sql);

      System.out.println("SQL语句影响了【" + rtn + "】行。");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

  /**
   * 测试更新数据
   */
  private static void update() {
    Connection conn = null;
    Statement stmt = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark_project",
          "root",
          "root");
      stmt = conn.createStatement();

      String sql = "update test_user set age=27 where name='李四'";
      int rtn = stmt.executeUpdate(sql);

      System.out.println("SQL语句影响了【" + rtn + "】行。");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

  /**
   * 测试删除数据
   */
  private static void delete() {
    Connection conn = null;
    Statement stmt = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark_project",
          "root",
          "root");
      stmt = conn.createStatement();

      String sql = "delete from test_user where name='李四'";
      int rtn = stmt.executeUpdate(sql);

      System.out.println("SQL语句影响了【" + rtn + "】行。");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

  /**
   * 测试查询数据
   */
  private static void select() {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark_project",
          "root",
          "root");
      stmt = conn.createStatement();

      String sql = "select * from test_user";
      rs = stmt.executeQuery(sql);

      while (rs.next()) {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        int age = rs.getInt(3);
        System.out.println("id=" + id + ", name=" + name + ", age=" + age);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

  /**
   * 测试PreparedStatement
   */
  private static void preparedStatement() {
    Connection conn = null;
    /**
     * 如果使用Statement，那么就必须在SQL语句中，实际地去嵌入值，比如之前的insert语句
     *
     * 但是这种方式有一个弊端，第一，是容易发生SQL注入，SQL注入，简单来说，就是，你的网页的用户
     * 在使用，比如论坛的留言板，电商网站的评论页面，提交内容的时候，可以使用'1 or 1'，诸如此类的
     * 非法的字符，然后你的后台，如果在插入评论数据到表中的时候，如果使用Statement，就会原封不动的
     * 将用户填写的内容拼接在SQL中，此时可能会发生对数据库的意外的损坏，甚至数据泄露，这种情况就叫做
     * SQL注入
     *
     * 第二种弊端，就是性能的低下，比如insert into test_user(name,age) values('张三',25)
     * insert into test_user(name,age) values('李四',26)
     * 其实两条SQL语句的结构大同小异，但是如果使用这种方式，在MySQL中执行SQL语句的时候，却需要对
     * 每一条SQL语句都实现编译，编译的耗时在整个SQL语句的执行耗时中占据了大部分的比例
     * 所以，Statement会导致执行大量类似SQL语句的时候的，性能低下
     *
     * 如果使用PreparedStatement，那么就可以解决上述的两个问题
     * 1、SQL注入，使用PreparedStatement时，是可以在SQL语句中，对值所在的位置使用?这种占位符的
     * 使用占位符之后，实际的值，可以通过另外一份放在数组中的参数来代表。此时PreparedStatement会对
     * 值做特殊的处理，往往特殊处理后，就会导致不法分子的恶意注入的SQL代码失效
     * 2、提升性能，使用PreparedStatement之后，其实结构类似的SQL语句，都变成一样的了，因为值的地方
     * 都会变成?，那么一条SQL语句，在MySQL中只会编译一次，后面的SQL语句过来，就直接拿编译后的执行计划
     * 加上不同的参数直接执行，可以大大提升性能
     */
    PreparedStatement pstmt = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf8",
          "root",
          "root");

      String sql = "insert into test_user(name,age) values(?,?)";

      pstmt = conn.prepareStatement(sql);

      pstmt.setString(1, "李四");
      pstmt.setInt(2, 26);

      int rtn = pstmt.executeUpdate();

      System.out.println("SQL语句影响了【" + rtn + "】行。");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (pstmt != null) {
          pstmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

}
