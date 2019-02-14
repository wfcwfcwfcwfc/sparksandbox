package com.feng.sparkproject.dao.impl;

import com.feng.sparkproject.dao.ITop10SessionDAO;
import com.feng.sparkproject.domain.Top10Session;
import com.feng.sparkproject.jdbc.JDBCHelper;

/**
 * top10活跃session的DAO实现
 *
 * @author Administrator
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

  @Override
  public void insert(Top10Session top10Session) {
    String sql = "insert into top10_session values(?,?,?,?)";

    Object[] params = new Object[]{top10Session.getTaskid(),
        top10Session.getCategoryid(),
        top10Session.getSessionid(),
        top10Session.getClickCount()};

    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeUpdate(sql, params);
  }

}
