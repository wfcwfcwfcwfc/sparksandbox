package com.feng.sparkproject.dao.impl;

import com.feng.sparkproject.dao.ISessionRandomExtractDAO;
import com.feng.sparkproject.domain.SessionRandomExtract;
import com.feng.sparkproject.jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 *
 * @author Administrator
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

  /**
   * 插入session随机抽取
   *
   * @param sessionAggrStat
   */
  public void insert(SessionRandomExtract sessionRandomExtract) {
    String sql = "insert into session_random_extract values(?,?,?,?,?)";

    Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
        sessionRandomExtract.getSessionid(),
        sessionRandomExtract.getStartTime(),
        sessionRandomExtract.getSearchKeywords(),
        sessionRandomExtract.getClickCategoryIds()};

    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    jdbcHelper.executeUpdate(sql, params);
  }

}
