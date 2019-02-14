package com.feng.sparkproject.dao;

import com.feng.sparkproject.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 *
 * @author Administrator
 */
public interface IAdClickTrendDAO {

  void updateBatch(List<AdClickTrend> adClickTrends);

}
