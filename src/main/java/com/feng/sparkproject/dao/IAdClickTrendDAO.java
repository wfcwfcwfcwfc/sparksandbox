package com.feng.sparkproject.dao;

import java.util.List;

import com.feng.sparkproject.domain.AdClickTrend;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDAO {

	void updateBatch(List<AdClickTrend> adClickTrends);
	
}
