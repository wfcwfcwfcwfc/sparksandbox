package com.feng.sparkproject.dao;

import com.feng.sparkproject.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 *
 * @author Administrator
 */
public interface IAdStatDAO {

  void updateBatch(List<AdStat> adStats);

}
