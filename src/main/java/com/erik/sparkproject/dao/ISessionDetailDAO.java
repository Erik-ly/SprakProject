package com.erik.sparkproject.dao;

import com.erik.sparkproject.domain.SessionDetail;

/**
 * session明细接口
 * @author Erik
 *
 */
public interface ISessionDetailDAO {
	
	/**
	 * 插入一条session明细数据
	 * @param sessionDetail
	 */
	void insert(SessionDetail sessionDetail);

}
