package com.erik.sparkproject.dao;

import com.erik.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * @author Erik
 *
 */
public interface ISessionRandomExtractDAO {
	
	/**
	 * 插入session随机抽取
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
	
}
