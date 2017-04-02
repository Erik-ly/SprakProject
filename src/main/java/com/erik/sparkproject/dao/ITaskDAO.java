package com.erik.sparkproject.dao;

import com.erik.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * @author Erik
 *
 */
public interface ITaskDAO {
	
	/**
	 * 根据主键查询业务
	 */
	
	Task findById(long taskid);
	
}
