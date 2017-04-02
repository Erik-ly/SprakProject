package com.erik.sparkproject.dao.factory;

import com.erik.sparkproject.dao.ISessionAggrStatDAO;
import com.erik.sparkproject.dao.ISessionDetailDAO;
import com.erik.sparkproject.dao.ISessionRandomExtractDAO;
import com.erik.sparkproject.dao.ITaskDAO;
import com.erik.sparkproject.dao.ITop10CategoryDAO;
import com.erik.sparkproject.dao.ITop10SessionDAO;
import com.erik.sparkproject.impl.SessinoRandomExtractDAOImpl;
import com.erik.sparkproject.impl.SessionAggrStatDAOImpl;
import com.erik.sparkproject.impl.SessionDetailDAOImpl;
import com.erik.sparkproject.impl.TaskDAOImpl;
import com.erik.sparkproject.impl.Top10CategoryDAOImpl;
import com.erik.sparkproject.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Erik
 *
 */
public class DAOFactory {
	/**
	 * 获取任务管理DAO
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
		
	}
	
	/**
	 * 获取session聚合统计DAO
	 * @return
	 */
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessinoRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	

}
