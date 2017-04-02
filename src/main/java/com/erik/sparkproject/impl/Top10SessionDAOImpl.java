package com.erik.sparkproject.impl;

import com.erik.sparkproject.dao.ITop10SessionDAO;
import com.erik.sparkproject.domain.Top10Session;
import com.erik.sparkproject.jdbc.JDBCHelper;

/**
 * top10活跃session的DAO实现
 * @author Erik
 *
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)";
		Object[] params = new Object[]{
				top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
