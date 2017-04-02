package com.erik.sparkproject.impl;

import com.erik.sparkproject.dao.ITop10CategoryDAO;
import com.erik.sparkproject.domain.Top10Category;
import com.erik.sparkproject.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 * @author Erik
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO{

	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[]{
				category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

}
