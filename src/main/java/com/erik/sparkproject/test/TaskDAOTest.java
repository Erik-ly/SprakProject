package com.erik.sparkproject.test;

import com.erik.sparkproject.dao.ITaskDAO;
import com.erik.sparkproject.dao.factory.DAOFactory;
import com.erik.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Erik
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		//Task导入包时导入自己写的，import com.erik.sparkproject.domain.Task;
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());
	}
}
