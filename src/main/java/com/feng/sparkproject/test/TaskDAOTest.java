package com.feng.sparkproject.test;

import com.feng.sparkproject.domain.Task;
import com.feng.sparkproject.dao.ITaskDAO;
import com.feng.sparkproject.dao.factory.DAOFactory;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());  
	}
	
}
