package com.erik.sparkproject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC增删改查
 * @author Erik
 *
 */
public class JdbcCRUD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//insert();
		//update();
		//delete();
		//select();
		preparedStatement();
	}
	
	/**
	 * 测试插入数据
	 */
	private static void insert() {
		//总结：JDBC最基本的使用过程
		//1.加载驱动类：Class.forName()
		//2.获取数据库连接：DriverManager.getConnection()
		//3.创建SQL语句执行句柄：Connection.createStatement()
		//4.执行SQL语句：Statement.executeUpdate()
		//5.释放数据库连接资源：finally,Connection.close()
		
		//引用JDBC相关的所有接口或者是抽象类的时候，必须是引用java.sql包下的
		//java.sql包下的，才代表了java提供的JDBC接口，只是一套规范
		Connection conn = null;
		//定义SQL语句执行句柄:Statement对象
		//Statement对象其实就是底层基于Connection数据库连接
		Statement stmt = null;
		
		try {
			//第一步，加载数据库驱动
			//使用Class.forName()方式加载数据库的驱动类
			//Class.forName()是Java提供的一种基于反射的方式，直接根据类的全限定名（包+类）
			//从类所在的磁盘文件（.class文件）中加载类对应的内容，并创建对应的class对象
			Class.forName("com.mysql.jdbc.Driver");
			
			//获取数据库的连接
			//使用DriverManager.getConnection()方法获取针对数据库的连接
			//需要给方法传入三个参数，包括url、user、password
			//其中url就是有特定格式的数据库连接串，包括“主协议：子协议“//主机名：端口号//数据库”
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkproject", 
					"root", 
					"erik");
			
			//基于数据库连接的Connection对象，创建SQL语句执行句柄，Statement对象
			//Statement对象 ，就是用于基于底层的Connection代表的数据库连接
			//允许我们通过Java程序，通过Statement对象，向MySQL数据库发送SQL语句
			//从而实现通过发送的SQL语句来执行增删改查等逻辑
			stmt = conn.createStatement();
			
			//基于Statement对象，来执行insert SQL语句插入一条数据
			//Statement.executeUpdate()方法可以用来执行insert、update、delete语句
			//返回类型是int值，也就是SQL语句影响的行数
			//String sql = "insert into test_user(name,age) values('张三',25)";
			String sql = "insert into test_user(name,age) values('李四',26)";

			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("SQL语句影响了【"+rtn+"】行。");
		}catch (Exception e){
			e.printStackTrace();
		} finally {
			 //最后一定要记得在finally代码块中，尽快在执行完SQL语句之后，就释放数据库连接
			try {
				if(stmt != null){
					stmt.close();
				}
				if(conn != null){
					conn.close();
				}
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试更新数据
	 */
	private static void update() {
		Connection conn =null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkproject", 
					"root", 
					"erik");
			stmt = conn.createStatement();
			
			String sql = "update test_user set age=27 where name='李四'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("SQL语句影响了【"+rtn+"】行。");
			
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if(conn != null)
					conn.close();
				
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试删除数据
	 */
	private static void delete() {
		Connection conn =null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkproject", 
					"root", 
					"erik");
			stmt = conn.createStatement();
			
			String sql = "delete from test_user where name='李四'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("SQL语句影响了【"+rtn+"】行。");
			
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if(conn != null)
					conn.close();
				
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试查询数据
	 */
	private static void select() {
		Connection conn = null;
		Statement stmt = null;
		//对于select查询语句，需要定义ResultSet
		//ResultSet代表了select语句查询出来的数据
		//需要通过ResutSet对象，来遍历查询出来的每一条数据，然后对数据进行保存或者处理
		ResultSet rs = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkproject", 
					"root", 
					"erik");
			stmt = conn.createStatement();
			
			String sql = "select * from test_user";
			rs = stmt.executeQuery(sql);
			
			//获取到ResultSet以后，就要对其进行遍历，然后获取查询出来的每一条数据
			while(rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int age = rs.getInt(3);
				System.out.println("id="+id+",name="+name+",age="+age);
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null){
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 使用Statement时，必须在SQL语句中，实际地区嵌入值，容易发生SQL注入
	 * 而且性能低下
	 * 
	 * 使用PreparedStatement，就可以解决上述的两个问题
	 * 1.SQL主任，使用PreparedStatement时，是可以在SQL语句中，对值所在的位置使用？这种占位符，
	 * 实际的值是放在数组中的参数，PreparedStatement会对数值做特殊处理，往往处理后会使恶意注入的SQL代码失效。
	 * 2.提升性能，使用P热怕热的Statement后，结构类似的SQL语句会变成一样的，因为值的地方会变成？，
	 * 一条SQL语句，在MySQL中只会编译一次，后面的SQL语句过来，就直接拿编译后的执行计划加上不同的参数直接执行，
	 * 可以大大提升性能
	 */
	private static void preparedStatement() {
		Connection conn = null;
		
		PreparedStatement pstmt = null;
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/sparkproject?characterEncoding=utf8", 
					"root", 
					"erik");
			
			//第一个，SQL语句中，值所在的地方，都用问号代表
			String sql = "insert into test_user(name,age) values(？,？)";

			pstmt = conn.prepareStatement(sql);
			
			//第二个，必须调用PreparedStatement的setX（）系列方法，对指定的占位符赋值
			pstmt.setString(1, "李四");
			pstmt.setInt(2, 26);
			
			//第三个，执行SQL语句时，直接使用executeUpdate（）即可
			int rtn = pstmt.executeUpdate();
			
			System.out.println("SQL语句影响了【"+rtn+"】行。");
		}catch (Exception e){
			e.printStackTrace();
		} finally {
			try {
				if(pstmt != null){
					pstmt.close();
				}
				if(conn != null){
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
