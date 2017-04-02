package com.erik.sparkproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import com.alibaba.fastjson.JSONObject;

import com.erik.sparkproject.conf.ConfigurationManager;
import com.erik.sparkproject.constant.Constants;
import com.erik.sparkproject.dao.ISessionAggrStatDAO;
import com.erik.sparkproject.dao.ISessionDetailDAO;
import com.erik.sparkproject.dao.ISessionRandomExtractDAO;
import com.erik.sparkproject.dao.ITaskDAO;
import com.erik.sparkproject.dao.ITop10CategoryDAO;
import com.erik.sparkproject.dao.ITop10SessionDAO;
import com.erik.sparkproject.dao.factory.DAOFactory;
import com.erik.sparkproject.domain.SessionAggrStat;
import com.erik.sparkproject.domain.SessionDetail;
import com.erik.sparkproject.domain.SessionRandomExtract;
import com.erik.sparkproject.domain.Task;
import com.erik.sparkproject.domain.Top10Category;
import com.erik.sparkproject.domain.Top10Session;
import com.erik.sparkproject.test.MockData;
import com.erik.sparkproject.util.*;
import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 用户访问session分析spark作业
 * 
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1.时间范围：起始日期-结束日期
 * 2.性别：男或女
 * 3.年龄范围
 * 4.职业：多选
 * 5.城市：多选
 * 6.搜索词：多个搜索词，只要某个session中的任何一个
 *   action搜索过指定的关键词，那么session就符合条件
 * 7.点击品类：多个品类，只要某个session中的任何一个
 *   action点击过某个品类，那么session就符合条件
 * 
 * 我们的Spark作业如何接受用户创建的任务呢？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，
 * 任务参数以JSON格式封装在task_param字段中
 * 接着J2EE平台执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数传递给spark作业的main函数
 * 参数就封装在main函数得到args数组中
 * 
 * 这是spark本事提供的特性
 * 
 * 
 * @author Erik
 *
 */
public class UserVisitSessionAnalyzeSpark {

	public static void main(String[] args) {
		args = new String[]{"2"};
		//构建spark上下文
		
		//首先在Constants.java中设置spark作业相关的常量
		//String SPARK_APP_NAME = "UserVisitSessionAnalyzeSpark";
		//保存Constants.java配置
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME)
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		//生成模拟测试数据
		mockData(sc, sqlContext);
		
		//创建需要使用的DAO组件
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		//那么就首先得查询出来指定的任务，并获取任务的查询参数
		long taskid = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		System.out.println(taskid);
		System.out.println(taskParam);
		
		//如果要进行session粒度的数据聚合，
		//首先要从user_visit_action表中，查询出来指定日期范围内的数据
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		
		//聚合
		//首先，可以将行为数据按照session_id进行groupByKey分组
		//此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据惊醒join
		//然后就可以获取到session粒度的数据，同时数据里面还包含了session对应的user信息
		//到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,
		//clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBySession(sqlContext, actionRDD);
		
		
		//接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		//相当于我们自己编写的算子，是要访问外面的任务参数对象的
		//匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
		
		//重构，同时进行过滤和统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SesssionAggrStatAccumulator());
		
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		
		//生成公共RDD：通过筛选条件的session的访问明细数据
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		
		//对于Accumulator这种分布式累加计算的变量的使用，有一个重要的说明
		//从Accumulator中，获取数据，插入数据库的时候，一定要是在某一个action操作以后
		//如果没有action的话，那么整个程序根本不会运行
		//而且必须把能够触发job执行的操作，放在最终写入MySQL方法之前
		//计算出的结果，在J2EE中，是以两张柱状图显示的
		
		/**
		 * 特别说明
		 * 我们知道，要将一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后才能
		 * 从Accmulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
		 * 所以，我们在这里，将随机抽取功能的实现代码放在session聚合统计功能的最终计算和写库之前
		 * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
		 * 
		 */
		randomExtractSession(task.getTaskid(),filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		
		//计算出各个范围的session占比，并写入MySQL
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());
		
		/**
		 * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
		 * 
		 * 如果不进行重构，直接来实现，思路；
		 * 1.sessionRDD，映射成<sessioinid,Row>的格式
		 * 2.按session聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
		 * 3.遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中对应的值
		 * 4.使用自定义Accumulator中的统计值，去计算各个区间的比例
		 * 5.将最后计算出来的结果，写入MySQL对应的表中
		 * 
		 * 普通实现思路的问题：
		 * 1.为什么还要用actionRDD去映射？其实之前在session聚合的时候已经做过映射了，多次一举
		 * 2.是不是一定要为了session的聚合这个功能单独去遍历一遍session？其实没必要，已经有session数据
		 * 	之前过滤session的时候，其实相当于是在遍历session了，那么这里就没必要再过滤一遍了
		 * 
		 * 重构实现思路：
		 * 1.不要去生成任何新的RDD（处理上亿的数据）
		 * 2.不要去单独遍历一遍session（处理上千万的数据）
		 * 3.可以在进行session聚合的时候就直接计算出来每个session的访问时长和访问步长
		 * 4.在进行过滤的时候，本来就要遍历所有的聚合session信息，此时就可以在某个session通过筛选条件后
		 *   将访问时长和访问步长累加到自定义的Acccumulator上面
		 * 5.就是两种截然不同的思考方式和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达半小时
		 *   或者数个小时
		 * 
		 * 开发Spark大型复杂项目的一些经验准则：
		 * 1.尽量少生成RDD
		 * 2.尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里实现多个需要的功能
		 * 3.尽量少对进行RDD进行shuffle算子操作，比如groupByKey,reduceByKey,sortByKey
		 *   shuffle操作，会导致大量的磁盘读写，严重降低性能
		 *   有shuffle的算子和没有shuffle的算子，性能甚至会有长达几十分钟甚至数个小时的差别
		 *   有shuffle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（后续会有完整解决方案）
		 * 4.无论做什么功能，性能第一
		 *   在传统的J2EE或者.NET或者PHP，软件/系统/网站开发中，架构和可维护性，可扩展性的重要程度
		 *   远远高于性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
		 *   
		 *   在大数据项目中，比如MapReduce，Hive，Spark,Storm中，性能的重要程度远远大于一次额代码的规范
		 *   和设计模式，代码的划分，类的划分；大数据最重要的是性能。
		 *   主要就是因为大数据以及大数据项目的特点决定了大数据的程度和项目的速度都比较慢
		 *   如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长达数小时，甚至数十个小时
		 *   
		 *   所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能，其次考虑功能代码的划分、解耦合
		 *   
		 *   我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
		 *   如果采用第二种方案，那么其实就是性能优先
		 */
		
		//获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				getTop10Category(task.getTaskid(), sessionid2detailRDD);
		
		//获取top10活跃session
		getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionid2detailRDD);
		
		//关闭spark上下文
		sc.close();

	}
	

	/**
	 * 获取SQLContext
	 * 如果在本地测试环境的话，那么久生成SQLContext对象
	 *如果在生产环境运行的话，那么就生成HiveContext对象
	 * @param sc SparkContext
	 * @return SQLContext
	 */
	private static SQLContext getSQLContext(SparkContext sc) {
		//在my.properties中配置
		//spark.local=true（打包之前改为flase）
		//在ConfigurationManager.java中添加
		//public static Boolean getBoolean(String key) {
		//	String value = getProperty(key);
		//	try {
		//		return Boolean.valueOf(value);
		//	} catch (Exception e) {
		//		e.printStackTrace();
		//	}
		//	return false;	
		//}
		//在Contants.java中添加
		//String SPARK_LOCAL = "spark.local";
		
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		}else {
			return new HiveContext(sc);
		}	
	}
	
	/**
	 * 生成模拟数据
	 * 只有是本地模式，才会生成模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext SQLContext
	 * @param taskParam 任务参数
	 * @return 行为数据RDD
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam) {
		
		//先在Constants.java中添加任务相关的常量
		//String PARAM_START_DATE = "startDate";
		//String PARAM_END_DATE = "endDate";
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		String sql = "select * "
				+ "from user_visit_action"
				+ "where date>='" + startDate + "'"
				+ "and date<='" + endDate + "'";
		
		DataFrame actionDF = sqlContext.sql(sql);
		
		return actionDF.javaRDD();
	}
	
	/**
	 * 获取sessionid2到访问行为数据的映射的RDD
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>(){

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}
			
		});
		
	}
	
	
	/**
	 * 对行为数据按sesssion粒度进行聚合
	 * @param actionRDD 行为数据RDD
	 * @return session粒度聚合数据
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			SQLContext sqlContext, JavaRDD<Row> actionRDD) {
		//现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
		//现在需要将这个Row映射成<sessionid,Row>的格式
		JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
				
				/**
				 * PairFunction
				 * 第一个参数，相当于是函数的输入
				 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
				 */
				new PairFunction<Row, String, Row>() {
					
					private static final long serialVersionUID = 1L;
					
					public Tuple2<String, Row> call(Row row) throws Exception {
						
						//按照MockData.java中字段顺序获取
						//此时需要拿到session_id，序号是2
						return new Tuple2<String, Row>(row.getString(2), row);
					}
			
				});
		
		//对行为数据按照session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = 
				sessionid2ActionRDD.groupByKey();
		
		//对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		//到此为止，获取的数据格式如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
				new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
				
					private static final long serialVersionUID = 1L;
				
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
							throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						Long userid = null;
						
						//session的起始和结束时间
						Date startTime = null;
						Date endTime = null;
						//session的访问步长
						int stepLength = 0;
						
						//遍历session所有的访问行为
						while(iterator.hasNext()) {
							//提取每个 访问行为的搜索词字段和点击品类字段
							Row row = iterator.next();
							if(userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							//实际上这里要对数据说明一下
							//并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段的
							//其实，只有搜索行为是有searchKeyword字段的
							//只有点击品类的行为是有clickCaregoryId字段的
							//所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
							
							//所以是否将搜索词点击品类id拼接到字符串中去
							//首先要满足不能是null值
							//其次，之前的字符串中还没有搜索词或者点击品类id
							
							if(StringUtils.isNotEmpty(searchKeyword)) {
								if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							if(clickCategoryId != null) {
								if(!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							
							//计算session开始和结束时间
							Date actionTime = DateUtils.parseTime(row.getString(4));
							if(startTime == null) {
								startTime = actionTime;
							}
							if(endTime == null) {
								endTime = actionTime;
							}
							
							if(actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if(actionTime.after(endTime)) {
								endTime = actionTime;
							}
							
							//计算session访问步长
							stepLength ++;
						}
						
						//计算session开始和结束时间
						//现在DateUtils.java中添加方法
						//public static Date parseTime(String time) {
						//	try {
						//		return TIME_FORMAT.parse(time);
						//	} catch (ParseException e) {
						//		e.printStackTrace();
						//	}
						//	return null;
						//}
						
					
						
						//StringUtils引入的包是import com.erik.sparkproject.util.trimComma;
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						//计算session访问时长（秒）
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						
						//返回的数据即是<sessionid, partAggrInfo>
						//但是，这一步聚合后，其实还需要将每一行数据，根对应的用户信息进行聚合
						//问题来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid，而应该是userid
						//才能够跟<userid, Row>格式的用户信息进行聚合
						//如果我们这里直接返回<sessionid, partAggrInfo>,还得再做一次mapToPair算子
						//将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
						
						//所以，我们这里其实可以直接返回数据格式就是<userid,partAggrInfo>
						//然后在直接将返回的Tuple的key设置成sessionid
						//最后的数据格式，还是<sessionid,fullAggrInfo>
						
						//聚合数据，用什么样的格式进行拼接？
						//我们这里统一定义，使用key=value|key=vale
						
						//在Constants.java中定义spark作业相关的常量
						//String FIELD_SESSION_ID = "sessionid";
						//String FIELD_SEARCH_KEYWORDS = "searchKeywords";
						//String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
						//String FIELD_VISIT_LENGTH = "visitLength";
						//String FIELD_STEP_LENGTH = "stepLength";
						//String FIELD_START_TIME = "starttime";
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);
						
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
			
				});
		
		//查询所有用户数据，并映射成<userid,Row>的格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
				new PairFunction<Row, Long, Row>(){

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
					
				});
		
		//将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
				userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		//对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
				
				new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						//在Constants.java中添加以下常量
						//String FIELD_AGE = "age";
						//String FIELD_PROFESSIONAL = "professional";
						//String FIELD_CITY = "city";
						//String FIELD_SEX = "sex";
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex ;
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
				
					
				});
		return sessionid2FullAggrInfoRDD;
	}
	
	/**
	 * 过滤session数据，并进行聚合统计
	 * @param sessionid2AggrInfoRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam, 
			final Accumulator<String> sessionAggrAccumulator) {
		//为了使用后面的ValieUtils,所以，首先将所有的筛选参数拼接成一个连接串
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
		
		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		
		//根据筛选参数进行过滤
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
				
				new Function<Tuple2<String, String>, Boolean>() {

				
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						//首先，从tuple中，获取聚合数据
						String aggrInfo = tuple._2;
						
						//接着，依次按照筛选条件进行过滤
						//按照年龄范围进行过滤（startAge、endAge）
						//先在Constants.java中添加常量
						//String PARAM_START_AGE = "startAge";
						//String PARAM_END_AGE = "endage";
						//String PARAM_PROFESSIONALS = "professionals";
						//String PARAM_CITIES = "cities";
						//String PARAM_SEX = "sex";
						//String PARAM_KEYWORDS = "keywords";
						//String PARAM_CATEGORY_IDS = "categoryIds";
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
								parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
							return false;
						}
						
						//按照职业范围进行过滤（professionals）
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)) {
							return false;
						}
						
						//按照城市范围进行过滤（cities）
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
								parameter, Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						
						//按照性别过滤
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
								parameter, Constants.PARAM_SEX)) {
							return false;
						}
						
						//按照搜索词过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
								parameter, Constants.PARAM_KEYWORDS)) {
							return false;
							
						}
						
						//按照点击品类id进行搜索
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
								parameter, Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						
						//如果经过了之前的多个过滤条件之后，程序能够走到这里
						//那么说明该session是通过了用户指定的筛选条件的，也就是需要保留的session
						//那么就要对session的访问时长和访问步长进行统计，
						//根据session对应的范围进行相应的累加计数
						//只要走到这一步，那么就是需要计数的session
						sessionAggrAccumulator.add(Constants.SESSION_COUNT);
						
						//计算出session的访问时长和访问步长的范围，并进行相应的累加
						long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
						long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
						calculateVisitLength(visitLength);
						calculateStepLength(stepLength);
						
						return true;
					}
					
					/**
					 * 计算访问时长范围
					 * @param visitLength
					 */
					private void calculateVisitLength(long visitLength) {
						if(visitLength >= 1 && visitLength <= 3) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s);
						}else if(visitLength >= 4 && visitLength <= 6) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s);
						}else if(visitLength >= 7 && visitLength <= 9) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s);
						}else if(visitLength >= 10 && visitLength <= 30) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s);
						}else if(visitLength > 30 && visitLength <= 60) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s);
						}else if(visitLength > 60 && visitLength <= 180) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m);
						}else if(visitLength > 180 && visitLength <= 600) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m);
						}else if(visitLength > 600 && visitLength <= 1800) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m);
						}else if(visitLength > 1800) {
							sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m);
						}
					}
					
					/**
					 * 计算访问步长范围
					 * @param stepLength
					 */
					private void calculateStepLength(long stepLength) {
						if(stepLength >= 1 && stepLength <= 3) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3);
						}else if(stepLength >= 4 && stepLength <= 6) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6);
						}else if(stepLength >= 7 && stepLength <= 9) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9);
						}else if(stepLength >= 10 && stepLength <= 30) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30);
						}else if(stepLength > 30 && stepLength <= 60) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60);
						}else if(stepLength > 60) {
							sessionAggrAccumulator.add(Constants.STEP_PERIOD_60);
						}
					}
			
		});
		
		return filteredSessionid2AggrInfoRDD;
	}
	
	/**
	 * 获取通过筛选条件的session的访问明细数据RDD
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2detailRDD(
			JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Row> call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
			
				});
		return sessionid2detailRDD;
	}
	
	/**
	 * 随机抽取session
	 * @param sessionid2AggrInfo
	 */
	private static void randomExtractSession(
			final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		/**
		 * 第一步，计算每天每小时的session数量
		 */
		
		//获取<yyyy-mm-dd_hh,aggrinfo>格式的RDD
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<String, String>, String, String>(){

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(
							Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}			
				});
		
		//得到每天每小时的session数量
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		
		/**
		 * 第二步，使用按是按比例随机抽取算法，计算出每小时要抽取session的索引
		 */
		
		//将<yyyy-mm-dd_hh,count>格式的map转换成<yyyy-mm-dd,<hh,count>>的格式，方便后面使用
		Map<String, Map<String, Long>> dateHourCountMap =
				new HashMap<String, Map<String, Long>>();
		for(Map.Entry<String, Object>countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));
			
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			
			hourCountMap.put(hour, count);
		}
		
		//开始实现按照时间比例随机抽取算法
		
		//总共抽取100个session，先按照天数平分
		int extractNumberPerDay = 100 / dateHourCountMap.size();
		
		//每一天每一个小时抽取session的索引，<date,<hour,(3,5,20,200)>>
		final Map<String, Map<String, List<Integer>>> dateHourExtractMap =
				new HashMap<String, Map<String, List<Integer>>>();
		Random random = new Random();
		
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			
			//计算出这一天的session总数
			long sessionCount = 0L;
			for(long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}
			
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if(hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			//遍历每个小时
			for(Map.Entry<String, Long>hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				//计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
				//就可以算出当前小时所需抽取的session数量
				long hourExtractNumber = (int)(((double)count / (double)sessionCount) 
						* extractNumberPerDay);
				
				if(hourExtractNumber > count) {
					hourExtractNumber = (int)count;
				}
				
				//先获取当前小时的存放随机数list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if(extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				//生成上面计算出来的数量的随机数
				for(int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int)count);
					
					//生成不重复的索引
					while(extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int)count);
					}
					
					extractIndexList.add(extractIndex);
				}
				
			}
			
		}
		
		/**
		 * 第三步：遍历每天每小时的session，根据随机索引抽取
		 */
		
		//执行groupByKey算子，得到<dateHour,(session aggrInfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
		
		//我们用flatMap算子遍历所有的<dateHour,(session aggrInfo)>格式的数据
		//然后会遍历每天每小时的session
		//如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
		//那么抽取该session，直接写入MySQL的random_extract_session表
		//将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
		//然后最后一步，用抽取出来的sessionid去join它们的访问行为明细数据写入session表
		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> tuple) 
									throws Exception {
						List<Tuple2<String, String>> extractSessionids = 
								new ArrayList<Tuple2<String, String>>();
						
						String dateHour = tuple._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];
						Iterator<String> iterator = tuple._2.iterator();
						
						//拿到这一天这一小时的随机索引
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
						
						//先建domain和DAO
						//先在包com.erik.sparkproject.domain中新建SessionRandomExtract.java
						//然后在包com.erik.sparkproject.dao中新建ISessionRandomExtractDAO.java
						//接着在包com.erik.sparkproject.impl中新建SessinoRandomExtractDAOImpl.java
						//最后在DAOFactory.java中添加
						//public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
						//return new SessinoRandomExtractDAOImpl();
					    //}
						ISessionRandomExtractDAO sessionRandomExtractDAO = 
								DAOFactory.getSessionRandomExtractDAO();
						
						int index = 0;
						while(iterator.hasNext()) {
							String sessionAggrInfo = iterator.next();
							
							if(extractIndexList.contains(index)) {
								String sessionid = StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
								
								//将数据写入MySQL
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								
								//增加参数
								//private static void randomExtractSession(
								//final long taskid,
								//JavaPairRDD<String, String> sessionid2AggrInfoRDD)
								sessionRandomExtract.setTaskid(taskid);
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setSessionid(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID));
								sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
								
								sessionRandomExtractDAO.insert(sessionRandomExtract);
								
								//将sessionid加入list
								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
							}
							
							index ++;
							
						}
						
						return extractSessionids;
					}
					
				});
		
		/**
		 * 第四步：获取抽取出来的session的明细数据
		 */
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = 
				extractSessionidsRDD.join(sessionid2actionRDD);
		extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

		
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				//在包com.erik.sparkproject.domain中新建SessionDetail.java
				//在包com.erik.sparkproject.dao中新建ISessionDetailDAO.java接口
				//在包com.erik.sparkproject.impl中新建SessionDetailDAOImpl.java
				//在DAOFactory.java中添加
				//public static ISessionDetailDAO getSessionDetailDAO() {
				//return new SessionDetailDAOImpl();
			    //}
				Row row = tuple._2._2;
				
				//封装sessionDetail的domain
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);
				sessionDetail.setUserid(row.getLong(1));
				sessionDetail.setSessionid(row.getString(2));
				sessionDetail.setPageid(row.getLong(3));
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));
				sessionDetail.setClickCategoryId(row.getLong(6));
				sessionDetail.setClickProductId(row.getLong(7));
				sessionDetail.setOrderCategoryIds(row.getString(8));
				sessionDetail.setOrderProductIds(row.getString(9));
				sessionDetail.setPayCategoryIds(row.getString(10));
				sessionDetail.setPayProductIds(row.getString(11));
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);
			}
			
		});
	}
	
	/*
	 * 计算各session范围占比，并写入MySQL
	 */
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		//从Accumulator统计串中获取值
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		//计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);
		
		//将访问结果封装成Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		
		//调用对应的DAO插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
	}
	
	
	/**
	 * 获取Top10热门品类
	 * @param filteredSessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(
			long taskid,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		
		//获取session访问过的所有品类id
		//访问过指的是点击、下单、支付的品类
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

				
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						
						Row row = tuple._2;
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						Long clickCategoryId = row.getLong(6);
						if(clickCategoryId != null) {
							list.add(new Tuple2<Long ,Long>(clickCategoryId, clickCategoryId));
						}
						
						String orderCategoryIds = row.getString(8);
						if(orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
							for(String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 
										Long.valueOf(orderCategoryId)));
							}
						}
						
						String payCategoryIds = row.getString(10);
						if(payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds.split(",");
							for(String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
										Long.valueOf(payCategoryId)));
							}
						}
						
						return list;
					}
					
				});
		
		//必须去重
		//如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid以及countInfo进行排序
		//最后很可能会拿到重复的数据
		categoryidRDD = categoryidRDD.distinct();
		
		/**
		 * 第二步：计算各品类的点击、下单和支付的次数
		 */
		
		//访问明细中，其中三种访问行为是点击、下单和支付
		//分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
		//分别过滤点击、下单和支付行为，然后通过map、reduceByKey等算子进行计算
		
		//计算各个品类点击次数 
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionid2detailRDD);
		
		//计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionid2detailRDD);
		
		//计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
				getPayCategoryId2CountRDD(sessionid2detailRDD);
		
		/**
		 * 第三步：join各品类与它的点击、下单和支付的次数
		 * 
		 * caetoryidRDD中包含了所有符合条件的session访问过的品类id
		 * 
		 * 上面分别计算出来的三份各品类的点击、下单和支付的次数，可能不是包含所有品类的，
		 * 比如，有的品类就只是被点击过，但是没有人下单和支付
		 * 所以，这里就不能使用join操作，而是要使用leftOutJoin操作，
		 * 就是说，如果categoryidRDD不能join到自己的某个数据，比如点击、下单或支付数据，
		 * 那么该categoryidRDD还是要保留下来的
		 * 只不过，没有join到的那个数据就是0了
		 * 
		 */
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
				categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, 
				payCategoryId2CountRDD);
		
		/**
		 * 第四步：自定义二次排序key
		 */
		
		
		/**
		 * 第五步：将数据映射成<SortKey,info>格式的RDD，然后进行二次排序（降序）
		 */
		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
				new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<CategorySortKey, String> call(
							Tuple2<Long, String> tuple) throws Exception {
						String countInfo = tuple._2;
						long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_PAY_COUNT));
						
						CategorySortKey sortKey = new CategorySortKey(clickCount, 
								orderCount, payCount);
						return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
					}
					
				});
		
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = 
				sortKey2countRDD.sortByKey(false);
		
		/**
		 * 第六步：用kake(10)取出top10热门品类，并写入MySQL
		 */
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				sortedCategoryCountRDD.take(10);
		for(Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));
			
			//封装domain对象
			Top10Category category = new Top10Category();
			category.setTaskid(taskid);
			category.setCategoryid(categoryid);
			category.setClickCount(clickCount);
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);
			
			top10CategoryDAO.insert(category);
		}
		return top10CategoryList;
	}
	
	
	/**
	 * 获取各品类点击次数RDD
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
				new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						return row.get(6) != null ? true : false;
					}
		});
		
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				new PairFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) 
							throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});
		
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
			
		return clickCategoryId2CountRDD;
		
	}
	
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row>sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
				new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(8) != null ? true : false;
					}
		});
		
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) 
							throws Exception {
						
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for(String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
			
		});
		
		return orderCategoryId2CountRDD;
	}
	
	
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
				new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(10) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) 
							throws Exception {
						
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
						}
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>(){

					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
		});
		
		return payCategoryId2CountRDD;
	}
	
	
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		
		//如果使用leftOuterJoin就有可能出现右边那个RDD中join过来时没有值
		//所以Tuple2中的第二个值用Optional<Long>类型就代表可能有值也可能没有值
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;
						
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + 
								Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
					
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;
						
						if(optional.isPresent()) {
							orderCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;
						
						if(optional.isPresent()) {
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		return tmpMapRDD;
	}
	
	/**
	 * 获取top10活跃session
	 * @param taskid
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(
			JavaSparkContext sc,
			final long taskid, 
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		
		/**
		 * 第一步：将top10热门品类的id生成一份RDD
		 */
		
		//处理List
		List<Tuple2<Long, Long>> top10CategoryIdList = 
				new ArrayList<Tuple2<Long, Long>>();
		
		for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
		}
		
		JavaPairRDD<Long, Long> top10CategoryIdRDD = 
				sc.parallelizePairs(top10CategoryIdList);
		
		/**
		 * 第二步：计算top10品类被各session点击的次数
		 */
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = 
				sessionid2detailRDD.groupByKey();
		
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						
						//计算出该session对每个品类的点击次数
						while(iterator.hasNext()) {
							Row row = iterator.next();
							
							if(row.get(6) != null) {
								long categoryid = row.getLong(6);
								
								Long count = categoryCountMap.get(categoryid);
								if(count == null) {
									count = 0L;
								}
								count ++;
								
								categoryCountMap.put(categoryid, count);
							}
						}
						
						//返回结果为<categoryid,session:count>格式
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));
						}
						
						return list;
					}
		});
		
		//获取到top10热门品类被各个session点击的次数
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = 
				top10CategoryIdRDD.join(categoryid2sessionCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
						return new Tuple2<Long, String>(tuple._1, tuple._2._2);
					}
					
				});
		
		/**
		 * 第三步：分组取topN算法实现，获取每个品类的top10活跃用户
		 */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				top10CategorySessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple) throws Exception {
						
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						//定义取TopN的排序数组
						String[] top10Sessions = new String[10];
						
						while(iterator.hasNext()) {
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);
							
							//遍历排序数组
							for(int i = 0; i < top10Sessions.length; i++) {
								//如果当前位没有数据，直接将i位数据赋值为当前sessioncount
								if(top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								}else {
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
									
									//如果sessionCount比i位的sessionCount大
									if(count > _count) {
										//从排序数组最后一位开始，到i位所有数据往后挪一位
										for(int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										//将i位赋值为sessionCount
										top10Sessions[i] = sessionCount;
										break;
									}
									
									//如果sessionCount比i位的sessionCount要小，继续外层for循环
								}
							}
						}
						
						//将数据写入MySQL表
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						for(String sessionCount : top10Sessions) {
							if(sessionCount != null) {
								String sessionid = sessionCount.split(",")[0];
								long count = Long.valueOf(sessionCount.split(",")[1]);
								
								//将top10session插入MySQL表
								Top10Session top10Session = new Top10Session();
								top10Session.setTaskid(taskid);
								top10Session.setCategoryid(categoryid);
								top10Session.setSessionid(sessionid);
								top10Session.setClickCount(count);
								
								ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
								top10SessionDAO.insert(top10Session);
								
								list.add(new Tuple2<String, String>(sessionid, sessionid));
							}
						}
						
						return list;
					}
			
		});
		
		/**
		 * 第四步：获取top10活跃session的明细数据，并写入MySQL
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2detailRDD);  
		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
			
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
			}
		});
	}
	
}
























