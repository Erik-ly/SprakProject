package com.erik.sparkproject.spark.session;

import org.apache.spark.AccumulatorParam;

import com.erik.sparkproject.constant.Constants;
import com.erik.sparkproject.util.StringUtils;

/**
 * session聚合统计Accumulator
 * 使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 然后可以基于这种特殊的数据格式实现复杂的分布式计算逻辑
 * 各个task分布式在运行，可以根据自己的需求，task给Accumulator传入不同的值
 * 根据不同的值，去做复杂的逻辑
 * 
 * Spark Core里很使用的高端技术
 * @author Erik
 *
 */
public class SesssionAggrStatAccumulator implements AccumulatorParam<String> {

	private static final long serialVersionUID = -2113961376143864034L;

	//zero方法，主要用于数据初始化
	//这里就返回一个值，就是初始化中，所有范围区间的数值都是0
	//各个范围区间的统计数量的拼接，还是采用key=value|key=value的连接串格式
	//在Constants.java中添加
	//String SESSION_COUNT = "session_count";
	//String TIME_PERIOD_1s_3s = "1s_3s";
	//String TIME_PERIOD_4s_6s = "4s_6s";
	//String TIME_PERIOD_7s_9s = "7s_9s";
	//String TIME_PERIOD_10s_30s = "10s_30s";
	//String TIME_PERIOD_30s_60s = "30s_60s";
	//String TIME_PERIOD_1m_3m = "1m_3m";
	//String TIME_PERIOD_3m_10m = "3m_10m";
	//String TIME_PERIOD_10m_30m = "10m_30m";
	//String TIME_PERIOD_30m = "30m";
	//String STEP_PERIOD_1_3 = "1_3";
	//String STEP_PERIOD_4_6 = "4_6";
	//String STEP_PERIOD_7_9 = "7_9";
	//String STEP_PERIOD_10_30 = "10_30";
	//String STEP_PERIOD_30_60 = "30_60"; 
	//String STEP_PERIOD_60 = "60";
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	//addInPlace和addAccumulator可以理解为是一样的
	//这两个方法，主要实现，v1可能就是我们初始化的那个连接串
	//v2就是在遍历session的时候，判断出某个session对应的区间，然后用Constants.TIME_PERIOD_1S_3S
	//所以，我们要做的事情就是在v1中，找到对应的v2对应的value，累加1，然后再更新到连接串里面去
	public String addInPlace(String v1, String v2) {
		return null;
	}

	public String addAccumulator(String v1, String v2) {
		return null;
	}

	/**
	 * session统计计算逻辑
	 * @param v1连接串
	 * @param v2 范围区间
	 * @return更新以后的连接串
	 */
	private String add(String v1, String v2) {
		//校验：v1为空的时候，直接返回v2
		if(StringUtils.isEmpty(v1)) {
			return v2;
		}
		
		//使用StringUtils工具类，从v1中提取v2对应的值，并累加1
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue != null) {
			//将范围区间原有的值累加1
			int newValue = Integer.valueOf(oldValue) + 1;
			
			//使用StringUtils工具类，将v1中，v2对应的值设置成心的累加胡的值
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
	
		return v1;
	}
}
