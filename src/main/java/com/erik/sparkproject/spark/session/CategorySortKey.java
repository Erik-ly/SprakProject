package com.erik.sparkproject.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 品类二次排序key
 * 
 * 封装你要进行排序算法需要的几个字段；点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 * 
 * 跟其他key相比，如何来判定大于、大于等于、小于、小于等于
 * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
 * 
 * 自定义的二次排序key，必须要实现serializable接口，表明是可以序列化 的，否则会报错
 * @author Erik
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable{

	private static final long serialVersionUID = -565992549650791884L;
	
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	//使用右键-source-Generate Constructor using Fields进行构造方法
	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	public boolean $greater(CategorySortKey other) {
		if(clickCount > other.getClickCount()) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount > other.getOrderCount()) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				payCount > other.getPayCount()) {
			return true;
		}
		return false;
	}

	public boolean $greater$eq(CategorySortKey other) {
		if($greater(other)) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				payCount == other.getPayCount()) {
			return true;
		}
		return false;
	}

	public boolean $less(CategorySortKey other) {
		if(clickCount < other.getClickCount()) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount < other.getOrderCount()) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				payCount < other.getPayCount()) {
			return true;
		}
		return false;
	}

	public boolean $less$eq(CategorySortKey other) {
		if($less(other)) {
			return true;
		}else if(clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				payCount == other.getPayCount()) {
			return true;
		}
		return false;
	}

	public int compare(CategorySortKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int)(clickCount - other.getClickCount());
		}else if(orderCount - other.getOrderCount() != 0) {
			return (int)(orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount() != 0){
			return (int)(payCount - other.getPayCount());
		}
		return 0;
	}

	public int compareTo(CategorySortKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int)(clickCount - other.getClickCount());
		}else if(orderCount - other.getOrderCount() != 0) {
			return (int)(orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount() != 0){
			return (int)(payCount - other.getPayCount());
		}
		return 0;
	}

	
	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}

}
