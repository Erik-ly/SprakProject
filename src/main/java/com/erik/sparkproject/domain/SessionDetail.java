package com.erik.sparkproject.domain;

/**
 * session明细
 * @author Erik
 *
 */
public class SessionDetail {
	private long taskid;
	private long userid;
	private String sessionid;
	private long pageid;
	private String actionTime;
	private String searchKeyword;
	private long clickCategoryId;
	private long clickProductId;
	private String orderCategoryIds;
	private String orderProductIds;
	private String payCategoryIds;
	private String payProductIds;
	
	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public long getPageid() {
		return pageid;
	}
	public void setPageid(long pageid) {
		this.pageid = pageid;
	}
	public String getActionTime() {
		return actionTime;
	}
	public void setActionTime(String actionTime) {
		this.actionTime = actionTime;
	}
	public String getSearchKeyword() {
		return searchKeyword;
	}
	public void setSearchKeyword(String searchKeyword) {
		this.searchKeyword = searchKeyword;
	}
	public long getClickCategoryId() {
		return clickCategoryId;
	}
	public void setClickCategoryId(long clickCategoryId) {
		this.clickCategoryId = clickCategoryId;
	}
	public long getClickProductId() {
		return clickProductId;
	}
	public void setClickProductId(long clickProductId) {
		this.clickProductId = clickProductId;
	}
	public String getOrderCategoryIds() {
		return orderCategoryIds;
	}
	public void setOrderCategoryIds(String orderCategoryIds) {
		this.orderCategoryIds = orderCategoryIds;
	}
	public String getOrderProductIds() {
		return orderProductIds;
	}
	public void setOrderProductIds(String orderProductIds) {
		this.orderProductIds = orderProductIds;
	}
	public String getPayCategoryIds() {
		return payCategoryIds;
	}
	public void setPayCategoryIds(String payCategoryIds) {
		this.payCategoryIds = payCategoryIds;
	}
	public String getPayProductIds() {
		return payProductIds;
	}
	public void setPayProductIds(String payProductIds) {
		this.payProductIds = payProductIds;
	}
	
	

}
