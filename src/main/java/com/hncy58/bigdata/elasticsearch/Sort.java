package com.hncy58.bigdata.elasticsearch;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.elasticsearch.search.sort.SortOrder;


/**
 * 排序。支持多列组合排序，score 默认排在最后一列排序
 * @author tdz
 * @date 2016年11月17日 下午4:07:41
 *
 */
public class Sort
{
	/** 排序字段 */
	public String field;
	
	/** 排序方式: asc | desc */
	public SortOrder order; // 默认 desc
	
	/**
	 * 构造方法，默认为 倒排
	 * @param field
	 */
	public Sort(String field)
	{
		this.field = field;
		this.order = SortOrder.DESC;
	}
			
	/**
	 * 构造方法，指定排序方式
	 * @param field 排序字段
	 * @param order	排序方式
	 */
	public Sort(String field, SortOrder order)
	{
		this.field = field;
		this.order = order;
	}
	
	/**
	 * 构造方法，指定排序方式
	 * @param field 排序字段
	 * @param order	排序方式
	 */
	public Sort(String field, String order)
	{
		this.field = field;
		
		if(order.equalsIgnoreCase(SortOrder.ASC.toString())){
			this.order = SortOrder.ASC;
		} else {
			this.order = SortOrder.DESC;
		}
	}
	
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}
}
