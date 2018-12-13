package com.hncy58.bigdata.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * 分页查询结果包装类
 * @author tdz
 * @date 2016年11月17日 下午4:08:38
 *
 */
public class PageQueryResult
{
	
	/* 记录行数 */
	private long total;
	
	/** 耗时 */
	private long usetime;
	
	
	/* Jdbc 结果集 */
	private List<Map<String, Object>> resultSet;
	
	/**
	 * 构造方法
	 * @param total
	 * @param resultSet
	 */
	public PageQueryResult(long total, List<Map<String, Object>> resultSet)
	{
		this.total = total;
		this.resultSet = resultSet;
	}
	
	public PageQueryResult(long total, long usetime, List<Map<String, Object>> resultSet)
	{
		this.total = total;
		this.usetime = usetime;
		this.resultSet = resultSet;
	}
	
	/**
	 * 对象转 Map
	 * @return { records: 查询结果集, count: countSQL 执行结果}
	 */
	public Map<String, Object> toMap()
	{
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("total", total);
		map.put("usetime", usetime);
		map.put("records", resultSet);
		return map;
	}
	
	public List<Map<String, Object>> getResultSet()
	{
		return this.resultSet;
	}
	
	public long getTotalSize()
	{
		return this.total;
	}
	
	public long getUsetime()
	{
		return this.usetime;
	}
	
	/**
	 * 对象转字符串
	 * @return 转换后的字符串
	 */
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}
		
}
