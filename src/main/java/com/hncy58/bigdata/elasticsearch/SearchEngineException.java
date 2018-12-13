package com.hncy58.bigdata.elasticsearch;

/**
 * 搜索引擎异常类
 * @author tdz
 * @date 2016年11月17日 下午4:07:53
 *
 */
public class SearchEngineException extends Exception
{

	private String error;
	
	
	public SearchEngineException(String error)
	{
		this.error = error;
	}
	
	public String getMessage()
	{
		return this.error;
	}

	private static final long serialVersionUID = 489878136398127195L;
	
}
