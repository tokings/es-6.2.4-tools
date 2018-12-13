package com.hncy58.bigdata.elasticsearch.metadata;

/**
 * 元数据描述文件定义异常
 * @author tdz
 * @date 2016年11月17日 下午4:06:52
 *
 */
public class SchemaDefinedException extends Exception
{

	
	public SchemaDefinedException(String error)
	{
		super(error);
	}

	private static final long serialVersionUID = 6084888161967011135L;

}
