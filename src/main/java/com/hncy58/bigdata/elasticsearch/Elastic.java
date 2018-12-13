package com.hncy58.bigdata.elasticsearch;


/**
 * Elastic工具类
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月17日 下午4:09:21
 *
 */
public class Elastic
{
	
	public static String type(String _type)
	{
		if (_type.equalsIgnoreCase("int") || _type.equalsIgnoreCase("numeric")) {
			return "String";
		}
		
		if (_type.equalsIgnoreCase("date")) {
			return "date";
		}
		
		return _type;	
	}
}
