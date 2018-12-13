package com.hncy58.bigdata.elasticsearch;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;


/**
 * 查询条件
 * @author tdz
 * @date 2016年11月17日 下午4:09:04
 * @date 2018年08月27日 下午3:25:04 增加existsquery 与 notexistsquery，搜索中是否存在某个字段
 *
 */
public class Criteria
{

	public static enum operation {
		equal, like, range, between, wildcard, fuzzy, equalfilter, rangefilter, betweenfilter, 
		wildcardfilter, parse, querystring, notequal, gt, gte, lt, lte, existsquery, notexistsquery
	};
		
	public operation op;
	
	public String key;
	
	public float boost = 1f;
	
	public Object[] values;
	
	
	public Criteria(operation op, String key, Object... values)
	{
		init(op, key, values);
	}
	
	public Criteria(operation op, Object... values)
	{
		init(op, values);
	}
	
	public Criteria(operation op, float boost, String key, Object... values)
	{
		init(op, key, values);
		
		this.boost = boost;
	}
	
	public void init(operation op, Object... values){
		this.op = op;
		this.values = values;
	}
	
	public void init(operation op, String key, Object... values)
	{		
		this.op = op;
		this.key = key;
		this.values = values;
		
		/*if (values.length > 0 && StringUtil.isNull(values[0].toString())) { // if None of keywords to find then MatchQuery -> TermQuery 
			if (op == operation.like && EAP.search.type().equals("elasticsearch")) {
				this.op = operation.equal; 
			}
			this.values = new String[]{"*"};
		}*/
	}
	
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}
}
