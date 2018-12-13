package com.hncy58.bigdata.elasticsearch.metadata;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.dom4j.Element;

import com.hncy58.bigdata.elasticsearch.util.StringUtil;


/**
 * 字段描述
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月17日 下午4:06:39
 *
 */
public class Field
{
	
	private String name;
	
	private String title;
	
	private String type;
	
	private int length;
	
	private int boost;
	
	private String pkGenerator;
	
	private String dictionKind;
	
	private boolean index;
	
	private boolean store;
	
	private boolean analyze;
	
	private boolean keyword;
	
	private boolean sorted;
	
	private boolean allowNUll;
	
	private boolean includeInAll;

	private String defaultVal;
	
	public Field(Element field)
	{
		String value;
		
		this.name = field.attributeValue("name");

		this.title = field.attributeValue("title");
		this.title = StringUtil.nvl(this.title, "");
		
		this.type = field.attributeValue("type");
		
		this.dictionKind = field.attributeValue("diction-kind");
		
		this.length = Integer.parseInt(StringUtil.nvl(field.attributeValue("length"), "0"));
		
		this.boost = Integer.parseInt(StringUtil.nvl(field.attributeValue("boost"), "1"));
		
		this.pkGenerator = StringUtil.nvl(field.attributeValue("pk-generator"), "");
		
		this.index = field.attributeValue("index").equals("true");
		
		this.store = field.attributeValue("store").equals("true");
		
		value = StringUtil.nvl(field.attributeValue("sorted"), "");
		this.sorted = value.equals("true");
		
		value = StringUtil.nvl(field.attributeValue("analyze"), "");		
		this.analyze = value.equals("true");		
		
		value = StringUtil.nvl(field.attributeValue("keyword"), "");		
		this.keyword = value.equals("true");
		
		value = StringUtil.nvl(field.attributeValue("allowNull"), "");		
		this.allowNUll = value.equals("true");

		value = StringUtil.nvl(field.attributeValue("include_in_all"), "");
		this.includeInAll = value.equals("true");
		
		value = StringUtil.nvl(field.attributeValue("default_val"), "");
		this.defaultVal = value;
	}


	public String getName()
	{
		return this.name;
	}

	public String getTitle()
	{
		return this.title;
	}
	
	public String getType()
	{
		return this.type;
	}
	
	public String getDictionKey()
	{
		return this.dictionKind;
	}
	
	public boolean isDictionKey()
	{
		return !StringUtil.isNull(this.dictionKind);
	}

	public int getLength()
	{
		return this.length;
	}

	public String getPkGenerator()
	{
		return this.pkGenerator;
	}
	
	public boolean isIndex()
	{
		return this.index;
	}


	public boolean isStore()
	{
		return this.store;
	}
	
	public boolean isSorted()
	{
		return this.sorted;
	}
	
	public boolean isAnalyze()
	{
		return this.analyze;
	}
	
	public boolean isKeyword()
	{
		return this.keyword;
	}
	
	public int getBoost()
	{
		return this.boost;
	}
	
	public boolean isAllowNUll() {
		return this.allowNUll;
	}
	
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}


	public boolean isIncludeInAll() {
		return includeInAll;
	}


	public void setIncludeInAll(boolean includeInAll) {
		this.includeInAll = includeInAll;
	}


	public String getDefaultVal() {
		return this.defaultVal;
	}


	public void setDefaultVal(String defaultVal) {
		this.defaultVal = defaultVal;
	}
}
