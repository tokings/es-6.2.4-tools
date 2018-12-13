package com.hncy58.bigdata.elasticsearch.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.dom4j.Element;

import com.hncy58.bigdata.elasticsearch.util.StringUtil;

/**
 * 表定义
 * @author tdz
 * @date 2016年11月17日 下午4:07:21
 *
 */
public class Table
{
	private String name;
	
	private String title;
	
	private Field primaryKeyField;
	
	private String indice;
	
	private List<Field> fields = new ArrayList<Field>();
	

	@SuppressWarnings("unchecked")
	public Table(Element element)
	{
		this.name = element.getName();
		
		this.title = element.attributeValue("title");
		this.title = StringUtil.nvl(this.title, "");
		
		this.indice = element.attributeValue("indice");
		this.indice = StringUtil.nvl(this.indice, "");
		
		List<Element> columns = element.elements("column");
		
		for (Element column: columns) {
			
			fields.add(new Field(column));
			
			if (!StringUtil.isNull(column.attributeValue("primary-key"))) {
				
				this.primaryKeyField = new Field(column);
			}			
		}
	}

	public String getName()
	{
		return this.name;
	}
	
	public String getTitle()
	{
		return this.title;
	}
	
	public String getIndice()
	{
		return this.indice;
	}
	
	public Field getPrimaryKey()
	{
		return this.primaryKeyField;
	}
	
	public List<Field> getFields()
	{
		return this.fields;
	}
	
	public Map<String, Field> getMetaData()
	{
		Map<String, Field> metadata = new HashMap<String, Field>();	
		
		for (Field field: fields) {
			metadata.put(field.getName(), field);
		}	
		
		return metadata;
	}
	
	public List<String> validate(Map<String,Object> data, String[] fields)
	{
		List<String> list = new ArrayList<String>();
		
		List<Field> fieldList = this.getFields();
		
		String errorDescription = "";
		
		for(Field field:fieldList)
		{
			String name = field.getName();
			
			for(int j=0; j<fields.length; j++)
			{
				if(name.equals(fields[j]))
				{
					String value = StringUtil.toString(data.get(name),"");//要校验的字段的值
					
					int dataLength = value.length();//要校验的字段的实际长度
					
//					errorDescription = FieldTypeValidator.validateField(field, value, dataLength, j);
					errorDescription = "";
					
					if(!"".equals(errorDescription))
					{
						list.add(errorDescription);
					}
				}
			}
		}
		
		return list;
	}
	
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}
}
