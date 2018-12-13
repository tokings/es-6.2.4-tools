package com.hncy58.bigdata.elasticsearch;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hncy58.bigdata.elasticsearch.metadata.Field;
import com.hncy58.bigdata.elasticsearch.metadata.Table;
import com.hncy58.bigdata.elasticsearch.util.DateUtil;
import com.hncy58.bigdata.elasticsearch.util.StringUtil;

//import com.hisun.metadata.Field;
//import com.hisun.metadata.Table;
//import com.hisun.util.DateUtil;
//import com.hisun.util.StringUtil;


/**
 * 抽象索引工具类
 * @author tdz
 * @date 2016年11月17日 下午4:09:11
 *
 */
public abstract class AbstractIndexer implements LuceneIndexer
{

	protected String TIMEOUT = "3000";
	
		
	protected boolean running;
	
	
	public boolean isRunning()
	{
		return this.running;
	}
	
	protected Map<String, Object> indexRowMapping(String table, Map<String, Object> row ) throws SearchEngineException
	{
		Table conf = null;
//		Table conf = EAP.schema.get(table);
		
//		if (!EAP.schema.contains(table)) {
//			throw new SearchEngineException("Schema.xml 未包含指定的 table ： " + table);
//		}
		
		String pkId = row.get(conf.getPrimaryKey().getName()).toString();
		
		if (StringUtil.isNull(pkId)) {
			throw new SearchEngineException("索引记录异常：" + conf.getName() + "主键未定义，请检查模块元数据定义文件（META-INF/schema.xml）！");
		}
					
		List<Field> fields = conf.getFields();
					
		Map<String, Object> values = new HashMap<String, Object>();
		
		StringBuffer allText = new StringBuffer();
		StringBuffer keywords = new StringBuffer();
		StringBuffer _sort = new StringBuffer();
		boolean sortFlag = false;
		
		String kindValue, kindCode, keyword, defaultVal;
		
		for (Field field: fields) {
	
			if (row.containsKey(field.getName()) && field.isStore()) {
				
				if (!StringUtil.isObjectNull(row.get(field.getName())) || !StringUtil.isObjectNull(field.getDefaultVal())) {	
					
					keyword = row.get(field.getName()).toString();
					
					values.put(field.getName(), row.get(field.getName()));
					
					if (field.getType().equalsIgnoreCase("Date")) {
						values.put(field.getName(), DateUtil.getDayAfter(keyword, Calendar.HOUR_OF_DAY, 0).replace(' ', 'T'));	
					} 
					if (field.getType().equalsIgnoreCase("String")) {
						defaultVal = StringUtil.toString(row.get(field.getName()), "");
						
						if (StringUtil.isObjectNull(row.get(field.getName()))) {
							defaultVal = field.getDefaultVal();
						}
						
						values.put(field.getName(), defaultVal);
					}
					if (field.getType().equalsIgnoreCase("Integer")) {
						defaultVal = StringUtil.toString(row.get(field.getName()), "-1");
						
						if (StringUtil.isObjectNull(row.get(field.getName()))) {
							defaultVal = field.getDefaultVal();
						}
						
						values.put(field.getName(), Integer.valueOf(defaultVal));
					}
					if (field.getType().equalsIgnoreCase("Long")) {
						defaultVal = StringUtil.toString(row.get(field.getName()), "-1");
						
						if (StringUtil.isObjectNull(row.get(field.getName()))) {
							defaultVal = field.getDefaultVal();
						}
						
						values.put(field.getName(), Long.valueOf(defaultVal));
					}
					if (field.getType().equalsIgnoreCase("Numeric")) {
						values.put(field.getName(), ((BigDecimal)row.get(field.getName())).longValue());
					}
					if (field.getType().equalsIgnoreCase("Float")) {
						values.put(field.getName(), (Float)row.get(field.getName()));
					}
					
					if (field.isKeyword()) {
						allText.append(row.get(field.getName()).toString()).append(" ");
					}
					
					if (field.isKeyword()) {
						if (keyword.length() > 0) {
							keywords.append(keyword).append(",");
						}
					}
					
					if (field.isSorted()) {
						sortFlag = true;
						_sort.append(",").append(row.get(field.getName()));
					}
				}
			}
		}	
		
		/*if (values.containsKey("_all")) {
			if (allText.length() > 0) {
				values.put("_all", allText.append(values.get("_all")).toString());
			} else {
				values.put("_all", values.get("_all"));
			}
		} else {
			if (allText.length() > 0) {
				values.put("_all", allText.deleteCharAt(allText.length()-1).toString());
			}
		}*/
		
		if (!values.containsKey("KEYWORDS") && keywords.length() > 0) {
			values.put("KEYWORDS", keywords.toString());
		}
		
		if (sortFlag) {
			values.put("_sort",_sort.toString().substring(1));
		}
		
		return values;
	}

}
