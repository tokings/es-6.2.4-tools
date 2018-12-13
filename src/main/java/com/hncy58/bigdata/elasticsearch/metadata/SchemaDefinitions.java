package com.hncy58.bigdata.elasticsearch.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.dom4j.Element;

import com.hncy58.bigdata.elasticsearch.LuceneIndexer;
import com.hncy58.bigdata.elasticsearch.SearchEngineException;


/**
 * 元数据定义缓存
 * @author tdz
 * @date 2016年11月17日 下午4:07:02
 *
 */
public class SchemaDefinitions
{
		
	private final static Map<String, Table> config = new HashMap<String, Table>(); 
	
	
	/**
	 * 从 XML 描述文件中的节点导入配置描述缓存
	 * @param element 表格对应的文档节点
	 * @throws SearchEngineException 
	 * @throws IOException 
	 */
	public void load(LuceneIndexer indexer, Element element) throws IOException, SearchEngineException
	{
		Table table = new Table(element);	
		
		if (!table.getIndice().equals("")) {
//			ServiceLog.debug("Load index mapping: " + table.getIndice() + " -> " + table.getName());
			try{
//				indexer.loadSchema(table.getIndice(), table.getName(), table);
			}catch(Exception e){
//				ServiceLog.error("Failed to Load index mapping:"+table.getName(),e);
			}
		}
		
		config.put(element.getName(), table);		
	}
	
	public void load(Element element) throws IOException, SearchEngineException
	{
		Table table = new Table(element);	
		if (!table.getIndice().equals("")) {
//			ServiceLog.debug("Load index mapping: " + table.getIndice() + " -> " + table.getName());
			try{
//				EAP.search.loadSchema(table.getIndice(), table.getName(), table);
			}catch(Exception e){
//				ServiceLog.error("Failed to Load index mapping:"+table.getName(),e);
			}
			
		}
		
		config.put(element.getName(), table);		
	}
	
	/**
	 * 获取指定表格的配置描述
	 * @param table
	 * @return
	 */
	public Table get(String table)
	{
		return config.get(table);
	}
	
	/**
	 * 检查是否存在指定表格的配置描述
	 * @param table
	 * @return
	 */
	public boolean contains(String table)
	{
		return config.containsKey(table);
	}
	
}
