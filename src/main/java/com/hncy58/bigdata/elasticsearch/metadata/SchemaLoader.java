package com.hncy58.bigdata.elasticsearch.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hncy58.bigdata.elasticsearch.SearchEngineException;
import com.hncy58.bigdata.elasticsearch.util.ResourceFileUtil;


/**
 * 元数据定义
 * @author tdz
 * @date 2016年11月17日 下午4:07:12
 *
 */
public class SchemaLoader
{	
	
	@SuppressWarnings("unchecked")
	public void load(String context, String indice, InputStream in) throws IOException, SearchEngineException
	{		
		if (in == null) {
			return;
//			ServiceLog.info("Schema.xml not exits in Module" + context );
		}
		
		SAXReader reader = new SAXReader();
		
		try {
			Document doc = reader.read(in);
			
			List<Element> tables = doc.getRootElement().elements();
			
			for (Element table: tables) {
				
//				EAP.schema.load(EAP.search, table);
			}
			
		} catch (DocumentException e) {
//			ServiceLog.error("Fail to read schema.xml in Module" + context);
		} finally {
			if (in != null) { 
				try { in.close(); } catch (IOException e) { } 
			}
		}
	}
	
	/**
	 * 从json文件中加索引配置信息
	 * @author luodongshan
	 * @date 2018年6月9日 上午9:28:15
	 * @param jsonFilePath
	 * @return 索引定义的Map, 如果文件不存在，返回null
	 * @throws Exception 出错
	 */
	public static Map loadIndexDefintionsFromJsonFile(String jsonFilePath) throws Exception {
		
		FileReader fileReader;
		try {
			File file = ResourceFileUtil.getResourceFile(jsonFilePath);
			
			if(!file.exists()) {
				return null;
			}
			
			fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			int limit = 1024000;
			char []charBuffer = new char[limit];
			StringBuilder builder = new StringBuilder();
			while(true) {
				int count = bufferedReader.read(charBuffer);
				if(count < 0) {
					break;
				}
				for(int i = 0; i < count; i++) {
					builder.append(charBuffer[i]);
				}
			}
			String jsonString = builder.toString().trim();
			JSONObject jsonObj = (JSONObject) JSON.parse(jsonString);
			Map schemaMap = JSON.toJavaObject(jsonObj, Map.class);
//			System.out.println(jsonObj.toJSONString());
//			System.out.println(((JSONObject)jsonObj.get("mappings")).toJSONString());
//			System.out.println(schemaMap.get("mappings").toString());
//			System.out.println(schemaMap.get("mappings"));
//			System.out.println(((Map)schemaMap.get("mappings")).get("table"));
			bufferedReader.close();
			return schemaMap;
		} catch (Exception e) {
			throw e;
		}
		
	}
	
}
