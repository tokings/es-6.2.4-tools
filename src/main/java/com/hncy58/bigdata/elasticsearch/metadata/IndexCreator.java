package com.hncy58.bigdata.elasticsearch.metadata;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSON;
import com.hncy58.bigdata.elasticsearch.client.TransportClientBuilder;

/**
 * 索引创建器
 * 
 * @author luodongshan
 * @date 2018年6月9日 上午10:16:53
 *
 */
public class IndexCreator {

	/**
	 * 根据指定的索引名创建索引，索引配置文件从
	 * /indices/${indexName}.json配置文件中读取，如果文件不存在或者有问题，则不设置mapping
	 * 
	 * @author luodongshan
	 * @date 2018年6月9日 上午10:19:03
	 * @param indexName
	 *            索引名
	 * @throws Exception
	 */
	public static boolean createIndexByFile(String indexName, String type) throws Exception {
		String jsonFilePath = "/indices/" + indexName + ".json";
		return createIndexByFile(indexName, type, jsonFilePath);
	}

	/**
	 * 
	 * @author luodongshan
	 * @date 2018年6月9日 上午10:25:53
	 * @param indexName
	 * @param jsonFilePath
	 * @throws Exception
	 */
	public static boolean createIndexByFile(String indexName, String type, String jsonFilePath) throws Exception {
		TransportClient client = (new TransportClientBuilder()).build();
		boolean ret = createIndexBySchemaFile(client, indexName, type, jsonFilePath);
		client.close();
		return ret;
	}

	/**
	 * 
	 * 使用指定的客户端，指定的索引名从指定的文件中加载索引配置信息，使用配置信息中的mapping来创建索引
	 * 
	 * @author luodongshan
	 * @date 2018年6月9日 上午10:25:56
	 * @param client
	 *            ES客户端
	 * @param indexName
	 *            索引名
	 * @param jsonFilePath
	 *            索引配置信息json文件
	 * @return
	 * @throws Exception
	 */
	public static boolean createIndexBySchemaFile(TransportClient client, String indexName, String type, String jsonFilePath)
			throws Exception {

		Map schemaMap = SchemaLoader.loadIndexDefintionsFromJsonFile(jsonFilePath);
		boolean exists = client.admin().indices().prepareExists(indexName).get().isExists();
		boolean ret = false;
		
		if(exists) {
			PutMappingRequestBuilder mappingBuilder = client.admin().indices().preparePutMapping(indexName).setType(type);
			mappingBuilder.setSource(JSON.toJSONString(schemaMap.get("mappings")), XContentType.JSON);
			ret = mappingBuilder.get().isAcknowledged();
		} else {
			CreateIndexRequestBuilder createBuilder = client.admin().indices().prepareCreate(indexName);
			createBuilder.setSettings(JSON.toJSONString(schemaMap.get("settings")), XContentType.JSON);
			// 创建索引的同时配置Mapping
			if (null != schemaMap) {
				createBuilder.addMapping(type, JSON.toJSONString(schemaMap.get("mappings")), XContentType.JSON);
			}
			
			CreateIndexResponse res = createBuilder.get();
			ret = res.isAcknowledged();
		}

		return ret;
	}
}
