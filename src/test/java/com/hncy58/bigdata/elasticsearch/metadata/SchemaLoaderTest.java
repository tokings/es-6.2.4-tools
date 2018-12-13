package com.hncy58.bigdata.elasticsearch.metadata;

import static org.junit.Assert.*;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.hncy58.bigdata.elasticsearch.client.TransportClientBuilder;

public class SchemaLoaderTest {

	@Test
	public void test() {
		try {
			SchemaLoader.loadIndexDefintionsFromJsonFile("/indices/lds-info_customer_credit.json");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void creatIndexAndPutMapping() {
		TransportClient client = (new TransportClientBuilder()).build();
		String index = "lddds-info_customer_credit";
		
		try {
			Map schemaMap = SchemaLoader.loadIndexDefintionsFromJsonFile("/indices/lds-info_customer_credit.json");
//			client.admin().indices().prepareCreate(index).get();
			
			// 创建索引的同时配置Mapping
			CreateIndexResponse res = client.admin().indices().prepareCreate(index).addMapping("table", JSON.toJSONString(schemaMap.get("mappings")), XContentType.JSON).get();
			System.out.println(res);
			System.out.println(JSON.toJSONString(res));
		} catch (Exception e) {
			System.out.println(e.getMessage());
			fail(e.getMessage());
		}
	}
}
