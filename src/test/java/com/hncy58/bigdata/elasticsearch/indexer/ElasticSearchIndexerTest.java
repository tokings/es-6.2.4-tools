package com.hncy58.bigdata.elasticsearch.indexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.hncy58.bigdata.elasticsearch.PageQueryResult;
import com.hncy58.bigdata.elasticsearch.Query;
import com.hncy58.bigdata.elasticsearch.SearchEngineException;
import com.hncy58.bigdata.elasticsearch.client.TransportClientBuilder;
import com.hncy58.bigdata.elasticsearch.searcher.ElasticSearchSearcher;

public class ElasticSearchIndexerTest {

	private static String[] dbs = { "test_20181211" };
	private static String table = "table";

	@Test
	public void testSearcherByEqual() {
		ElasticSearchSearcher elasticSearchSearcher = new ElasticSearchSearcher();

		Query query = new Query(1, 1);
		query.addEqualFilter("key", "2");
		try {
			PageQueryResult result = elasticSearchSearcher.query(dbs, table, query);
			System.out.println(result.toMap());
			System.out.println(JSON.toJSONString(result));
		} catch (SearchEngineException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSearcherByRange() {
		ElasticSearchSearcher elasticSearchSearcher = new ElasticSearchSearcher();

		Query query = new Query(1, 1);
		query.addBetweenCriteria("key", "1", "200");
		try {
			PageQueryResult result = elasticSearchSearcher.query(dbs, table, query);
			System.out.println(JSON.toJSONString(result));
		} catch (SearchEngineException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testUpsertSingleRow() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();

		Map<String, Object> row = new HashMap<String, Object>();

		String id = "2";
//		String id = new Random().nextInt(100000) + "";
		row.put("key", id);
		row.put("value", UUID.randomUUID().toString());
		row.put("remark", "remark");

		try {
			elasticSearchIndexer.update(dbs[0], table, id, row);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIndexSingleRow() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();

		Map<String, Object> row = new HashMap<String, Object>();

		String id = "2";
//		String id = new Random().nextInt(100000) + "";
		row.put("key", id);
		row.put("value", UUID.randomUUID().toString());
//		row.put("remark", "remark");

		try {
			elasticSearchIndexer.index(dbs[0], table, id, row);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUpsertAllList() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();
		
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for(int i = 0; i < 10; i++) {
			Map<String, Object> row = new HashMap<String, Object>();
			String id = new Random().nextInt(100000) + "";
			row.put("key", id);
			row.put("value", UUID.randomUUID().toString());
			list.add(row);
		}
		
		try {
			elasticSearchIndexer.update(dbs[0], table, "key", list);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetMappingFromIndex() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();
		Client client = new TransportClientBuilder().build();
		;

		String db = "lds_inf_customer_credit";
		String table = "customer_credit";

		db = "ods_risk-inf_customer_credit";
		table = "table";

		try {

			GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(db).types(table);
			GetMappingsResponse resp = client.admin().indices().getMappings(getMappingsRequest).actionGet();
			System.out.println(JSON.toJSON(resp));

			System.out.println(JSON.toJSON(resp.getMappings()));

			ImmutableOpenMap<String, IndexMetaData> metaData = client.admin().cluster().prepareState().execute()
					.actionGet().getState().getMetaData().getIndices();
			System.out.println(JSON.toJSON(metaData));
			ImmutableOpenMap<String, MappingMetaData> mapping = metaData.get(db).getMappings();
			System.out.println(JSON.toJSONString(mapping));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIndexerByList() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();

		String db = "lds_index_list";
		String table = "table";
		table = "table";

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put("cert_id", "430524198511242234" + "ldss" + i);
			row.put("id", "2HbNi1rCKaEhSv0w148RrK" + i);
			list.add(row);
		}

		try {
			elasticSearchIndexer.indexAll(db, table, list, "id", false);
			// elasticSearchIndexer.delete(db, table, "2HbNi1rCKaEhSv0w148RrK");
			// elasticSearchIndexer.delete(db);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeleteByList() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();

		String db = "lds_index_list";
		String table = "table";
		table = "table";

		List<String> list = new ArrayList<String>();
		for (int i = 0; i < 9; i++) {
			Map<String, Object> row = new HashMap<String, Object>();
			list.add("2HbNi1rCKaEhSv0w148RrK" + i);
		}

		try {
			elasticSearchIndexer.deleteAll(db, table, list);
			// elasticSearchIndexer.delete(db, table, "2HbNi1rCKaEhSv0w148RrK");
			// elasticSearchIndexer.delete(db);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
