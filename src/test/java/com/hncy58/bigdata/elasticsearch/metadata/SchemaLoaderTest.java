package com.hncy58.bigdata.elasticsearch.metadata;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.hncy58.bigdata.elasticsearch.Criteria;
import com.hncy58.bigdata.elasticsearch.PageQueryResult;
import com.hncy58.bigdata.elasticsearch.Query;
import com.hncy58.bigdata.elasticsearch.indexer.ElasticSearchIndexer;
import com.hncy58.bigdata.elasticsearch.searcher.ElasticSearchSearcher;

public class SchemaLoaderTest {

	public static final ElasticSearchIndexer indexer = new ElasticSearchIndexer("localhost:9300,localhost:9301",
			"hncy58");

	@Test
	public void test() {
		try {
			SchemaLoader.loadIndexDefintionsFromJsonFile("/indices/lds-info_customer_credit.json");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void createIndexAndPutMapping() {
		Client client = indexer.getClient();
		String index = "test_join_1";
		String type = "_doc";

		try {
			Map schemaMap = SchemaLoader.loadIndexDefintionsFromJsonFile("/indices/test_join.json");
			// 创建索引的同时配置settings、mapping
			CreateIndexResponse res = client.admin().indices().prepareCreate(index)
					.setSettings(JSON.toJSONString(schemaMap.get("settings")), XContentType.JSON)
					.addMapping(type, JSON.toJSONString(schemaMap.get("mappings")), XContentType.JSON).get();
			System.out.println(JSON.toJSONString(res));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void deleteIndex() {
		Client client = indexer.getClient();
		String index = "test_20190412";
		try {
			DeleteIndexResponse res = client.admin().indices().prepareDelete(index).execute().get();
			System.out.println(JSON.toJSONString(res));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void putMapping() {
		Client client = indexer.getClient();
		String index = "employee";
		try {
			Map schemaMap = SchemaLoader.loadIndexDefintionsFromJsonFile("/indices/employee.json");
			PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(index);

			PutMappingResponse res = builder.setType(index)
					.setSource(JSON.toJSONString(schemaMap.get("mappings")), XContentType.JSON).get();
			System.out.println(JSON.toJSONString(res));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void batchIndexGeoPoint() {
		String index = "test_20190412";
		long start = System.currentTimeMillis();
		Random random = new Random();

		List<Map<String, Object>> list = new ArrayList<>();

		try {
			for (int i = 0; i < 10000; i++) {
				Map<String, Object> row = new HashMap<>();
				row.put("id", start + i);
				row.put("name", "唐嘉浩" + i);
				row.put("age", random.nextInt(100));
				row.put("salary", 1000000.00 + random.nextInt(100000));
				row.put("birth", "2019-0" + (random.nextInt(8) + 1) + "-1" + random.nextInt(10) + " 18:22:00.000");
				row.put("location", random.nextInt(90) + ".715088," + random.nextInt(180) + ".98088");
				row.put("remark", "健健康康" + start + i);

				list.add(row);

				// indexer.index(index, "table", row.get("id").toString(), row);
			}

			indexer.indexAll(index, "table", list, "id");
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void indexGeoPoint() {
		String index = "test_20190412";
		long start = System.currentTimeMillis();
		Random random = new Random();

		List<Map<String, Object>> list = new ArrayList<>();

		try {
			Map<String, Object> row = new HashMap<>();
			row.put("id", 1);
			row.put("name", "唐嘉浩");
			row.put("age", random.nextInt(100));
			row.put("salary", 1000000.00 + random.nextInt(100000));
			row.put("birth", "2019-0" + (random.nextInt(8) + 1) + "-1" + random.nextInt(10) + " 18:22:00.000");
			row.put("location", random.nextInt(90) + ".715088," + random.nextInt(180) + ".98088");
			row.put("remark", "健健康康" + start);
			row.put("remark_2", "健健康康" + start);
			row.put("age_1", random.nextInt(100));
			row.put("salary_1", 1000000.00 + random.nextInt(100000));

			list.add(row);

			indexer.index(index, "table", row.get("id").toString(), row);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void indexParent() {
		String index = "company";

		List<Map<String, Object>> list = new ArrayList<>();

		try {
			Map<String, Object> row = new HashMap<>();
			row.put("id", "london");
			row.put("name", "湖南长银五八消费金融股份有限公司");
			row.put("city", "长沙");
			row.put("country", "中国");
			row.put("companyId", "london");

			list.add(row);

			indexer.index(index, index, row.get("id").toString(), row);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void indexChild() {
		String index = "employee";
		Random random = new Random();
		List<Map<String, Object>> list = new ArrayList<>();

		try {
			Map<String, Object> row = new HashMap<>();
			row.put("id", 1);
			row.put("pid", "london");
			row.put("name", "唐嘉浩");
			row.put("birth", "2019-0" + (random.nextInt(8) + 1) + "-1" + random.nextInt(10));
			row.put("hobby", "hobby");
			row.put("companyId", "london");
			row.put("employeeId", 1);

			list.add(row);

			indexer.index(index, index, row.get("id").toString(), row.get("pid").toString(), row);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void queryParent() {
		String pIndex = "company";
		String index = "employee";
		ElasticSearchSearcher searcher = new ElasticSearchSearcher(indexer.getClient());
		try {
			Query query = new Query(1, 10000);
			
			List<Criteria> subCriterias = new ArrayList<>();
			subCriterias.add(new Criteria(Criteria.operation.equal, "country", "london"));
			
			query.addHasParentFilter(pIndex, subCriterias.toArray(new Criteria[subCriterias.size()]));;
			
			PageQueryResult pr = searcher.query(new String[] { index }, index, query);
			pr.getResultSet().forEach(t -> System.out.println(t));
			
			System.out.println(pr.getTotalSize());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void queryJoin() {
		String prelat = "question";
		String crelat = "answer";
		String index = "test_join";
		String type = "_doc";
		ElasticSearchSearcher searcher = new ElasticSearchSearcher(indexer.getClient());
		try {
			Query query = new Query(1, 10000);
			
			List<Criteria> subCriterias = new ArrayList<>();
			
//			subCriterias.add(new Criteria(Criteria.operation.querystring, "text", "父文档"));
//			query.addHasParentFilter(prelat, subCriterias.toArray(new Criteria[subCriterias.size()]));
			
			subCriterias.add(new Criteria(Criteria.operation.querystring, "text", "子文档"));
			query.addHasChildFilter(crelat, subCriterias.toArray(new Criteria[subCriterias.size()]));
			
			PageQueryResult pr = searcher.query(new String[] { index }, type, query);
			pr.getResultSet().forEach(t -> System.out.println(t));
			
			System.out.println(pr.getTotalSize());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void indexJoinParent() {
		String index = "test_join";
		String type = "_doc";
		String joinCol = "my_join_field";
		String joinVal = "question";
		long start = System.currentTimeMillis();
		List<Map<String, Object>> list = new ArrayList<>();

		try {
			Map<String, Object> row = new HashMap<>();
			row.put("id", start);
			row.put(joinCol, joinVal);
			row.put("text",  "父文档-唐嘉浩健健康康" + start);

			list.add(row);

			indexer.index(index, type, row.get("id").toString(), row);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void batchIndexJoinParent() {
		String index = "test_join";
		String type = "_doc";
		String joinCol = "my_join_field";
		String joinVal = "question";
		String keyFieldName = "id";
		long start = System.currentTimeMillis();
		List<Map<String, Object>> list = new ArrayList<>();
		int cnt = 10;
		
		try {
			for (int i = 0; i < cnt; i++) {
				Map<String, Object> row = new HashMap<>();
				row.put("id", start + i);
				row.put(joinCol, joinVal);
				row.put("text",  "父文档-唐嘉浩健健康康" + start);
				
				list.add(row);
			}
			
			indexer.indexAll(index, type, list, keyFieldName);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void indexJoinChild() {
		String index = "test_join";
		String type = "_doc";
		String joinCol = "my_join_field";
		String joinVal = "answer";
		long start = System.currentTimeMillis();
		Random random = new Random();
		
		List<Map<String, Object>> list = new ArrayList<>();
		
		try {
			Map<String, Object> row = new HashMap<>();
			Map<String, Object> joinMap = new HashMap<>();
			
			joinMap.put("name", joinVal);
			joinMap.put("parent", 1);
			
			row.put("id", start);
			row.put("text",  "子文档-唐嘉浩健健康康" + start);
			row.put(joinCol, joinMap);
			
			list.add(row);
			
			indexer.index(index, type, row.get("id").toString(), joinMap.get("parent").toString(), row);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
	
	@Test
	public void batchIndexJoinChild() {
		String index = "test_join";
		String type = "_doc";
		String joinCol = "my_join_field";
		String joinVal = "answer";
		String keyFieldName = "id";
		String parentFieldName = "parent_id";
		long start = System.currentTimeMillis();
		
		List<Map<String, Object>> list = new ArrayList<>();
		int count = 10;
		
		try {
			for (int i = 0; i < count; i++) {
				Map<String, Object> row = new HashMap<>();
				Map<String, Object> joinMap = new HashMap<>();
				
				joinMap.put("name", joinVal);
				joinMap.put("parent", 1);
				
				row.put("id", start + i);
				row.put("parent_id", joinMap.get("parent"));
				row.put("text",  "子文档-唐嘉浩健健康康" + start);
				row.put(joinCol, joinMap);
				
				list.add(row);
			}
			
			indexer.indexAll(index, type, keyFieldName, parentFieldName, list);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}

	@Test
	public void queryGeoPoint() {
		String index = "test_20190412";
		ElasticSearchSearcher searcher = new ElasticSearchSearcher(indexer.getClient());
		try {
			Query query = new Query(1, 10000);
			query.addGeoDistanceFilter("location", new Object[] { 40.71588, -73.98888, 10000 });
			// query.addEqualCriteria("id", "1");
			PageQueryResult pr = searcher.query(new String[] { index }, "table", query);
			pr.getResultSet().forEach(t -> System.out.println(t));

			System.out.println(pr.getTotalSize());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			indexer.getClient().close();
		}
	}
}
