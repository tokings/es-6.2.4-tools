package com.hncy58.bigdata.elasticsearch.indexer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.hncy58.bigdata.elasticsearch.AbstractIndexer;
import com.hncy58.bigdata.elasticsearch.Elastic;
import com.hncy58.bigdata.elasticsearch.PageQueryResult;
import com.hncy58.bigdata.elasticsearch.Query;
import com.hncy58.bigdata.elasticsearch.QueryToEsQuery;
import com.hncy58.bigdata.elasticsearch.SearchEngineException;
import com.hncy58.bigdata.elasticsearch.bean.ToDeleteDoc;
import com.hncy58.bigdata.elasticsearch.bean.ToIndexDoc;
import com.hncy58.bigdata.elasticsearch.client.TransportClientBuilder;
import com.hncy58.bigdata.elasticsearch.metadata.Field;
import com.hncy58.bigdata.elasticsearch.metadata.Table;
import com.hncy58.bigdata.elasticsearch.searcher.ElasticSearchSearcher;
import com.hncy58.bigdata.elasticsearch.util.StringUtil;

/**
 * ElasticSearch 索引工具实现类
 * 
 * @author tdz
 * @date 2016年11月17日 下午4:09:34
 *
 */
public class ElasticSearchIndexer extends AbstractIndexer {

	/** 一次批量提交的记录数 */
	private int BATCH_COMMIT_SIZE = 50000;

	private TimeValue TIMEOUT = new TimeValue(3000);

	private TimeValue TIMEOUT_BAT = TimeValue.timeValueMillis(10000);

	/** 搜索引擎客户端 */
	private TransportClient client;

	/**
	 * 构造方法
	 * 读取默认的配置文件数据构造客户端 
	 */
	public ElasticSearchIndexer() {
		 this.client = new TransportClientBuilder().build();
	}

	/**
	 * 构造方法
	 * 
	 * @param client
	 */
	public ElasticSearchIndexer(TransportClient client) {
		this.client = client;
		this.running = true;
	}

	/**
	 * @param indexServers
	 *            索引服务器
	 * @param homePath
	 *            home路径
	 * @param clusterName
	 *            集群名称
	 */
	public ElasticSearchIndexer(final String indexServers, final String clusterName) {
		String[] servers = indexServers.split(",");
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
//		client = TransportClient.builder().settings(settings).build();
		client = new PreBuiltTransportClient(settings);

		for (String server : servers) {
			try {
				String[] hostPort = server.split(":");
				client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.valueOf(hostPort[1])));
				this.running = true;
			} catch (UnknownHostException e) {

			}
		}
	}

	public Client getClient() {
		return client;
	}

	/**
	 * 删除表中指定的文档
	 * 
	 * @param table
	 *            文档所在表
	 * @param id
	 *            文档id
	 */
	@Override
	public void delete(String db, String table, String id) {
		client.prepareDelete(db, table, id).setTimeout(TIMEOUT).execute().actionGet();

		DeleteResponse response = client.prepareDelete(db, table, id).setTimeout(TIMEOUT).execute().actionGet();
//		System.out.println(response.toString());
//		if (response.isFound()) {
//			ServiceLog.debug("Document(" + id + ") deleted");
//		} else {
//			ServiceLog.debug("Document(" + id + ") not found");
//		}
	}
	
	@Override
	public void deleteAll(String indice, String table, List<String> list) throws SearchEngineException {
		try {
			DeleteRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (String key : list) {

				index = client.prepareDelete(indice, table, key);

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			// 如果请求中有action需要执行才提交，防止与ES无用交互和异常输出
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量删除文档索引失败：" + e);
		}
	}
	
	@Override
	public void deleteAll(List<ToDeleteDoc> list) throws SearchEngineException {
		try {
			DeleteRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (ToDeleteDoc toDeleteDoc : list) {

				index = client.prepareDelete(toDeleteDoc.getIndex(), toDeleteDoc.getTable(), toDeleteDoc.getId());

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			// 如果请求中有action需要执行才提交，防止与ES无用交互和异常输出
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量删除文档索引失败：" + e);
		}
	}

	@Override
	public void delete(String db, String table, Map<String, Object> row) {
		// ServiceLog.info("Delete one doc: " + row);

		Set<Entry<String, Object>> set = row.entrySet();

		BoolQueryBuilder builder = QueryBuilders.boolQuery();
		QueryBuilder query = null;

		for (Entry<String, Object> entry : set) {
			query = QueryBuilders.matchQuery(entry.getKey(), entry.getValue());
			builder.must(query);
		}

		SearchRequestBuilder req = client.prepareSearch(db).setTypes(table)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		req = req.setQuery(builder).setTimeout(TIMEOUT).setFrom(0).setSize(50);
		SearchResponse response = req.execute().actionGet();
		SearchHit[] hits = response.getHits().getHits();

		for (SearchHit hit : hits) {
			if (table.equalsIgnoreCase(hit.getType())) {
				delete(db, table, hit.getId());
			}
		}
	}

	/**
	 * 新增索引记录
	 * 
	 * @param db
	 *            文档所属库
	 * @param table
	 *            文档所属表
	 * @param row
	 *            索引文档内容
	 */
	@Override
	public void index(String db, String table, Map<String, Object> row) throws SearchEngineException {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();

			builder.startObject();
			for (Entry<String, Object> entry : row.entrySet()) {
				builder.field(entry.getKey().toLowerCase(), entry.getValue());
			}
			builder.endObject();

			client.prepareIndex(db, table).setSource(builder).setTimeout(TIMEOUT).execute().actionGet();
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + db + "." + table + ":" + row + ")创建索引失败：" + e);
		}
	}
	
	/**
	 * 新增索引记录
	 * 
	 * @param db
	 *            文档所属库
	 * @param table
	 *            文档所属表
	 * @param id
	 * 			  id字段
	 * @param row
	 *            索引文档内容
	 */
	@Override
	public void index(String db, String table, String id, Map<String, Object> row) throws SearchEngineException {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();

			builder.startObject();
			for (Entry<String, Object> entry : row.entrySet()) {
				builder.field(entry.getKey().toLowerCase(), entry.getValue());
			}
			builder.endObject();

			IndexResponse resp = client.prepareIndex(db, table).setSource(builder).setId(id).setTimeout(TIMEOUT).execute().actionGet();
			System.out.println(resp.status());
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + db + "." + table + ":" + row + ")创建索引失败：" + e);
		}
	}
	
	@Override
	public void index(String db, String table, String id, String parentId, Map<String, Object> row) throws SearchEngineException {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.map(row);
			IndexResponse resp = client.prepareIndex(db, table).setSource(builder).setId(id).setRouting(parentId).setTimeout(TIMEOUT).execute().actionGet();
			System.out.println(resp.status());
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + db + "." + table + ":" + row + ")创建索引失败：" + e);
		}
	}

	/**
	 * 从指定表进行检索
	 * 
	 * @param table
	 *            索引表
	 * @param query
	 *            查询条件
	 * @see Query
	 */
	@Override
	public PageQueryResult query(String[] dbs, String table, Query query) throws SearchEngineException {
		return new ElasticSearchSearcher(this.client).query(dbs, table, query);
	}

	/**
	 * 从指定表进行检索
	 * 
	 * @param table
	 *            索引表
	 * @param query
	 *            查询条件
	 * @see Query
	 */
	@Override
	public Map<Object, Object> queryStatistics(String[] dbs, Query query, String... table)
			throws SearchEngineException {
		return new ElasticSearchSearcher(this.client).queryStatistics(dbs, query, table);
	}

	public QueryBuilder transfer(Query query, BoolQueryBuilder builder) {
		return QueryToEsQuery.transfer(query, builder);
	}

	/** .field("indexAnalyzer", "ik").field("searchAnalyzer", "ik") */
	public void loadSchema(String indice, String type, Table table) throws IOException {
		// boolean sortedFieldExists = false;
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		builder.startObject(table.getName());
		builder.startObject("properties");

		List<Field> fields = table.getFields();

		for (Field field : fields) {
			if (field.isIndex()) {
				builder.startObject(field.getName()).field("type", Elastic.type(field.getType()))
						.field("boost", field.getBoost())
						.field("index", field.isAnalyze() ? "analyzed" : "not_analyzed")
						.field("store", field.isStore() ? "yes" : "no")
						.field("include_in_all", field.isIncludeInAll() ? "true"
								: "false");
			} else {
				builder.startObject(field.getName()).field("type", Elastic.type(field.getType()))
						.field("store", field.isStore() ? "yes" : "no").field("index", "no")
						.field("include_in_all", field.isIncludeInAll() ? "true" : "false");
			}

			if (field.isSorted()) {
				// sortedFieldExists = true;
				builder.field("doc_values", true);
			}

			builder.endObject();
		}
		// if (sortedFieldExists) {
		// builder.startObject("_sort").field("type", "string")
		// .field("doc_values", true)
		// .field("store", "yes").field("index", "not_analyzed")
		// .field("include_in_all", "false").endObject();
		// }
		builder.endObject();
		builder.endObject();
		builder.endObject();

		PutMappingRequest mapping = Requests.putMappingRequest(indice).type(type).source(builder);

		client.admin().indices().putMapping(mapping).actionGet();
	}

	@Override
	public void stop() {
		client.close();
	}

	@Override
	public String type() {
		return "elastic";
	}

	/**
	 * 删除指定的索引
	 * @author luodongshan
	 * @date 2018年6月8日 下午5:28:30
	 * @param db
	 * @throws SearchEngineException
	 */
	public void delete(String db) throws SearchEngineException {
		client.admin().indices().prepareDelete(db).execute().actionGet();
//		client.prepareDelete().setId(id)
		// ServiceLog.info("Delete all documents of table: " + table);
	}
	
	@Override
	public void delete(String db, String table) throws SearchEngineException {
//		client.prepareDelete().setId(id)
		// ServiceLog.info("Delete all documents of table: " + table);
	}

	@Override
	public void indexAll(String indice, String table, List<Map<String, Object>> list) throws SearchEngineException {
		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (Map<String, Object> row : list) {

				builder = XContentFactory.jsonBuilder();

				builder.startObject();

				for (Entry<String, Object> entry : row.entrySet()) {
					builder.field(entry.getKey(), entry.getValue());
				}

				builder.endObject();

				index = client.prepareIndex(indice, table).setSource(builder);

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			// 如果请求中有action需要执行才提交，防止与ES无用交互和异常输出
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}

	@Override
	public void indexAll(String indice, String table, List<Map<String, Object>> list, String keyFieldName)
			throws SearchEngineException {
		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (Map<String, Object> row : list) {

				builder = XContentFactory.jsonBuilder();

				builder.map(row);

				index = client.prepareIndex(indice, table).setSource(builder).setId(row.get(keyFieldName).toString());

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}

			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}
	
	@Override
	public void indexAll(String indice, String table, List<Map<String, Object>> list, String keyFieldName, Boolean needIndexKeyField)
			throws SearchEngineException {
		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (Map<String, Object> row : list) {

				builder = XContentFactory.jsonBuilder();

				builder.startObject();

				for (Entry<String, Object> entry : row.entrySet()) {
					
					if(!needIndexKeyField && entry.getKey().equals(keyFieldName)) {
						// 主键字段不需要单独进行索引，则跳过
						continue;
					}
					builder.field(entry.getKey(), entry.getValue());
				}

				builder.endObject();

				index = client.prepareIndex(indice, table).setSource(builder).setId(row.get(keyFieldName).toString());

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}

			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}
	
	@Override
	public void indexAll(List<ToIndexDoc> list) throws SearchEngineException {
		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for(ToIndexDoc toIndexDoc : list) {
				for (Map<String, Object> row : toIndexDoc.getDocList()) {
	
					builder = XContentFactory.jsonBuilder();
	
					builder.startObject();
	
					for (Entry<String, Object> entry : row.entrySet()) {
						
						if(!toIndexDoc.getNeedIndexKeyField() && entry.getKey().equals(toIndexDoc.getKeyFieldName())) {
							// 主键字段不需要单独进行索引，则跳过
							continue;
						}
						builder.field(entry.getKey(), entry.getValue());
					}
	
					builder.endObject();
	
					index = client.prepareIndex(toIndexDoc.getIndex(), toIndexDoc.getTable()).setSource(builder).setId(row.get(toIndexDoc.getKeyFieldName()).toString());
	
					request.add(index);
	
					if (gtBatchSize) {
						if (i % BATCH_COMMIT_SIZE == 0) {
							response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
							if (response.hasFailures()) {
								throw new SearchEngineException(response.buildFailureMessage());
							}
							// 如果分批提交，每次提交后重新创建空的批量请求
							request = client.prepareBulk();
						}
					}
	
					i++;
				}
			}
	
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}
	
	public void updateAll(List<ToIndexDoc> list) throws SearchEngineException {
		try {
			XContentBuilder builder;

			UpdateRequestBuilder update;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for(ToIndexDoc toIndexDoc : list) {
				for (Map<String, Object> row : toIndexDoc.getDocList()) {
	
					builder = XContentFactory.jsonBuilder();
	
					builder.startObject();
	
					for (Entry<String, Object> entry : row.entrySet()) {
						
						if(!toIndexDoc.getNeedIndexKeyField() && entry.getKey().equals(toIndexDoc.getKeyFieldName())) {
							// 主键字段不需要单独进行索引，则跳过
							continue;
						}
						builder.field(entry.getKey(), entry.getValue());
					}
	
					builder.endObject();
					update = client.prepareUpdate(toIndexDoc.getIndex(), toIndexDoc.getTable(), row.get(toIndexDoc.getKeyFieldName()).toString()).setDoc(builder);
					update.setUpsert(builder);
					request.add(update);
	
					if (gtBatchSize) {
						if (i % BATCH_COMMIT_SIZE == 0) {
							response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
							if (response.hasFailures()) {
								throw new SearchEngineException(response.buildFailureMessage());
							}
							// 如果分批提交，每次提交后重新创建空的批量请求
							request = client.prepareBulk();
						}
					}
	
					i++;
				}
			}
	
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}

	@Override
	public void index(String indice, String table, Map<String, Object> row, boolean batchCommit)
			throws SearchEngineException {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();

			builder.startObject();
			for (Entry<String, Object> entry : row.entrySet()) {
				builder.field(entry.getKey(), entry.getValue());
			}
			builder.endObject();

			client.prepareIndex(indice, table).setSource(builder).setTimeout(TIMEOUT).execute().actionGet();
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + indice + "." + table + ":" + row + ")创建索引失败：" + e);
		}
	}

	@Override
	public void index(String datasource, String table, Map<String, Object> row, String routeValue)
			throws SearchEngineException {

		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();

			builder.startObject();

			for (Entry<String, Object> entry : row.entrySet()) {
				builder.field(entry.getKey(), entry.getValue());
			}

			builder.endObject();

			client.prepareIndex(datasource, table).setSource(builder).setRouting(routeValue).setTimeout(TIMEOUT)
					.execute().actionGet();

		} catch (Exception e) {
			throw new SearchEngineException("文档(" + datasource + "." + table + ":" + row + ")创建索引失败：" + e);
		}
	}
	
	@Override
	public void indexAll(String indice, String table, String keyFieldName, String parentFieldName,
			List<Map<String, Object>> list) throws SearchEngineException {

		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			for (Map<String, Object> row : list) {

				builder = XContentFactory.jsonBuilder();

				builder.map(row);

				index = client.prepareIndex(indice, table, row.get(keyFieldName).toString()).setSource(builder)
						.setRouting(row.get(parentFieldName).toString());

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}

	@Override
	public void indexAll(String indice, String table, List<Map<String, Object>> list, String keyFieldName,
			String routeKey) throws SearchEngineException {

		try {
			XContentBuilder builder;

			IndexRequestBuilder index;

			BulkRequestBuilder request = client.prepareBulk();

			BulkResponse response = null;

			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);

			long i = 0;

			Map<String, Object> values;

			for (Map<String, Object> row : list) {

				values = indexRowMapping(table, row);

				builder = XContentFactory.jsonBuilder();

				builder.startObject();

				for (Entry<String, Object> entry : values.entrySet()) {
					builder.field(entry.getKey(), entry.getValue());
				}

				builder.endObject();

				index = client.prepareIndex(indice, table, row.get(keyFieldName).toString()).setSource(builder)
						.setRouting(row.get(routeKey).toString());

				request.add(index);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("批量创建文档索引失败：" + e);
		}
	}

	@Override
	public void index(String datasource, String table, String SQL) throws SearchEngineException {
		// TODO Auto-generated method stub

	}

	@Override
	public void indexAll(String datasource, String table, String SQL, String keyFieldName, String dsName,
			Object... parameters) throws SearchEngineException {
		// TODO Auto-generated method stub

	}

	@Override
	public void indexAllWithRowMapper(String db, String table, String SQL, String keyFieldName, String dsName,
			Object... parameters) throws SearchEngineException {
		// TODO Auto-generated method stub

	}
	
	public void update(String db, String table, String id, Map<String, Object> row) throws SearchEngineException {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();

			builder.startObject();
			for (Entry<String, Object> entry : row.entrySet()) {
				builder.field(entry.getKey(), entry.getValue());
			}	
			builder.endObject();
			
			if(StringUtil.isEmpty(id) && row.containsKey("_id")) {
				id = StringUtil.toString(row.get("_id"));
			}


			client.prepareUpdate(db, table, id).setDoc(builder).setUpsert(builder).setTimeout(TIMEOUT).execute().actionGet();
//			client.prepareUpdate(db, table, id).setDoc(builder).setTimeout(TIMEOUT).execute().actionGet();
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + db + "." + table + ":" + row + ")更新索引失败：" + e);
		}
	}
	
	public void update(String db, String table, String idFieldName, List<Map<String, Object>> list) throws SearchEngineException {
		try {
			XContentBuilder builder;
			UpdateRequestBuilder update;
			BulkRequestBuilder request = client.prepareBulk();
			BulkResponse response = null;
			boolean gtBatchSize = (list.size() > BATCH_COMMIT_SIZE);
			long i = 0;

			for (Map<String, Object> row : list) {

				if(!row.containsKey(idFieldName) 
						|| StringUtil.isObjectNull(row.get(idFieldName))) {
					continue;
				}
				
				builder = XContentFactory.jsonBuilder();
				builder.startObject();
				for (Entry<String, Object> entry : row.entrySet()) {
					builder.field(entry.getKey(), entry.getValue());
				}
				builder.endObject();

				update = client.prepareUpdate(db, table, row.get(idFieldName).toString()).setDoc(builder).setUpsert(builder);
				request.add(update);

				if (gtBatchSize) {
					if (i % BATCH_COMMIT_SIZE == 0) {
						response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
						if (response.hasFailures()) {
							throw new SearchEngineException(response.buildFailureMessage());
						}
						// 如果分批提交，每次提交后重新创建空的批量请求
						request = client.prepareBulk();
					}
				}

				i++;
			}
			
			if(request.numberOfActions() > 0) {
				response = request.setTimeout(TIMEOUT_BAT).execute().actionGet();
				if (response.hasFailures()) {
					throw new SearchEngineException(response.buildFailureMessage());
				}
			}
		} catch (Exception e) {
			throw new SearchEngineException("文档(" + db + "." + table + ":" + list + ")批量更新索引失败：" + e);
		}
	}
}
