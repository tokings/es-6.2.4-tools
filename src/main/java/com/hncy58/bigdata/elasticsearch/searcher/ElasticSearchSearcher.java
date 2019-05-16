package com.hncy58.bigdata.elasticsearch.searcher;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import com.hncy58.bigdata.elasticsearch.Criteria;
import com.hncy58.bigdata.elasticsearch.LuceneSearcher;
import com.hncy58.bigdata.elasticsearch.PageQueryResult;
import com.hncy58.bigdata.elasticsearch.Query;
import com.hncy58.bigdata.elasticsearch.SearchEngineException;
import com.hncy58.bigdata.elasticsearch.Sort;
import com.hncy58.bigdata.elasticsearch.client.TransportClientBuilder;
import com.hncy58.bigdata.elasticsearch.util.StringUtil;

/**
 * ElasticSearch搜索器
 * 
 * @author tdz
 * @date 2016年11月17日 下午4:10:11
 *
 */
public class ElasticSearchSearcher implements LuceneSearcher {
	/** 搜索引擎客户端 */
	private Client client;

	/**
	 * 构造方法
	 * 
	 * @param servers
	 *            支持多机集群
	 */
	public ElasticSearchSearcher() {
		this.client = new TransportClientBuilder().build();
	}

	/**
	 * 通过集群名称、服务器列表构造搜索器
	 * 
	 * @param clusterName
	 *            集群名称
	 * @param clusterServers
	 *            集群服务器列表
	 */
	public ElasticSearchSearcher(String clusterServers, String clusterName) {
		super();
		this.client = new TransportClientBuilder(clusterServers, clusterName).build();
	}

	/**
	 * 构造方法
	 * 
	 * @param servers
	 *            支持多机集群
	 */
	public ElasticSearchSearcher(Client client) {
		this.client = client;
	}

	/**
	 * 从指定表进行检索
	 * 
	 * @param dbs
	 *            索引名
	 * @param table
	 *            索引表
	 * @param query
	 *            查询条件
	 * @see Query
	 */
	@Override
	public PageQueryResult query(String[] dbs, String table, Query query) throws SearchEngineException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		long count = 0;

		float findWordHitRatio = 0;

		// 导出结果处理
		SearchResponse response = queryPageResult(dbs, query, table);
		long usetime = response.getTook().getMillis();

		float maxScore = response.getHits().getMaxScore();

		count = response.getHits().getTotalHits();

		if (count > 0 && query.getHighlightFields().size() > 0) { // 默认按 评分 排序
			findWordHitRatio = computeHitRatioUseHighlightField(query.getCriterias(), response.getHits().getHits()[0]);
		}

		SearchHit[] hits = response.getHits().getHits();

		for (SearchHit hit : hits) {
			if (table.equalsIgnoreCase(hit.getType())) {
				list.add(toMap(hit, maxScore, findWordHitRatio));
			}
		}

		return new PageQueryResult(count, usetime, list);
	}

	@Override
	public Map<Object, Object> queryStatistics(String[] dbs, Query query, String... table)
			throws SearchEngineException {
		Map<Object, Object> result = new HashMap<Object, Object>();

		SearchResponse response = queryPageResult(dbs, query, table);

		if (!query.getAggregations().isEmpty()) {
			result.putAll(response.getAggregations().getAsMap());
			// for(Entry<String, AbstractAggregationBuilder> entry :
			// query.getAggregations().entrySet()) {
			// Map<Object, Long> aggs = new HashMap<Object, Long>();
			// Terms terms = response.getAggregations().get(entry.getKey());
			// if (terms != null) {
			// for (Terms.Bucket entry1 : terms.getBuckets()) {
			// aggs.put(entry1.getKey(), entry1.getDocCount());
			// }
			// }
			//
			// result.put(entry.getKey(), aggs);
			// }
		}

		return result;
	}

	@SuppressWarnings("deprecation")
	private SearchResponse queryPageResult(String[] dbs, Query query, String... table) {
		BoolQueryBuilder builder = QueryBuilders.boolQuery();
		BoolQueryBuilder filter = QueryBuilders.boolQuery();
		QueryBuilder qb = null;
		// FilterBuilder fb = null;
		QueryBuilder fb = null;

		SearchRequestBuilder req = client.prepareSearch(dbs).setTypes(table).setSearchType(query.getSearchType());
		// 查询条件
		if (query.isSearchAll()) {
			qb = QueryBuilders.matchAllQuery();
			builder.must(qb);
		} else {
			fillQuery(builder, filter, qb, fb, query.getCriterias().toArray(new Criteria[query.getCriterias().size()]));
		}

		int from = (query.getPageNo() - 1) * query.getPageSize();

		SearchResponse response;

		HighlightBuilder highlightBuilder = new HighlightBuilder();
		highlightBuilder.preTags("<em>");
		highlightBuilder.postTags("</em>");
		// highlightBuilder.f
		// 高亮显示字段
		req.highlighter(highlightBuilder);
		// req.setHighlighterPreTags("<em>");
		// req.setHighlighterPostTags("</em>");

		for (String highlightField : query.getHighlightFields()) {
			highlightBuilder.field(highlightField);
			// req.addHighlightedField(highlightField);
		}

		if (filter.hasClauses()) {
			QueryBuilders.boolQuery().must(builder).must(filter);
			req.setQuery(QueryBuilders.boolQuery().must(builder).must(filter));
		} else {
			req.setQuery(builder);
		}

		// 查询
		if (query.getSort().size() == 0) {
			req = req.setTimeout(new TimeValue(query.getTimeout())).setFrom(from).setSize(query.getPageSize());
		} else {
			req.setTimeout(new TimeValue(query.getTimeout())).setFrom(from).setSize(query.getPageSize());
			for (Sort sort : query.getSort()) {
				// req.addSort(sort.field, sort.order);
				SortBuilder sortBuilder = SortBuilders.fieldSort(sort.field).order(sort.order);
				req.addSort(sortBuilder);
			}
		}

		if (!query.getAggregations().isEmpty()) {
			for (Entry<String, AbstractAggregationBuilder> entry : query.getAggregations().entrySet()) {
				req.addAggregation(entry.getValue());
			}
		}

		if (query.getRoute().size() == 1) {
			req = req.setRouting(query.getRoute().get(0));
		} else if (query.getRoute().size() > 1) {
			req = req.setRouting(query.getRoute().toArray(new String[] {}));
		}

		if (query.getOutputFields() != null && query.getOutputFields().length > 0) {
//			req.fields(query.getOutputFields());
			req.setFetchSource(query.getOutputFields(), null);
		}

		response = req.execute().actionGet();

		return response;
	}
	
	private void fillQuery(BoolQueryBuilder builder, BoolQueryBuilder filter, QueryBuilder qb, QueryBuilder fb, Criteria... criterias) {
		
		for (Criteria criteria : criterias) {
			qb = null;
			// Query
			if (criteria.op == Criteria.operation.like) {
				qb = QueryBuilders.queryStringQuery(criteria.values[0].toString()).defaultField(criteria.key);
			}
			if (criteria.op == Criteria.operation.wildcard) {
				BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
				for (Object match : criteria.values) {
					qb = QueryBuilders.wildcardQuery(criteria.key, match.toString());
					subQuery.should(qb);
				}
				builder.must(subQuery);
				continue;
			}
			if (criteria.op == Criteria.operation.notequal) {
				qb = QueryBuilders.termsQuery(criteria.key, criteria.values);
				builder.mustNot(qb);
				continue;
			}

			// add by lds 20180827 existsquery 与 notexistsquery，搜索中是否存在某个字段
			if (criteria.op == Criteria.operation.existsquery) {
				builder.must(QueryBuilders.existsQuery(criteria.key));
				continue;
			}
			if (criteria.op == Criteria.operation.notexistsquery) {
				builder.mustNot(QueryBuilders.existsQuery(criteria.key));
				continue;
			}

			// added by tdz at 2019-04-12
			if (criteria.op == Criteria.operation.geodistance) {
				GeoDistanceQueryBuilder tmpqb = QueryBuilders.geoDistanceQuery(criteria.key).point(
						Double.valueOf(criteria.values[0].toString()),
						Double.valueOf(criteria.values[1].toString()));
				if (criteria.values.length > 3) {
					tmpqb.distance(Double.valueOf(criteria.values[2].toString()),
							DistanceUnit.fromString(criteria.values[3].toString()));
					tmpqb.geoDistance(GeoDistance.fromString(criteria.values[4].toString()));
				} else {
					tmpqb.distance(Double.valueOf(criteria.values[2].toString()), DistanceUnit.DEFAULT);
					if (criteria.values.length > 4) {
						tmpqb.geoDistance(GeoDistance.fromString(criteria.values[4].toString()));
					}
				}
				qb = tmpqb;
			}
			// added by tdz at 2019-04-12 end

			// added by tdz at 2019-04-24
			if(criteria.op == Criteria.operation.has_parent) {
				
				QueryBuilder queryBuilder = null ;
				Criteria[] subCriterias = (Criteria[]) criteria.values;
				
				BoolQueryBuilder subBuilder = QueryBuilders.boolQuery();
				BoolQueryBuilder subFilter = QueryBuilders.boolQuery();
				QueryBuilder subQb = null;
				QueryBuilder subFb = null;
				
				fillQuery(subBuilder, subFilter, subQb, subFb, subCriterias);
				
				if (filter.hasClauses()) {
					queryBuilder = QueryBuilders.boolQuery().must(subBuilder).must(subFilter);
				} else {
					queryBuilder = subBuilder;
				}
				
				HasParentQueryBuilder tmpqb = new HasParentQueryBuilder(criteria.key, queryBuilder , true);
				qb = tmpqb;
			}
			
			if(criteria.op == Criteria.operation.has_child) {
				
				QueryBuilder queryBuilder = null ;
				Criteria[] subCriterias = (Criteria[]) criteria.values;
				
				BoolQueryBuilder subBuilder = QueryBuilders.boolQuery();
				BoolQueryBuilder subFilter = QueryBuilders.boolQuery();
				QueryBuilder subQb = null;
				QueryBuilder subFb = null;
				
				fillQuery(subBuilder, subFilter, subQb, subFb, subCriterias);
				
				if (filter.hasClauses()) {
					queryBuilder = QueryBuilders.boolQuery().must(subBuilder).must(subFilter);
				} else {
					queryBuilder = subBuilder;
				}
				
				HasChildQueryBuilder tmpqb = new HasChildQueryBuilder(criteria.key, queryBuilder , ScoreMode.Total);
				qb = tmpqb;
			}
			// added by tdz at 2019-04-24 end
			
			
			if (criteria.op == Criteria.operation.querystring) {
				QueryStringQueryBuilder tmpQb = QueryBuilders.queryStringQuery(criteria.values[0].toString());
				if (criteria.key != null) {
					tmpQb.defaultField(criteria.key);
				}
				qb = tmpQb;
			}
			if (criteria.op == Criteria.operation.equal) {
				qb = QueryBuilders.termsQuery(criteria.key, criteria.values);
			}
			if (criteria.op == Criteria.operation.range) {
				qb = QueryBuilders.rangeQuery(criteria.key).includeLower(false).from(criteria.values[0])
						.to(criteria.values[1]);
			}
			if (criteria.op == Criteria.operation.between) {
				qb = QueryBuilders.rangeQuery(criteria.key).gte(criteria.values[0]).lte(criteria.values[1]);
			}
			if (criteria.op == Criteria.operation.gt) {
				qb = QueryBuilders.rangeQuery(criteria.key).gt(criteria.values[0]);
			}
			if (criteria.op == Criteria.operation.gte) {
				qb = QueryBuilders.rangeQuery(criteria.key).gte(criteria.values[0]);
			}
			if (criteria.op == Criteria.operation.lt) {
				qb = QueryBuilders.rangeQuery(criteria.key).lt(criteria.values[0]);
			}
			if (criteria.op == Criteria.operation.lte) {
				qb = QueryBuilders.rangeQuery(criteria.key).lte(criteria.values[0]);
			}
			if (criteria.op == Criteria.operation.fuzzy) {
				if (criteria.values.length > 2) {
					int fuzziness = (int) criteria.values[1];
					int maxExpansions = (int) criteria.values[2];

					qb = QueryBuilders.fuzzyQuery(criteria.key, criteria.values[0])
							.fuzziness(Fuzziness.fromEdits(fuzziness)).maxExpansions(maxExpansions);
					;
				} else {
					qb = QueryBuilders.fuzzyQuery(criteria.key, criteria.values[0]).fuzziness(Fuzziness.AUTO);
				}
			}
			if (qb != null) {
				if (criteria.boost > 1) {
					builder.must(qb).boost(criteria.boost);
				} else {
					builder.must(qb);
				}
				continue;
			}

			// Filter
			if (criteria.op == Criteria.operation.equalfilter) {

				fb = QueryBuilders.termsQuery(criteria.key, criteria.values);
				filter.must(fb);
			}
			if (criteria.op == Criteria.operation.rangefilter) {
				fb = QueryBuilders.rangeQuery(criteria.key).includeLower(false).from(criteria.values[0])
						.to(criteria.values[1]);
				filter.must(fb);
			}
			if (criteria.op == Criteria.operation.betweenfilter) {
				fb = QueryBuilders.rangeQuery(criteria.key).includeLower(true).includeUpper(true)
						.from(criteria.values[0]).to(criteria.values[1]);
				filter.must(fb);
			}
			if (criteria.op == Criteria.operation.wildcard) {
				for (Object match : criteria.values) {
					fb = QueryBuilders.termQuery(criteria.key, match.toString());
					filter.must(fb);
				}
			}
		}
	}

	private Map<String, Object> toMap(SearchHit hit, float maxScore, float findWordHitRatio) {
		Map<String, Object> map = new HashMap<>();

		if (!hit.getFields().isEmpty()) {
			Map<String, DocumentField> fields = hit.getFields();
			for (Entry<String, DocumentField> field : fields.entrySet()) {
				map.put(field.getKey(), field.getValue().getValues().get(0));
			}
			map.putAll(hit.getSourceAsMap());
		} else {
			Map fileds = hit.getFields();
			map = hit.getSourceAsMap();
			map.put("score", toPercent(hit.getScore() * 100 / maxScore * findWordHitRatio));
			replaceDateTimeAndHighlightField(hit);
		}

		map.put("_id", hit.getId());
		map.put("_type", hit.getType());
		map.put("_index", hit.getIndex());
		
		return map;
	}

	private void replaceDateTimeAndHighlightField(SearchHit hit) {
		String date, key, terms = "";
		Map<String, Object> row = hit.getSourceAsMap();

		Map<String, HighlightField> highlights = hit.getHighlightFields();

		for (Entry<String, Object> entry : row.entrySet()) {

			key = entry.getKey();

			if (entry.getValue() == null) {
				continue;
			}
			// 替换高亮显示样式
			if (highlights.containsKey(entry.getKey())) {
				row.put(entry.getKey(), concatText(hit.getHighlightFields().get(key).fragments()));
			}
			// 只保留年月日时分秒
			if (entry.getKey().contains("TIME_") && ((String) entry.getValue()).contains("T")) {
				date = ((String) entry.getValue()).replace('T', ' ').substring(0, 16);
				row.put(entry.getKey(), date);
			}
			// 如果是关键词，进行分词
			if (entry.getKey().equals("keywords")) {
				terms = (String) entry.getValue();
			}
		}
		if (!StringUtil.isNull(terms)) {
			row.put("terms", terms);
		}
	}

	private float computeHitRatioUseHighlightField(List<Criteria> criterias, SearchHit hit) {
		Object[] find;
		String match = "";

		int matchSize = 0, ratioLength = 0;

		Set<String> tokens = new HashSet<String>();

		Float ratio = 0f;

		for (Criteria criteria : criterias) {

			if (criteria.op == Criteria.operation.like) {

				find = criteria.values;

				for (Object word : find) {
					filterToken(tokens, word.toString());
				}

				if (hit.getHighlightFields().size() > 0) { // 待完善，需要去重！
					if (hit.getHighlightFields().containsKey("KEYWORDS")) {
						match = concatText(hit.getHighlightFields().get("KEYWORDS").getFragments());
						matchSize = match.split("</?em>").length - 1;
					}
				}

				ratio = Float.valueOf(matchSize) / Float.valueOf(tokens.size());
				ratioLength++;
			}
		}

		ratio = ratio / ratioLength;

		return (ratio > 1) ? 1 : ratio;
	}

	private void filterToken(Set<String> tokens, String temp) {
		for (int i = 0; i < temp.length(); i++) {
			// if (!TokenUtil.contains(temp.charAt(i))) {
			// tokens.add(String.valueOf(temp.charAt(i)));
			// }
		}
	}

	private String concatText(Text[] texts) {
		StringBuffer buf = new StringBuffer();

		for (Text text : texts) {
			buf.append(text.string()).append("...");
		}

		return buf.delete(buf.length() - 3, buf.length()).toString();
	}

	private String toPercent(float f) {
		if (Float.isNaN(f)) {
			return "0";
		}

		DecimalFormat df = new DecimalFormat("0.00");

		return df.format(f);
	}
}
