package com.hncy58.bigdata.elasticsearch;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * 自定义查询转换器
 * @author tdz
 * @date 2016年11月17日 下午4:10:34
 *
 */
public class QueryToEsQuery {
	
	public static QueryBuilder transfer(Query query, BoolQueryBuilder builder)
	{
		QueryBuilder qb = null;
		//FilterBuilder fb = null;
		
		// 查询条件
		if (query.isSearchAll()) {
			qb = QueryBuilders.matchAllQuery();
			builder.must(qb);
		} else {
			for (Criteria criteria: query.getCriterias()) {	
				qb = null;
				
				// Query
				if (criteria.op == Criteria.operation.like) {
					qb = QueryBuilders.queryStringQuery(criteria.values[0].toString()).defaultField(criteria.key);
				}
				if (criteria.op == Criteria.operation.wildcard) {
					BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
					for (Object match: criteria.values) {
						qb = QueryBuilders.wildcardQuery(criteria.key, match.toString());
						subQuery.should(qb);
					}
					builder.must(subQuery);
					continue;
				}
				if (criteria.op == Criteria.operation.equal) {
					qb = QueryBuilders.termsQuery(criteria.key, criteria.values);
				}				
				if (criteria.op == Criteria.operation.notequal) {
					qb = QueryBuilders.termsQuery(criteria.key, criteria.values);
					builder.mustNot(qb);
					continue;
				}
				
				// add by lds 20180827 existsquery 与 notexistsquery，搜索中是否存在某个字段
				if(criteria.op == Criteria.operation.existsquery) {
					builder.must(QueryBuilders.existsQuery(criteria.key));
					continue;
				}
				if(criteria.op == Criteria.operation.notexistsquery) {
					builder.mustNot(QueryBuilders.existsQuery(criteria.key));
					continue;
				}
				
				
				if (criteria.op == Criteria.operation.range) {
					qb = QueryBuilders.rangeQuery(criteria.key).includeLower(false)
							.from(criteria.values[0]).to(criteria.values[1]);
				}
				if (criteria.op == Criteria.operation.between) {
					qb = QueryBuilders.rangeQuery(criteria.key).gte(criteria.values[0]).lte(criteria.values[1]);
				}
				if (criteria.op == Criteria.operation.gt) {
					qb = QueryBuilders.rangeQuery(criteria.key).gt(criteria.values);
				}
				if (criteria.op == Criteria.operation.gte) {
					qb = QueryBuilders.rangeQuery(criteria.key).gte(criteria.values);
				}
				if (criteria.op == Criteria.operation.lt) {
					qb = QueryBuilders.rangeQuery(criteria.key).lt(criteria.values);
				}
				if (criteria.op == Criteria.operation.lte) {
					qb = QueryBuilders.rangeQuery(criteria.key).lte(criteria.values);
				}
				if (criteria.op == Criteria.operation.fuzzy) {
					if (criteria.values.length > 2) { 
						int fuzziness = (int) criteria.values[1];
						int maxExpansions = (int) criteria.values[2];
						
						qb = QueryBuilders.fuzzyQuery(criteria.key, criteria.values[0])
								.fuzziness(Fuzziness.fromEdits(fuzziness)).maxExpansions(maxExpansions);;
					} else {
						qb = QueryBuilders.fuzzyQuery(criteria.key, criteria.values[0])
								.fuzziness(Fuzziness.AUTO);
					}
				}
				if (criteria.op == Criteria.operation.rangefilter) {
					qb = QueryBuilders.rangeQuery(criteria.key).includeLower(false)
					.from(criteria.values[0]).to(criteria.values[1]);
				}
				if (criteria.op == Criteria.operation.equalfilter) {
					qb = QueryBuilders.termsQuery(criteria.key, criteria.values);
				}
				
				if (qb != null) {
					if (criteria.boost > 1) {
						builder.must(qb).boost(criteria.boost);
					} else {
						builder.must(qb);
					}
					continue;
				}			
			}			
		}
		return qb;
	}
	
}
