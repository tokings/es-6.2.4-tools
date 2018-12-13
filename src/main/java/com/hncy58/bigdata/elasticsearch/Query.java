package com.hncy58.bigdata.elasticsearch;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.hncy58.bigdata.elasticsearch.Criteria.operation;
import com.hncy58.bigdata.elasticsearch.util.DateUtil;


/**
 * 搜索表达式对象
 * @author tdz
 * @date 2016年11月17日 下午4:08:31
 *
 */
public class Query
{
	
	/** 查询页码 */
	private int pageNo;
	
	/** 返回记录数*/
	private int pageSize;
	
	/** 排序字段*/
	private List<Sort> sorts = new ArrayList<Sort>();
	
	/** 是否高亮 */
	private Set<String> highlights = new HashSet<String>();
	
	/** 过滤字段列表 */
	private String[] fields;
	
//	private StringBuffer route = new StringBuffer();
	private List<String> route = new ArrayList<String>();
	
	/** 查条件 */
	private List<Criteria> criterias = new ArrayList<Criteria>(); 
	
	/** 查询超时 */
	private long timeout = 3000;
	
	/** RDD 查询的索引字段 */
//	private String[] rddIndexFields = new String[]{};
	
	/** 查询类型 */
	private SearchType searchType = SearchType.DFS_QUERY_THEN_FETCH;
	
	/** rdd查询类型 */
	private String rddSparkData;
	
	/** rdd查询字段 */
	private String rddSparkField;
	
	private Map<String, AbstractAggregationBuilder> aggregations = new HashMap<String, AbstractAggregationBuilder>();
	
	/**
	 * 构造方法	
	 * @param pageNo 	页码
	 * @param pageSize	每页显示行数
	 */
	public Query(int pageNo, int pageSize)
	{
		this.pageNo = pageNo;
		this.pageSize = pageSize;
	}
	
	/**
	 * 构造方法	
	 * @param pageNo 	页码
	 * @param pageSize	每页显示行数
	 * @param fields	返回字段列表。该参数不指定时，返回所有索引表中的存储字段
	 */
	public Query(int pageNo, int pageSize, String[] fields)
	{
		this.pageNo = pageNo;
		this.pageSize = pageSize;
		this.fields = fields;
	}
	
	/**
	 * 添加等于判断的字段条件
	 * @param key	字段名
	 * @param value	字段值
	 */
	public void addEqualCriteria(String key, Object... value)
	{
		criterias.add(new Criteria(operation.equal, key, value));
	}
	
	public void addEqualCriteria(Map<String, Object> pairs)
	{
		for (Entry<String, Object> entry: pairs.entrySet()) {
			criterias.add(new Criteria(operation.equal, entry.getKey(), entry.getValue()));
		}
	}
	
	/**
	 * 添加不等于判断的字段条件
	 * @param key	字段名
	 * @param value	字段值
	 */
	public void addNotEqualCriteria(String key, Object... value)
	{
		criterias.add(new Criteria(operation.notequal, key, value));
	}
	
	public void addNotEqualCriteria(Map<String, Object> pairs)
	{
		for (Entry<String, Object> entry: pairs.entrySet()) {
			criterias.add(new Criteria(operation.notequal, entry.getKey(), entry.getValue()));
		}
	}
	
	/**
	 * 添加模糊匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值
	 */
	public void addLikeCriteria(String key, Object value)
	{
		criterias.add(new Criteria(operation.like, key, value));
	}
	
	public void addLikeCriteria(String key, float boost, Object... value)
	{
		criterias.add(new Criteria(operation.like,boost,key,value));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addRangeCriteria(String key, long from, long to)
	{
		criterias.add(new Criteria(operation.range, key, from, to));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addGtCriteria(String key, long from)
	{
		criterias.add(new Criteria(operation.gt, key, from));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addGtCriteria(String key, Object from)
	{
		criterias.add(new Criteria(operation.gt, key, from));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addGteCriteria(String key, long from)
	{
		criterias.add(new Criteria(operation.gte, key, from));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addGteCriteria(String key, Object from)
	{
		criterias.add(new Criteria(operation.gte, key, from));
	}

	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addLtCriteria(String key, long to)
	{
		criterias.add(new Criteria(operation.lt, key, to));
	}

	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addLtCriteria(String key, Object to)
	{
		criterias.add(new Criteria(operation.lt, key, to));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addLteCriteria(String key, long to)
	{
		criterias.add(new Criteria(operation.lte, key, to));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addLteCriteria(String key, Object to)
	{
		criterias.add(new Criteria(operation.lte, key, to));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addRangeCriteria(String key, Date from, Date to)
	{
		String sfrom = DateUtil.dateToString(from).replace(' ', 'T');
		String sto = DateUtil.dateToString(to).replace(' ', 'T');
		
		criterias.add(new Criteria(operation.range, key, sfrom, sto));
	}
	
	public void addBetweenCriteria(String key, Date from, Date to)
	{
		String sfrom = DateUtil.dateToString(from).replace(' ', 'T');
		String sto = DateUtil.dateToString(to).replace(' ', 'T');
		
		criterias.add(new Criteria(operation.between, key, sfrom, sto));
	}
	
	public void addBetweenCriteria(String key, Float from, Float to)
	{
		criterias.add(new Criteria(operation.between, key, from, to));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addRangeCriteria(String key, String from, String to)
	{
		criterias.add(new Criteria(operation.range, key, from, to));
	}

	public void addBetweenCriteria(String key, long from, long to)
	{
		criterias.add(new Criteria(operation.between, key, from, to));
	}
	
	public void addBetweenCriteria(String key, String from, String to)
	{
		criterias.add(new Criteria(operation.between, key, from, to));
	}
	
	/**
	 * 搜索中存在指定字段
	 * @date 20180827 by lds
	 * @param key 字段名
	 */
	public void addExistsQueryCriteria(String key)
	{		
		criterias.add(new Criteria(operation.existsquery, key, (Object[])null));
	}
	
	/**
	 * 搜索中不存在指定字段
	 * @date 20180827 by lds
	 * @param key 字段名
	 */
	public void addNotExistsQueryCriteria(String key)
	{		
		criterias.add(new Criteria(operation.notexistsquery, key, (Object[])null));
	}
	
	/**
	 * 添加区间范围匹配的字段条件
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addWildcardCriteria(String key, Object... value)
	{		
		criterias.add(new Criteria(operation.wildcard, key, value));
	}
	
	/**
	 * 添加区间范围匹配的字段条件(多条件匹配)
	 * @param key	字段名
	 * @param value	字段值列表
	 */
	public void addQueryString(Object value)
	{		
		criterias.add(new Criteria(operation.querystring, value));
	}
	
	/**
	 * 在指定的字段名里搜索
	 * @param fieldName	字段名
	 * @param value	字段值列表
	 */
	public void addQueryString(String fieldName, Object value)
	{		
		criterias.add(new Criteria(operation.querystring, fieldName, value));
	}
	
	/**
	 * 模糊查询
	 * @param key	字段名
	 * @param value	字段值
	 */
	public void addFuzzyCriteria(String key, Object... value)
	{		
		criterias.add(new Criteria(operation.fuzzy, key, value));
	}
	
	/**
	 * 添加表达式查询条件
	 * @param parseQuery表达式
	 * @param defaultField	默认字段名
	 */
	public void addParseCriteria(String parseQuery,String defaultField)
	{
		criterias.add(new Criteria(operation.parse, defaultField, parseQuery));
	}
	
	/**
	 * 添加等于过滤
	 * @param key
	 * @param value
	 */
	public void addEqualFilter(String key, Object... value)
	{
		criterias.add(new Criteria(operation.equalfilter, key, value));
	}
	
	/**
	 * 添加范围过滤
	 * @param key
	 * @param values
	 */
	public void addRangeFilter(String key, Object... values)
	{
		criterias.add(new Criteria(operation.rangefilter, key, values));
	}
	
	/**
	 * 添加通配符过滤
	 * @param key
	 * @param vlaues
	 */
	public void addWildcardFilter(String key, Object... values)
	{
		criterias.add(new Criteria(operation.wildcardfilter, key, values));
	}
	
	/**
	 * 添加查询排序
	 * @param field	排序字段
	 * @param order	排序方式，可选值：desc|asc，大小写不区分
	 */
	public void addSort(String field, SortOrder order)
	{
		sorts.add(new Sort(field, order));
	}
	
	/**
	 * 添加查询排序
	 * @param field	排序字段
	 * @param order	排序方式，可选值：desc|asc，大小写不区分
	 */
	public void addSort(String field, String order)
	{
		sorts.add(new Sort(field, order));
	}
	
	/**
	 * 是否无条件查询（即：条件列表为空的情况）
	 * @return
	 */
	public boolean isSearchAll()
	{
		return criterias.isEmpty();
	}	
	
	/**
	 * 设置需要高亮显示的字段
	 * @param highlightField
	 */
	public void addHighlight(String... highlightField)
	{
		for (String field: highlightField) {
			this.highlights.add(field);
	}}
	
	public void setPager(int pageNo, int pageSize)
	{
		this.pageNo = pageNo;
		this.pageSize = pageSize;
	}
	
//	public void setRddIndexFields(String rddIndexFields)
//	{
//		this.rddIndexFields = rddIndexFields.split(",");
//	}
//	
//	public String[] getRddIndexFields()
//	{
//		return this.rddIndexFields;
//	}
	
	public Map<String, AbstractAggregationBuilder> getAggregations() {
		return aggregations;
	}

	public void addAggregation(String aggregation, AbstractAggregationBuilder agg) {
		this.aggregations.put(aggregation, agg);
	}
	
	public void cleanAggregations() {
		this.aggregations.clear();
	}
	
	public void cleanCriterias() {
		this.criterias.clear();
	}

	public List<Criteria> getCriterias()
	{
		return this.criterias;
	}
	
	public List<Sort> getSort()
	{
		return this.sorts;
	}
	
	public void cleanSorts() {
		this.sorts.clear();
	}
	
	public int getPageNo()
	{
		return pageNo;
	}

	public int getPageSize()
	{
		return pageSize;
	}
	
	public void setOutPutFields(String[] fields) {
		this.fields = fields;
	}
	
	public String[] getOutputFields()
	{
		return this.fields;
	}

	public Set<String> getHighlightFields()
	{
		return this.highlights;
	}
	
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}

	public void addRoute(List<String> r)
	{
		this.route.addAll(r);
	}
	
	public List<String> getRoute()
	{
		return this.route;
	}
	
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}
	
	public long getTimeout()
	{
		return this.timeout;
	}
	
	public SearchType getSearchType() 
	{
		return this.searchType;
	}

	public void setSearchType(SearchType searchType)
	{
		this.searchType = searchType;
	}

	public String getRddSparkData() {
		return rddSparkData;
	}

	public void setRddSparkData(String rddSparkData) {
		this.rddSparkData = rddSparkData;
	}

	public String getRddSparkField() {
		return rddSparkField;
	}

	public void setRddSparkField(String rddSparkField) {
		this.rddSparkField = rddSparkField;
	}


}
