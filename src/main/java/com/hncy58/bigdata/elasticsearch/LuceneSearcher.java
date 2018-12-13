package com.hncy58.bigdata.elasticsearch;

import java.io.IOException;
import java.util.Map;

/**
 * 全文索引检索接口，支持分页。PageQueryResult 接口与 DB 查询的 DataProvider 一致。
 * @author tdz
 * @date 2016年11月17日 下午4:08:48
 *
 */
public interface LuceneSearcher 
{
		
	/**
	 * 搜索服务
	 * @param datasource 	索引库
	 * @param table			索引表
	 * @param query			查询条件
	 * @return
	 * @throws SearchEngineException
	 * @throws IOException 
	 */
	public PageQueryResult query(String[] datasources, String table, Query query) throws SearchEngineException;

	/**
	 * 搜索服务，用于统计
	 * @param datasource 	索引库
	 * @param table			索引表
	 * @param query			查询条件
	 * @return
	 * @throws SearchEngineException
	 * @throws IOException 
	 */
	public Map<Object, Object> queryStatistics(String[] datasources, Query query, String... table) throws SearchEngineException;

}
