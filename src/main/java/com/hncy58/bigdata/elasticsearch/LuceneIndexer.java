package com.hncy58.bigdata.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;

import com.hncy58.bigdata.elasticsearch.bean.ToDeleteDoc;
import com.hncy58.bigdata.elasticsearch.bean.ToIndexDoc;
import com.hncy58.bigdata.elasticsearch.metadata.Table;

/**
 * 全文索引接口
 * 
 * @author tdz
 * @date 2016年11月17日 下午4:08:57
 *
 */
public interface LuceneIndexer extends LuceneSearcher {

	/**
	 * 删除索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param id
	 *            索引文档id
	 * @throws SearchEngineException
	 */
	public void delete(String datasource, String table, String id) throws SearchEngineException;

	/**
	 * 批量删除索引记录
	 * 
	 * @param indice
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param list
	 *            索引文档id列表
	 * @throws SearchEngineException
	 */
	public void deleteAll(String indice, String table, List<String> list) throws SearchEngineException;

	/**
	 * 批量删除多个索引记录
	 * 
	 * @param list
	 *            待删除索引列表
	 * @throws SearchEngineException
	 */
	public void deleteAll(List<ToDeleteDoc> list) throws SearchEngineException;

	public void delete(String indice, String table) throws SearchEngineException;

	/**
	 * 根据查询条件删除索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param row
	 *            包含能准确筛选待删除记录的条件
	 * @throws SearchEngineException
	 */
	public void delete(String datasource, String table, Map<String, Object> row) throws SearchEngineException;

	/**
	 * 新增索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param row
	 *            索引文档内容
	 */
	public void index(String datasource, String table, Map<String, Object> row) throws SearchEngineException;

	/**
	 * 新增索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param id
	 *            索引文档id
	 * @param row
	 *            索引文档内容
	 */
	public void index(String datasource, String table, String id, Map<String, Object> row) throws SearchEngineException;

	/**
	 * 新增索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param id
	 *            索引文档id
	 * @param parent
	 *            父索引文档id
	 * @param row
	 *            索引文档内容
	 */
	public void index(String datasource, String table, String id, String parent, Map<String, Object> row)
			throws SearchEngineException;

	/**
	 * 更新索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param id
	 *            索引文档id
	 * @param row
	 *            索引文档内容
	 * @param routeValue
	 *            路由值
	 */
	public void index(String datasource, String table, Map<String, Object> row, String routeValue)
			throws SearchEngineException;

	public void index(String indice, String table, Map<String, Object> row, boolean batchCommit)
			throws SearchEngineException;

	/**
	 * 更新索引记录
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param SQL
	 *            查询索引记录行的语句
	 */
	public void index(String datasource, String table, String SQL) throws SearchEngineException;

	/**
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @param datasource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param SQL
	 *            查询语句
	 * @param keyFieldName
	 *            语句查询字段列表中的主键字段名称
	 * @param dsName
	 *            数据源
	 * @param parameters
	 *            查询语句的参数
	 * @throws SearchEngineException
	 */
	public void indexAll(String datasource, String table, String SQL, String keyFieldName, String dsName,
			Object... parameters) throws SearchEngineException;

	public void indexAllWithRowMapper(String db, String table, String SQL, String keyFieldName, String dsName,
			Object... parameters) throws SearchEngineException;

	/**
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @param essource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param list
	 *            需创建的索引
	 * @throws SearchEngineException
	 */
	public void indexAll(String essource, String table, List<Map<String, Object>> list, String keyFieldName)
			throws SearchEngineException;

	/**
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @param essource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param list
	 *            需索引的文档列表
	 * @param keyFieldName
	 *            语句查询字段列表中的主键字段名称
	 * @param needIndexKeyField
	 *            主键字段是否需要单独进行索引
	 * @throws SearchEngineException
	 */
	public void indexAll(String essource, String table, List<Map<String, Object>> list, String keyFieldName,
			Boolean needIndexKeyField) throws SearchEngineException;

	/**
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @author luodongshan
	 * @date 2018年6月30日 下午2:51:38
	 * @param list
	 *            需索引的文档列表
	 * @throws SearchEngineException
	 */
	public void indexAll(List<ToIndexDoc> list) throws SearchEngineException;

	/**
	 * 批量更新索引
	 * 
	 * @date 2018年12月5日 下午4:03:50
	 * @param list
	 * @throws SearchEngineException
	 */
	public void updateAll(List<ToIndexDoc> list) throws SearchEngineException;

	/**
	 * 
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @param essource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param lsit
	 *            需创建的索引
	 * @param keyFieldName
	 *            主键列
	 * @param routeFieldName
	 *            路由列
	 * @throws SearchEngineException
	 */
	public void indexAll(String essource, String table, List<Map<String, Object>> list, String keyFieldName,
			String routeFieldName) throws SearchEngineException;

	/**
	 * 
	 * 批量创建索引，结果集按指定条数循环提交
	 * 
	 * @param essource
	 *            索引库
	 * @param table
	 *            文档所属表
	 * @param lsit
	 *            需创建的索引
	 * @param keyFieldName
	 *            主键列
	 * @param parentFieldName
	 *            父文档主键列
	 * @throws SearchEngineException
	 */
	public void indexAll(String essource, String table, String keyFieldName, String parentFieldName,
			List<Map<String, Object>> list) throws SearchEngineException;

	/**
	 * 加载索引表的配置项
	 * 
	 * @param indice
	 *            索引库
	 * @param type
	 *            索引表
	 * @param table
	 *            表配置对象
	 * @throws SearchEngineException
	 */
	public void loadSchema(String indice, String type, Table table) throws IOException, SearchEngineException;

	/**
	 * 返回搜索引擎实现的服务器类型（可选：ElasticSearch）
	 * 
	 * @return 引擎类型
	 */
	public String type();

	/**
	 * 查询搜索引擎客户端连接状态，客户端正常启动连接服务器之后，置为 TRUE。尚未实现 连接状态 检测。
	 * 
	 * @return 连接状态
	 */
	public boolean isRunning();

	/**
	 * 停在索引服务，关闭客户端。暂未用。
	 */
	public void stop();

	/**
	 * 获取客户端实例
	 * 
	 * @return
	 */
	public Client getClient() throws SearchEngineException;

	void indexAll(String indice, String table, List<Map<String, Object>> datas) throws SearchEngineException;

}
