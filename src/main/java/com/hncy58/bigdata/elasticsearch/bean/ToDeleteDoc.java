package com.hncy58.bigdata.elasticsearch.bean;

/**
 * 待删除索引文档
 * 
 * @author luodongshan
 * @date 2018年6月30日 上午11:40:20
 *
 */
public class ToDeleteDoc {

	// 索引库
	private String index;
	
	// 文档所属于表
	private String table;
	
	// 文档id
	private String id;

	public ToDeleteDoc(String index, String table, String id) {
		this.index = index;
		this.table = table;
		this.id = id;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
