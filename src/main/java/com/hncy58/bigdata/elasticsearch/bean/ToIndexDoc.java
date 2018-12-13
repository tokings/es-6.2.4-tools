package com.hncy58.bigdata.elasticsearch.bean;

import java.util.List;
import java.util.Map;

/**
 * 待索引的文档，文档的主键在文档的 map中
 * @author luodongshan
 * @date 2018年6月30日 下午2:48:12
 *
 */
public class ToIndexDoc {

	// 索引库
	private String index;

	// 文档所属于表
	private String table;

	// 需索引的文档列表
	private List<Map<String, Object>> docList;

	// 语句查询字段列表中的主键字段名称
	private String keyFieldName;

	// 主键字段是否需要单独进行索引
	private Boolean needIndexKeyField;

	public ToIndexDoc(String index, String table, List<Map<String, Object>> docList, String keyFieldName,
			Boolean needIndexKeyField) {
		super();
		this.index = index;
		this.table = table;
		this.docList = docList;
		this.keyFieldName = keyFieldName;
		this.needIndexKeyField = needIndexKeyField;
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

	public List<Map<String, Object>> getDocList() {
		return docList;
	}

	public void setDocList(List<Map<String, Object>> docList) {
		this.docList = docList;
	}

	public String getKeyFieldName() {
		return keyFieldName;
	}

	public void setKeyFieldName(String keyFieldName) {
		this.keyFieldName = keyFieldName;
	}

	public Boolean getNeedIndexKeyField() {
		return needIndexKeyField;
	}

	public void setNeedIndexKeyField(Boolean needIndexKeyField) {
		this.needIndexKeyField = needIndexKeyField;
	}

}
