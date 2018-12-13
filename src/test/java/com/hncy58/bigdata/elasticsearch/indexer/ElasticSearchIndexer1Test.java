package com.hncy58.bigdata.elasticsearch.indexer;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.hncy58.bigdata.elasticsearch.bean.ToDeleteDoc;
import com.hncy58.bigdata.elasticsearch.bean.ToIndexDoc;

public class ElasticSearchIndexer1Test {

	@Test
	public void testElasticSearchIndexer() {
		fail("Not yet implemented");
	}

	@Test
	public void testElasticSearchIndexerTransportClient() {
		fail("Not yet implemented");
	}

	@Test
	public void testElasticSearchIndexerStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetClient() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteAllStringStringListOfString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteAllListOfToDeleteDoc() {
		
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();
		
		List<ToDeleteDoc> toDeleteDocList = new ArrayList<ToDeleteDoc>();
		for(int j = 0; j < 10; j++) {
			
			String db = "lds_index_list" + j;
			String table = "table";
			table = "table";
	
			for(int i = 0; i < 9; i++) {
				ToDeleteDoc toDeleteDoc = new ToDeleteDoc(db, table, "2HbNi1rCKaEhSv0w148RrK" + i);
				toDeleteDocList.add(toDeleteDoc);
			}
		}
		
		try {
			elasticSearchIndexer.deleteAll(toDeleteDocList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeleteStringStringMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexStringStringMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexStringStringStringMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testQuery() {
		fail("Not yet implemented");
	}

	@Test
	public void testQueryStatistics() {
		fail("Not yet implemented");
	}

	@Test
	public void testTransfer() {
		fail("Not yet implemented");
	}

	@Test
	public void testLoadSchema() {
		fail("Not yet implemented");
	}

	@Test
	public void testStop() {
		fail("Not yet implemented");
	}

	@Test
	public void testType() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllStringStringListOfMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllStringStringListOfMapOfStringObjectString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllStringStringListOfMapOfStringObjectStringBoolean() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllListOfToIndexDoc() {
		ElasticSearchIndexer elasticSearchIndexer = new ElasticSearchIndexer();

		List<ToIndexDoc> toIndexDocList = new ArrayList<ToIndexDoc>();
		for(int j = 1; j < 2; j++) {
				
			String db = "lds_index_list" + j;
			String table = "table";
			table = "table";
	
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for(int i = 0; i < 10; i++) {
				Map<String, Object> row = new HashMap<String, Object>();
				row.put("cert_id2", "k430524198511242234" + "ldss" + i);
				row.put("id", "2HbNi1rCKaEhSv0w148RrK" + i);	
				row.put("name", "2HbNi1rCKaEhSv0w148RrK" + i);	
				list.add(row);
			}
			ToIndexDoc toIndexDoc = new ToIndexDoc(db, table, list, "id", true);
			toIndexDocList.add(toIndexDoc);
		}
		
		try {
			elasticSearchIndexer.updateAll(toIndexDocList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIndexStringStringMapOfStringObjectBoolean() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexStringStringMapOfStringObjectString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllStringStringListOfMapOfStringObjectStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllStringStringStringStringStringObjectArray() {
		fail("Not yet implemented");
	}

	@Test
	public void testIndexAllWithRowMapper() {
		fail("Not yet implemented");
	}

	@Test
	public void testUpdateStringStringStringMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testUpdateStringStringStringListOfMapOfStringObject() {
		fail("Not yet implemented");
	}

}
