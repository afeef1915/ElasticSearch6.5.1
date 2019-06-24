
package com.javacodegeeks.example;

/*
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import com.fasterxml.jackson.databind.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
//import static org.elasticsearch.index.query.QueryBuilders.*;

import org.apache.http.HttpEntity;
import org.elasticsearch.search.*;
//import org.elasticsearch.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.transport.TransportMessage;
impot org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexResponse;

import org.elasticsearch.client.ml.PostDataRequest;
*/
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.apache.http.HttpHost;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpEntity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RethrottleRequest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;

public class Application {

	
    
	// The config parameters for the connection
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final int PORT_TWO = 9201;
	private static final String SCHEME = "http";

	private static RestHighLevelClient restHighLevelClient;
	// private static RestClient restClient;
	private static ObjectMapper objectMapper = new ObjectMapper();

	private static final String INDEX = "persondata";
	private static final String TYPE = "person";

	/**
	 * Implemented Singleton pattern here so that there is just one connection at a
	 * time.
	 * 
	 * @return RestHighLevelClient
	 */
	private static synchronized RestHighLevelClient makeConnection() {

		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), new HttpHost(HOST, PORT_TWO, SCHEME)));
		}

		return restHighLevelClient;
	}

	private static synchronized void closeConnection() throws IOException {
		restHighLevelClient.close();
		restHighLevelClient = null;
	}

	/*
	 * private static synchronized void closeConnection() throws IOException {
	 * restHighLevelClient.close(); restHighLevelClient = null; }
	 */
	private static Person insertPerson(Person person) {
		person.setPersonId(UUID.randomUUID().toString());
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("personId", person.getPersonId());
		dataMap.put("name", person.getName());
		IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, person.getPersonId()).source(dataMap);
		//System.out.println("request");
		//System.out.println(indexRequest);
		try {
			
			//IndexResponse response = restHighLevelClient.index(indexRequest);
				IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
				//System.out.println("response");
				//System.out.println(response);
				
		} catch (ElasticsearchException e) {
			e.getDetailedMessage();
		} catch (java.io.IOException ex) {
			ex.getLocalizedMessage();
		}
		return person;
	}

	
	private static Person getPersonById(String id) {
		
		//System.out.println(id);
		GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
		GetResponse getResponse = null;
		//System.out.println(getPersonRequest);
		try {
			//System.out.println(getPersonRequest);
			//System.out.println("try block");
			//getPersonRequest.storedFields("message"); 
			//getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
			//String message = getResponse.getField("name").getValue();
			
			getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
			String message = getResponse.getSourceAsString();
			 //getResponse = restHighLevelClient.get(getPersonRequest);
			System.out.println("test the message");
			//System.out.println(getResponse);
			System.out.println(message);
			//System.out.println(getPersonRequest);
			
			
			
			//getPersonRequest.routing("routing");
			//getPersonRequest.parent("parent"); 
			//getPersonRequest.preference("preference");
			
			//getResponse = restHighLevelClient.get(getPersonRequest);
			
			
			
		} catch (java.io.IOException e) {
			//System.out.println("error issue");
			//System.out.println(e.getMessage()); 
			//System.out.println(e.getLocalizedMessage());
			e.getLocalizedMessage();
		}
		//System.out.println(getResponse);
		//System.out.println("getting new Response");
		
		//System.out.println(getResponse);
		return getResponse != null ? objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;

	}

	private static Person updatePersonById(String id, Person person){
	    UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
	            .fetchSource(true);    // Fetch Object after its update
	    try {
	        String personJson = objectMapper.writeValueAsString(person);
	        updateRequest.doc(personJson, XContentType.JSON);
	        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
	        return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Person.class);
	    }catch (JsonProcessingException e){
	        e.getMessage();
	    } catch (java.io.IOException e){
	        e.getLocalizedMessage();
	    }
	    System.out.println("Unable to update person");
	    return null;
	}
	

	private static void deletePersonById(String id) {
		DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
		try {
			DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
		} catch (java.io.IOException e) {
			e.getLocalizedMessage();
		}
	}

	public static void main(String[] args) throws IOException {

		Logger logger = LoggerFactory.getLogger(Application.class);
		logger.info("This is how you configure Java Logging with SLF4J");

		makeConnection();

		System.out.println("Inserting a new Person with name Afeef...");
		Person person = new Person();
		person.setName("Afeef");
		person = insertPerson(person);
		System.out.println("Person inserted --> " + person);

		System.out.println("Getting Afeef...");
		//System.out.println(person.getPersonId());
		Person personFromDB = getPersonById(person.getPersonId());
		System.out.println("Person from DB  --> " + personFromDB);
		
		
		System.out.println("Changing name to `Mohd Afeef`...");
		person.setName("Mohd Afeef");
		updatePersonById(person.getPersonId(), person);
		System.out.println("Person updated  --> " + person);
		

		System.out.println("Deleting Afeef...");
		deletePersonById(personFromDB.getPersonId());
		System.out.println("Person Deleted");
		

		closeConnection();
	}
}
