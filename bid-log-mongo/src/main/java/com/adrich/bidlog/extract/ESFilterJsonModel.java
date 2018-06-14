package com.adrich.bidlog.extract;

import com.alibaba.fastjson.JSONObject;

public class ESFilterJsonModel {

	private CollectionType.Item collection;
	private String cacheIndexName = null;
	private String cacheIndexKey = null;
	private JSONObject json = null;
	
	public ESFilterJsonModel(CollectionType.Item collection){
		this.collection = collection;
	}
	public String getCacheIndexName() {
		return cacheIndexName;
	}
	public void setCacheIndexName(String cacheIndexName) {
		this.cacheIndexName = cacheIndexName;
	}
	public String getCacheIndexKey() {
		return cacheIndexKey;
	}
	public void setCacheIndexKey(String cacheIndexKey) {
		this.cacheIndexKey = cacheIndexKey;
	}
	public JSONObject getJson() {
		return json;
	}
	public void setJson(JSONObject json) {
		this.json = json;
	}
	public CollectionType.Item getCollection() {
		return collection;
	}
	public void setCollection(CollectionType.Item collection) {
		this.collection = collection;
	}
	
}
