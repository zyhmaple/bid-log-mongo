package com.adrich.bidlog.extract;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

public class ESJSONModel {

	private List<JSONObject> impList = new ArrayList<>();
	private List<ESFilterJsonModel> adzoneList = new ArrayList<>();
	private List<ESFilterJsonModel> appList = new ArrayList<>();
	private List<ESFilterJsonModel> siteList = new ArrayList<>();
	private List<ESFilterJsonModel> sspList = new ArrayList<>();
	private List<ESFilterJsonModel> dspList = new ArrayList<>();
	private String recordTime = "";
	public List<JSONObject> getImpList() {
		return impList;
	}
	public void setImpList(List<JSONObject> impList) {
		this.impList = impList;
	}
	public List<ESFilterJsonModel> getAdzoneList() {
		return adzoneList;
	}
	public void setAdzoneList(List<ESFilterJsonModel> adzoneList) {
		this.adzoneList = adzoneList;
	}
	public List<ESFilterJsonModel> getAppList() {
		return appList;
	}
	public void setAppList(List<ESFilterJsonModel> appList) {
		this.appList = appList;
	}
	public List<ESFilterJsonModel> getSiteList() {
		return siteList;
	}
	public void setSiteList(List<ESFilterJsonModel> siteList) {
		this.siteList = siteList;
	}
	public List<ESFilterJsonModel> getSspList() {
		return sspList;
	}
	public void setSspList(List<ESFilterJsonModel> sspList) {
		this.sspList = sspList;
	}
	public List<ESFilterJsonModel> getDspList() {
		return dspList;
	}
	public void setDspList(List<ESFilterJsonModel> dspList) {
		this.dspList = dspList;
	}
	public String getRecordTime() {
		return recordTime;
	}
	public void setRecordTime(String recordTime) {
		this.recordTime = recordTime;
	}
}
