package com.adrich.bidlog.model;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

public class BuildLogModel {
	private boolean stop = false;
	private List<JSONObject> impList = new ArrayList<JSONObject>();
	private List<JSONObject> adzoneList = new ArrayList<JSONObject>();
	private List<JSONObject> appList = new ArrayList<JSONObject>();
	private List<JSONObject> siteList = new ArrayList<JSONObject>();
	private List<JSONObject> sspList = new ArrayList<JSONObject>();
	private List<JSONObject> dspList = new ArrayList<JSONObject>();
	/**
	 * 						sspAddList
	 * 		  			  /
	 * sspList	->	bloom
	 * 		  			  \
	 * 						sspUpdateList
	 */
	private List<JSONObject> sspAddList = new ArrayList<JSONObject>();
	private List<JSONObject> sspUpdateList = new ArrayList<JSONObject>();
	
	private String recordTime = "";
	
	public List<JSONObject> getImpList() {
		return impList;
	}
	public void addImp(JSONObject imp) {
		if(imp!=null){
			this.impList.add(imp);
		}
	}
	public void addImpList(List<JSONObject> impList) {
		this.impList.addAll(impList);
	}
	public List<JSONObject> getAdzoneList() {
		return adzoneList;
	}
	public void addAdzoneList(List<JSONObject> adzoneList) {
		this.adzoneList.addAll(adzoneList);
	}
	public List<JSONObject> getAppList() {
		return appList;
	}
	public void addApp(JSONObject app) {
		if(app!=null){
			this.appList.add(app);
		}
	}
	public List<JSONObject> getSiteList() {
		return siteList;
	}
	public void addSite(JSONObject site) {
		if(site!=null){
			this.siteList.add(site);
		}
	}
	public List<JSONObject> getSspList() {
		return sspList;
	}
	public void addSsp(JSONObject ssp) {
		if(ssp!=null){
			this.sspList.add(ssp);
		}
	}
	public List<JSONObject> getDspList() {
		return dspList;
	}
	public void addDsp(JSONObject dsp) {
		if(dsp!=null){
			this.dspList.add(dsp);
		}
	}
	public boolean isStop() {
		return stop;
	}
	public void setStop(boolean stop) {
		this.stop = stop;
	}
	public void setAdzoneList(List<JSONObject> adzoneList) {
		this.adzoneList = adzoneList;
	}
	public void setAppList(List<JSONObject> appList) {
		this.appList = appList;
	}
	public void setSiteList(List<JSONObject> siteList) {
		this.siteList = siteList;
	}
	public void setSspList(List<JSONObject> sspList) {
		this.sspList = sspList;
	}
	public void setDspList(List<JSONObject> dspList) {
		this.dspList = dspList;
	}
	public String getRecordTime() {
		return recordTime;
	}
	public void setRecordTime(String recordTime) {
		this.recordTime = recordTime;
	}
	public List<JSONObject> getSspAddList() {
		return sspAddList;
	}
	public void setSspAddList(List<JSONObject> sspAddList) {
		this.sspAddList = sspAddList;
	}
	public List<JSONObject> getSspUpdateList() {
		return sspUpdateList;
	}
	public void setSspUpdateList(List<JSONObject> sspUpdateList) {
		this.sspUpdateList = sspUpdateList;
	}
}
