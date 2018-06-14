package com.adrich.bidlog.extract;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

public class LogJsonModel {

	private boolean stop = false;
	private JSONObject imp = new JSONObject();
	private List<JSONObject> adzoneList = new ArrayList<JSONObject>();
	private JSONObject app = new JSONObject();
	private JSONObject site = new JSONObject();
	private JSONObject ssp = new JSONObject();
	private JSONObject dsp = new JSONObject();
	public boolean isStop() {
		return stop;
	}
	public void setStop(boolean stop) {
		this.stop = stop;
	}
	public JSONObject getImp() {
		return imp;
	}
	public void setImp(JSONObject imp) {
		this.imp = imp;
	}
	public JSONObject getApp() {
		return app;
	}
	public void setApp(JSONObject app) {
		this.app = app;
	}
	public JSONObject getSite() {
		return site;
	}
	public void setSite(JSONObject site) {
		this.site = site;
	}
	public JSONObject getSsp() {
		return ssp;
	}
	public void setSsp(JSONObject ssp) {
		this.ssp = ssp;
	}
	public JSONObject getDsp() {
		return dsp;
	}
	public void setDsp(JSONObject dsp) {
		this.dsp = dsp;
	}
	public List<JSONObject> getAdzoneList() {
		return adzoneList;
	}
	public void setAdzoneList(List<JSONObject> adzoneList) {
		this.adzoneList = adzoneList;
	}
	
}
