package com.adrich.bidlog.extract.build;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public class BuildBidApp {

	/**
	 * App 集合的主键、片键。
	 */
	public static final String BID_APP_PROPERTY_PK = "sspcode_appid";

	public static JSONObject bulidJSONObject(JSONObject logJsonObjct, String sspCode,long currentTime) throws Exception{
		Object isApp = logJsonObjct.get("isApp");
		if("IsPCMark".equals(isApp)) {
			return null;
		}
		JSONObject app = logJsonObjct.getJSONObject("app");

		Object name = app.get("name");
		Object appid = app.get("appid");
		Object market = app.get("market");
		Object app_category = app.get("app_category");
		Object appInteractionType = app.get("appInteractionType");
		Object tag = app.get("tag");
		if(ObjectUtil.isEmpty(appid)) {
//			System.err.println("appid is null : " + logJsonObjct);
			return null;
		}
		String sspcode_appid = appid + "_" + sspCode;

		// 不存在站点信息，新增站点信息
		JSONObject jobj = new JSONObject();
		jobj.put(BID_APP_PROPERTY_PK, sspcode_appid);
		jobj.put("name", name);
		jobj.put("appid", appid);
		jobj.put("market", market);
		jobj.put("app_category", app_category);
		jobj.put("appinteractiontype", appInteractionType);
		jobj.put("tag", tag);
		jobj.put("sspcode", sspCode);
		jobj.put(BidLogExtract.COMMON_COL_UPDATETIME, currentTime);
		return jobj;
	}
}
