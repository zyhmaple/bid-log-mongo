package com.adrich.bidlog.extract.build;

import java.util.List;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.util.EsIdUtil;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public class BuildBidImp {

	public static final String BID_IMP_PROPERTY_PK = "sspcode_impid";
//	public static final String BID_IMP_PROPERTY_PK_SOLR = "id";
	public static final String BID_IMP_PROPERTY_SSPCODE = "sspcode";
	
	@SuppressWarnings("unchecked")
	public static JSONObject bulidJSONObject(JSONObject logJsonObjct,JSONObject bidSite, JSONObject bidApp,List<JSONObject> bidAdzoneList,JSONObject userSsp,long currentTime) throws Exception{
		List<JSONObject> impList = logJsonObjct.getObject("imp", List.class);
		if(impList == null || impList.size()==0) {
			return null;
		}
		
		JSONObject imp = impList.get(0);
		Object impid = imp.get("impid");
		if(ObjectUtil.isEmpty(impid)) {
			return null;
		}
		
		Object bidid = logJsonObjct.get("bidid");
		Object isApp = logJsonObjct.get(BidLogExtract.PROPERTY_ISAPP);
		Object startTime = logJsonObjct.get("startTime");
		Object sspCode = logJsonObjct.get("sspCode");
		Object ip = logJsonObjct.get("ip");
		
		String sspcode_impid = impid + "_" + sspCode;
//		if("20170530-142824_reqimp_135-508-21n6-485".equals(impid)){
//			System.out.println("");
//		}
		JSONObject device = logJsonObjct.getJSONObject("device");
		Object connectiontype = device.get("connectiontype");
		Object ts = device.get("ts");
		Object lat = device.get("lat");
		Object lon = device.get("lon");
		
//		SolrInputDocument newDocument = new SolrInputDocument();
		JSONObject newJObj = new JSONObject();
		newJObj.put("bidid", bidid);
		newJObj.put(BidLogExtract.PROPERTY_ISAPP_LOWER, isApp);
		newJObj.put("starttime", startTime);
		newJObj.put(BID_IMP_PROPERTY_SSPCODE, sspCode);
		newJObj.put("impid", impid);
		newJObj.put("ip", ip);
		newJObj.put("connectiontype", connectiontype);
		newJObj.put("ts", ts);
		newJObj.put("lat", lat);
		newJObj.put("lon", lon);
		
		if(bidSite !=null) {
			//获取站点关联信息
			Object siteid = bidSite.get(BuildBidSite.BID_SITE_PROPERTY_PK);
			newJObj.put(BuildBidSite.BID_SITE_PROPERTY_PK, siteid);

			String pageId = EsIdUtil.getBase64UUID((String)siteid);
			newJObj.put("page_siteid", pageId);
		}
		
		if(bidApp!=null) {
			//获取app关联信息
			Object appid = bidApp.get(BuildBidApp.BID_APP_PROPERTY_PK);
			newJObj.put("sspcode_appid", appid);
		}
		
		if(bidAdzoneList !=null && bidAdzoneList.size()>0) {
			//获取广告位关联信息
//			SolrInputDocument bidAdzoneDocument = bidAdzoneList.get(0);
			JSONObject bidAdzoneDocument = bidAdzoneList.get(0);
			Object adzoneid = bidAdzoneDocument.get(BuildBidAdzone.BID_ADZONE_PROPERTY_PK);
			newJObj.put("sspcode_ssp_adzoneid", adzoneid);
		}
		
		if(userSsp !=null) {
			//获取user信息
			Object sspcode_scid = userSsp.get(BuildUserSsp.USER_DMP_PROPERTY_SCIDDMPCODE);
			newJObj.put(BuildUserSsp.USER_DMP_PROPERTY_SCIDDMPCODE, sspcode_scid);
		}
		
		newJObj.put(BidLogExtract.COMMON_COL_UPDATETIME, currentTime);
		
		newJObj.put(BID_IMP_PROPERTY_PK, sspcode_impid);
		
		// FIXME 调用 solr add
//		SolrUtils.saveBidLog(CollectionType.Item.BID_IMP, newDocument);

		return newJObj;
	}
}
