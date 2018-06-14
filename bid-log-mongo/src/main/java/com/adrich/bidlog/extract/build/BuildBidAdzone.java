package com.adrich.bidlog.extract.build;


import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public class BuildBidAdzone {
	
	private static final int[] durations = {5,15,30,60};

	/**
	 * 广告位集合的主键、片键。
	 */
	public static final String BID_ADZONE_PROPERTY_PK = "sspcode_ssp_adzoneid";
//	public static final String BID_ADZONE_PROPERTY_PK_SOLR = "id";

	public static JSONObject bulidJSONObject(JSONObject impJsonObject, String sspCode, String isApp, JSONObject bidSite, JSONObject bidApp,long currentTime) {

		Object ssp_adzoneid = impJsonObject.get("adzoneid");
		if(ObjectUtil.isEmpty(ssp_adzoneid)) {
//			System.err.println("ssp_adzoneid is null : " + impJsonObject);
			return null;
		}

		// 广告位主键
		String sspcode_ssp_adzoneid = ssp_adzoneid + "_" + sspCode;

		// 以下是新增操作
//		SolrInputDocument newBidAdzone = new SolrInputDocument();
		JSONObject newJobj = new JSONObject();

		newJobj.put("ssp_adzoneid", ssp_adzoneid); // SSP 方的广告位ID
		newJobj.put("sspcode", sspCode); // SSP 标识
		newJobj.put("buyway", impJsonObject.get("buyWay")); // 购买方式：1：RTB;2:pdb;3:pmp;4:deal
		newJobj.put("bidfloor", impJsonObject.get("bidfloor")); // 广告位底价，单位是（分/CPM）
		Object isBanner = impJsonObject.get("isBanner"); // 是否为视频, banner, video
		newJobj.put("isbanner", isBanner);
		newJobj.put("width", impJsonObject.get("width")); // 宽度
		newJobj.put("height", impJsonObject.get("height")); // 高度
		newJobj.put("mime", impJsonObject.get("mime")); // 支持播放的视频格式，多个使用mongoDB数组保存
		int minduration = impJsonObject.getIntValue("minduration");// 视频广告最小时长
		int maxduration = impJsonObject.getIntValue("maxduration");// 视频广告最大时长
		newJobj.put("duration", getDuration(minduration,maxduration));
		newJobj.put("category", impJsonObject.get("category")); // 视频频道分类
		newJobj.put("title", impJsonObject.get("title")); // 视频名称
		newJobj.put("pageurl", impJsonObject.get("pageurl")); // 视频url
		newJobj.put("keywords", impJsonObject.get("keywords")); // 视频标签关键字

		newJobj.put(BidLogExtract.PROPERTY_ISAPP_LOWER, isApp);
		Object belongedId = null;
		if (BidLogExtract.ISAPP_ISPCMARK.equalsIgnoreCase(isApp)) {
			// PC
			if (bidSite != null) {
				belongedId = bidSite.get(BuildBidSite.BID_SITE_PROPERTY_PK);
			}
		} else if (BidLogExtract.ISAPP_ISAPPMARK.equalsIgnoreCase(isApp)) {
			// APP
			if (bidApp != null) {
				belongedId = bidApp.get(BuildBidApp.BID_APP_PROPERTY_PK);
			}
		}

		newJobj.put("belongedid", belongedId); // 所属位置ID
		newJobj.put(BidLogExtract.COMMON_COL_UPDATETIME, currentTime); // 最后更新时间

		newJobj.put(BID_ADZONE_PROPERTY_PK, sspcode_ssp_adzoneid);
		// FIXME 调用 solr add
//		MongodbCrud.insertOne(CollectionType.Item.BID_ADZONE, newBidAdzone);
//		SolrUtils.saveBidLog(CollectionType.Item.BID_ADZONE, newBidAdzone);
		return newJobj;
	}
	
	private static int getDuration(int minDuration,int maxDuration){
		int duration = 0;
		int paramDuration = minDuration == 0?maxDuration:minDuration;
		if(paramDuration == 0){
			return duration;
		}
		int index  = 0;
		for(int i=0;i<durations.length;i++){
			int aroundVal = Math.abs(durations[i] - paramDuration);
			if(i==0){
				duration = aroundVal;
				index = i;
			}else if(aroundVal<duration){
				duration = aroundVal;
				index = i;
			}
		}
		return durations[index];
	}
}
