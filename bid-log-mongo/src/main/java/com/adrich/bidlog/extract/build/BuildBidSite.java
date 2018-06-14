package com.adrich.bidlog.extract.build;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.util.EsIdUtil;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public class BuildBidSite {
	private static final Logger logger = LoggerFactory.getLogger(BuildBidSite.class);
	/**
	 * 站点集合的主键、片键。
	 */
	public static final String BID_SITE_PROPERTY_PK = "page";
	public static final String BID_SITE_PROPERTY_PK_ES = "id";

	public static JSONObject bulidJSONObject(JSONObject logJsonObjct, long currentTime) throws Exception{
		Object isApp = logJsonObjct.get("isApp");
		if("IsAPPMark".equals(isApp)) {
			return null;
		}
 		JSONObject site = logJsonObjct.getJSONObject("site");
		if(site==null || site.size()==0) {
			return null;
		}
		String page = (String)site.get(BID_SITE_PROPERTY_PK);
		if(ObjectUtil.isEmpty(page)){
			return null;
		}
		/*if (!((String) page).startsWith("http://") && !((String) page).startsWith("https://")) {
			logger.error("page is not html page ： " + logJsonObjct.toJSONString());
			return null;
		}*/
//		Map<String, Object> filterMap = new HashMap<String, Object>();
//		filterMap.put(BID_SITE_PROPERTY_PK, page);
//		Document oldSiteDocumet = MongodbCrud.findOneByAndEquals(CollectionType.Item.BID_SITE, filterMap);
		// FIXME 调用 solr 查询是否存在
		Object name = site.get("name");
		Object ref = site.get("ref");
		Object pageVertical = site.get("pageVertical");
		Object site_quality = site.get("site_quality");
		Object page_type = site.get("page_type");
		Object page_keywords = site.get("page_keywords");
		Object site_categoryegory = site.get("site_categoryegory");
		Object page_quality = site.get("page_quality");

		//不存在站点信息，新增站点信息
		JSONObject siteJobj = new JSONObject();
		
		siteJobj.put(BID_SITE_PROPERTY_PK, page);
		String esId = EsIdUtil.getBase64UUID((String)page);
		siteJobj.put(BID_SITE_PROPERTY_PK_ES, esId);
		siteJobj.put("name", name);
		siteJobj.put("ref", ref);
		siteJobj.put("pagevertical", pageVertical);
		siteJobj.put("site_quality", site_quality);
		siteJobj.put("page_type", page_type);
		siteJobj.put("page_keywords", page_keywords);
		siteJobj.put("site_categoryegory", site_categoryegory);
		siteJobj.put("page_quality", page_quality);
		siteJobj.put("iscrawl", 0);
		siteJobj.put("inserttime", currentTime);
		siteJobj.put(BidLogExtract.COMMON_COL_UPDATETIME, currentTime);
		return siteJobj;

	}
}
