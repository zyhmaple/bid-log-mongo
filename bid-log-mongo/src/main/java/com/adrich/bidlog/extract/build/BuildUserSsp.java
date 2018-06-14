package com.adrich.bidlog.extract.build;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import org.jboss.netty.util.internal.StringUtil;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.extract.UserDspConstants;
import com.adrich.bidlog.extract.UserSspConstants;
import com.adrich.bidlog.model.BuildLogModel;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public class BuildUserSsp {

	/**
	 * 各SSP用户信息集合的主键、片键。
	 */
	public static final String USER_DMP_PROPERTY_PK = UserSspConstants.PROPERTY_SCID;
	public static final String USER_DMP_PROPERTY_SCIDDMPCODE = "scid_dmpcode";

	/**
	 * 原始日志中用户属性集合。
	 */
	public static final String IP_KEY = "ips";
	public static final String USER_PROP_CITY = "city";
//	public static final String USER_PROP_KEYWORDS = "keywords";
	/**
	 * 原始日志中用户属性集合。
	 */
	private static final String USER_ATTRIBUTE_LIST_KEY = "userAttributeList";
	public static final String USER_ATTRIBUTE_LIST_KEY_ES = USER_ATTRIBUTE_LIST_KEY.toLowerCase();
	public static final String USER_ATTRIBUTE_LIST_NAME = "userattrnamelist";

	public static final String OS_iOS = "ios";
	public static final String OS_iOS_NUMBER = "1";
	public static final String OS_ANDROID = "android";
	public static final String OS_ANDROID_NUMBER = "2";
	public static final String OS_WINPHONE = "Windows Phone";
	public static final String OS_WINPHONE_NUMBER = "3";
	private static final String IDFA_NULL_1 = "(NULL)";
	private static final String IDFA_NULL_2 = "NULL";

	@SuppressWarnings("unchecked")
	public static JSONObject bulidJSONObject(JSONObject logJsonObjct,BuildLogModel sm,long currentTime,String isAppStr, boolean isApp) {

		String sspCode = logJsonObjct.getString("sspCode");
		String dmpCode = logJsonObjct.getString("dmpCode");

		//获取用户部分信息
//		Document user = logJsonObjct.get("user", Document.class);
		JSONObject user = logJsonObjct.getJSONObject("user");
		if (user == null) {
			return null;
		}
		String userid = user.getString("id");

		//获取设备部分信息
		String os = null;
		String did = null;
		String dpid = null;
		String idfa = null;
//		Document device = logJsonObjct.get("device", Document.class);
		JSONObject device = logJsonObjct.getJSONObject("device");
		if (device != null) {
			os = device.getString("os");
			did = device.getString("did");
			dpid = device.getString("dpid");
			idfa = device.getString("idfa");
		}

		// userAttributeList
		List<String> userAttributeListInBidLog = user.getObject(USER_ATTRIBUTE_LIST_KEY, List.class);
		String ipInBidLog = logJsonObjct.getString("ip");
		if (ipInBidLog != null) {
			ipInBidLog = ipInBidLog.replaceAll(" ", "");
		}

		//拼接用户/设备的唯一标识
		String scid = buildScid(isApp, userid, os, did, dpid, idfa);
		if (ObjectUtil.isEmpty(scid)) {
			return null;
		}

		String scid_dmpcode = null;
		if (isApp) {
			scid_dmpcode = scid + "_" + dmpCode;
		} else {
			scid_dmpcode = scid + "_" + sspCode;
		}

		// 2、根据 sspcode_scid 判重
//		Map<String, Object> filterMap = new HashMap<String, Object>();
//		filterMap.put(USER_SSP_PROPERTY_PK, sspcode_scid);

//		Document userSspDocInDb = MongodbCrud.findOneByAndEquals(CollectionType.Item.USER_SSP, filterMap, Arrays.asList(UserSspConstants.PROPERTY_DCID, USER_SSP_PROPERTY_PK));
		// FIXME 从 solr 中查询已存在数据
		JSONObject userSspDocInDb = null;
		if (userSspDocInDb != null) { // 库中已存在
			// FIXME 合并字段：ip、city、userAttributeList，然后更新到 solr
			// FIXME 如果有更新，则更新表： User_Dsp
			boolean haveUpdate = false;
			if (haveUpdate) {
				String dcid = (String)userSspDocInDb.get(UserSspConstants.PROPERTY_DCID);
				if (dcid != null) {
					updateUserDsp(dcid);
				}
			}
			return userSspDocInDb;
		} else {
			// 库中不存在新增一条记录
			// 构建新增时的  map
			Random rd = new Random();
//			IntStream intStream = rd.ints(0, 100);
//			SolrInputDocument newUserSspDoc = new SolrInputDocument();
			JSONObject newUserSspJobj = new JSONObject(true);
			newUserSspJobj.put(USER_DMP_PROPERTY_SCIDDMPCODE,scid_dmpcode);//rd.nextInt()//
			newUserSspJobj.put("sspcode",sspCode);
			newUserSspJobj.put(UserSspConstants.PROPERTY_DMP_CODE, dmpCode);
			newUserSspJobj.put(UserSspConstants.PROPERTY_SCID, scid);
			newUserSspJobj.put(BidLogExtract.PROPERTY_ISAPP.toLowerCase(), isAppStr);
			
			buildJSONObjectMapForUserPart(userAttributeListInBidLog, ipInBidLog, newUserSspJobj);
			newUserSspJobj.put("did", did);
			newUserSspJobj.put("dpid", dpid);
			newUserSspJobj.put("idfa", idfa);
			if (device != null) {
				Object make = device.get("make");
				Object model = device.get("model");
				Object devicetype = device.get("devicetype");
				Object macmd5 = device.get("macmd5");
				Object mac = device.get("mac");
				newUserSspJobj.put("make", make);
				newUserSspJobj.put("model", model);
				newUserSspJobj.put("os", os);
				newUserSspJobj.put("devicetype", devicetype);
				newUserSspJobj.put("macmd5", macmd5);
				newUserSspJobj.put("mac", mac);
			}
			// 如果是 app，需要在 USER_DSP 集合中新增数据
			if (isApp) {
				newUserSspJobj.put(UserSspConstants.PROPERTY_DCID, scid);
				newUserSspJobj.put(UserSspConstants.PROPERTY_DMP_CID, scid);

//				SolrInputDocument newUserDspDoc = new SolrInputDocument();
				// FIXME 调用 solr add
//				SolrUtils.saveBidLog(CollectionType.Item.USER_DSP, newUserDspDoc);
			}
			// FIXME 调用 solr add
//			SolrUtils.saveBidLog(CollectionType.Item.USER_SSP, newUserSspDoc);
			return newUserSspJobj;
		}
	}

	private static void updateUserDsp(String dcid) {
		// FIXME 将 USER_DSP 表对应 dcid 的 need_statistics 修改为 1，表示需要统计
		Map<String, Object> filterMap = new HashMap<String, Object>();
		filterMap.put(UserDspConstants.PROPERTY_DSP_PK, dcid);

//		UpdateField updateField = new MongodbCrud.UpdateField(UserDspConstants.PROPERTY_NEED_STATISTICS, UserDspConstants.NEED_STATISTICS_YES);
//
//		MongodbCrud.updateOneByAndEquals(CollectionType.Item.USER_DSP, filterMap, Arrays.asList(updateField));

	}
	private static void buildJSONObjectMapForUserPart(List<String> userAttributeList, String ip, JSONObject map) {
		map.put(IP_KEY, ip);
		map.put(USER_ATTRIBUTE_LIST_KEY_ES, userAttributeList);
		/**
		 * 客户端过滤完，需要存储的时候，后面再加此属性
		 * modify in 2017.12.22
		 */
		map.put(BidLogExtract.COMMON_COL_UPDATETIME, System.currentTimeMillis());
	}

	private static String buildScid(boolean isApp, String userid, String os, String did, String dpid, String idfa) {
		String scid = null;
		if (isApp) {
			//如果是app，则获取移动设备的唯一标识
			// 如果 idfa 是 NULL，当做空
			if (IDFA_NULL_1.equals(idfa) || IDFA_NULL_2.equals(idfa)) {
				idfa = null;
			}
			//ios: idfa > dpid(ios9之前的老设备)
			//android: did(imei) > dpid(Android)
			if (OS_iOS_NUMBER.equalsIgnoreCase(os) || OS_iOS.equalsIgnoreCase(os)) { // ios
				if (idfa != null && !idfa.isEmpty()) {
					scid = idfa;
				} else if (dpid != null && !dpid.isEmpty()) {
					scid = dpid;
				}
			} else if (OS_ANDROID_NUMBER.equalsIgnoreCase(os) || OS_ANDROID.equalsIgnoreCase(os)) { // android
				if (did != null && !did.isEmpty()) {
					scid = did;
				} else if (dpid != null && !dpid.isEmpty()) {
					scid = dpid;
				}
			} else { // windows phone 和 其他，或其他不正常情况
				if (did != null && !did.isEmpty()) {
					scid = did;
				} else if (idfa != null && !idfa.isEmpty()) {
					scid = idfa;
				} else {
					scid = dpid;
				}
			}

//			if (did != null && !"".equals(did)) {
//				//如果did有值，拼接did的值为唯一标识
//				scid = did;
//			} else if (dpid != null && !"".equals(dpid)) {
//				//如果dpid有值，拼接dpid的值为唯一标识
//				scid = dpid;
//			} else if (idfa != null && !"".equals(idfa)) {
//				//如果idfa有值，拼接idfa的值为唯一标识
//				scid = idfa;
//			}

		} else {
			//如果是pc，则直接使用传入的userid
			scid = userid;
		}
		if(!isNullOrEmpty(scid) && scid.indexOf("#")>=0){
			scid = scid.replaceAll("#", "%23");
		}
		return scid;
	}

	
	public static boolean isNullOrEmpty(String scid){
		if(scid==null||"".equals(scid))return true;
		return false;
	}
}
