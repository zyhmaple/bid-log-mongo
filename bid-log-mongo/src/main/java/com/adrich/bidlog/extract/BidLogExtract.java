package com.adrich.bidlog.extract;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adrich.bidlog.extract.build.BuildBidAdzone;
import com.adrich.bidlog.extract.build.BuildBidApp;
import com.adrich.bidlog.extract.build.BuildBidImp;
import com.adrich.bidlog.extract.build.BuildBidSite;
import com.adrich.bidlog.extract.build.BuildUserSsp;
import com.adrich.bidlog.model.BuildLogModel;
import com.adrich.bidlog.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;

public enum BidLogExtract {
	;
	private static final Logger logger = LoggerFactory.getLogger(BidLogExtract.class);
	/**
	 * 属性名：isApp
	 */
	public static final String PROPERTY_ISAPP = "isApp";

	public static final String PROPERTY_ISAPP_LOWER = PROPERTY_ISAPP.toLowerCase();
	/**
	 * PC
	 */
	public static final String ISAPP_ISPCMARK = "IsPCMark";
	/**
	 * App
	 */
	public static final String ISAPP_ISAPPMARK = "IsAPPMark";
	/**
	 * 公共字段
	 */
	public static final String COMMON_COL_UPDATETIME = "updatetime";

	/**
	 * 竞价日志抽取并入库入口。
	 * 
	 * @param logJson
	 *            竞价日志字符串。
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static void extractInfo(String logJson, BuildLogModel sm) throws Exception {

		JSONObject logJsonObjct = JSONObject.parseObject(logJson);
		String sspCode = logJsonObjct.getString("sspCode");

		String isAppStr = logJsonObjct.getString(PROPERTY_ISAPP);
		boolean isApp = BidLogExtract.ISAPP_ISAPPMARK.equalsIgnoreCase(isAppStr);

		List<JSONObject> impList = logJsonObjct.getObject("imp", List.class);

		long currentTime = System.currentTimeMillis();

		// 优先解析 userSsp，如果 ID 无效，整条数据抛弃
		JSONObject userSsp = null;
		try {
			userSsp = BuildUserSsp.bulidJSONObject(logJsonObjct, sm, currentTime, isAppStr, isApp);
			if (userSsp != null) {
				String scid = userSsp.getString(BuildUserSsp.USER_DMP_PROPERTY_PK);
				if (isUserSspIdInvalid(scid)) { // ID 无效，整条数据抛弃
					logger.warn("Invalid userSsp Id : [{}], JsonStr : [{}]", scid, logJson);
					return;
				}

				if (isApp) {
					JSONObject newUserDspJobj = new JSONObject(true);
					newUserDspJobj.put(UserDspConstants.PROPERTY_DSP_PK, scid);
					newUserDspJobj.put(BidLogExtract.PROPERTY_ISAPP.toLowerCase(), isAppStr);
					newUserDspJobj.put(UserDspConstants.PROPERTY_NEED_STATISTICS, UserDspConstants.NEED_STATISTICS_YES);
					sm.addDsp(newUserDspJobj);
				}
			}
			sm.addSsp(userSsp);
		} catch (Exception e) {
			logger.error("BuildUserSsp.bulid() error", e);
		}

		JSONObject bidSite = null;
		try {
			bidSite = BuildBidSite.bulidJSONObject(logJsonObjct, currentTime);
			sm.addSite(bidSite);
		} catch (Exception e) {
			logger.error("BuildBidSite.bulid() error", e);
		}

		if (ObjectUtil.isEmpty(sspCode)) {// ObjectUtil.isEmpty(sspCode)
			logger.error("sspCode is null:" + logJson);
			// 由于以下数据需要和sspCode关联，所以如果没有sspCode，则不执行如下代码
			return;
		}

		JSONObject bidApp = null;
		try {
			bidApp = BuildBidApp.bulidJSONObject(logJsonObjct, sspCode, currentTime);
			sm.addApp(bidApp);
		} catch (Exception e) {
			logger.error("BuildBidApp.bulid() error", e);
		}

		List<JSONObject> bidAdzoneList = new ArrayList<JSONObject>();
		if (impList != null) {
			for (JSONObject impJsonObject : impList) {
				JSONObject bidAdzone = null;
				try {
					bidAdzone = BuildBidAdzone.bulidJSONObject(impJsonObject, sspCode, isAppStr, bidSite, bidApp,
							currentTime);
					if (bidAdzone != null) {
						bidAdzoneList.add(bidAdzone);
					}
				} catch (Exception e) {
					logger.error("BuildBidAdzone.bulid() error", e);
				}
				// if (ObjectUtil.isEmpty(bidAdzone)) {
				// bidAdzoneList.add(bidAdzone);
				// }

			}
		}
		sm.addAdzoneList(bidAdzoneList);

		JSONObject bidImp = BuildBidImp.bulidJSONObject(logJsonObjct, bidSite, bidApp, bidAdzoneList, userSsp,
				currentTime);
		sm.addImp(bidImp);
	}

	/**
	 * ID 是否是无效的，无效：返回true；否则 false
	 * 
	 * @param userSspId
	 * @return
	 */
	private static boolean isUserSspIdInvalid(String userSspId) {
		return false;// InjectUtil.isIdInject(userSspId);
	}

	public static LogJsonModel extractInfo(String logJson) throws Exception {

		LogJsonModel jsonM = new LogJsonModel();

		JSONObject logJsonObjct = JSONObject.parseObject(logJson);
		String sspCode = logJsonObjct.getString("sspCode");

		String isAppStr = logJsonObjct.getString(PROPERTY_ISAPP);
		boolean isApp = BidLogExtract.ISAPP_ISAPPMARK.equalsIgnoreCase(isAppStr);

		List<JSONObject> impList = logJsonObjct.getObject("imp", List.class);

		JSONObject bidSite = null;
		long currentTime = System.currentTimeMillis();
		try {
			bidSite = BuildBidSite.bulidJSONObject(logJsonObjct, currentTime);
			jsonM.setSite(bidSite);
		} catch (Exception e) {
			logger.error("BuildBidSite.bulid() error", e);
		}

		if (ObjectUtil.isEmpty(sspCode)) {// ObjectUtil.isEmpty(
			// 由于以下数据需要和sspCode关联，所以如果没有sspCode，则不执行如下代码
			return jsonM;
		}

		JSONObject bidApp = null;
		try {
			bidApp = BuildBidApp.bulidJSONObject(logJsonObjct, sspCode, currentTime);
			jsonM.setApp(bidApp);
		} catch (Exception e) {
			logger.error("BuildBidApp.bulid() error", e);
		}

		List<JSONObject> bidAdzoneList = new ArrayList<JSONObject>();
		if (impList != null) {
			for (JSONObject impJsonObject : impList) {
				JSONObject bidAdzone = null;
				try {
					bidAdzone = BuildBidAdzone.bulidJSONObject(impJsonObject, sspCode, isAppStr, bidSite, bidApp,
							currentTime);
					if (bidAdzone != null) {
						bidAdzoneList.add(bidAdzone);
					}
				} catch (Exception e) {
					logger.error("BuildBidAdzone.bulid() error", e);
				}
				// if (ObjectUtil.isEmpty(bidAdzone)) {
				// bidAdzoneList.add(bidAdzone);
				// }

			}
		}
		jsonM.setAdzoneList(bidAdzoneList);

		JSONObject userSsp = null;
		try {
			userSsp = BuildUserSsp.bulidJSONObject(logJsonObjct, null, currentTime, isAppStr, isApp);
			jsonM.setSsp(userSsp);
		} catch (Exception e) {
			logger.error("BuildUserSsp.bulid() error", e);
		}

		JSONObject bidImp = BuildBidImp.bulidJSONObject(logJsonObjct, bidSite, bidApp, bidAdzoneList, userSsp,
				currentTime);
		jsonM.setImp(bidImp);
		return jsonM;
	}

	@SuppressWarnings("unchecked")
	public static void extractInfo(JSONObject logJsonObjct, BuildLogModel sm) throws Exception {

		// JSONObject logJsonObjct = JSONObject.parseObject(logJson);
		String sspCode = logJsonObjct.getString("sspCode");

		String isAppStr = logJsonObjct.getString(PROPERTY_ISAPP);
		boolean isApp = BidLogExtract.ISAPP_ISAPPMARK.equalsIgnoreCase(isAppStr);

		List<JSONObject> impList = logJsonObjct.getObject("imp", List.class);

		long currentTime = System.currentTimeMillis();

		// 优先解析 userSsp，如果 ID 无效，整条数据抛弃
		JSONObject userSsp = null;
		try {
			userSsp = BuildUserSsp.bulidJSONObject(logJsonObjct, sm, currentTime, isAppStr, isApp);
			if (userSsp != null) {
				String scid = userSsp.getString(BuildUserSsp.USER_DMP_PROPERTY_PK);
				if (isUserSspIdInvalid(scid)) { // ID 无效，整条数据抛弃
					logger.warn("Invalid userSsp Id : [{}], JsonStr : [{}]", scid, logJsonObjct.toString());
					return;
				}

				if (isApp) {
					JSONObject newUserDspJobj = new JSONObject(true);
					newUserDspJobj.put(UserDspConstants.PROPERTY_DSP_PK, scid);
					newUserDspJobj.put(BidLogExtract.PROPERTY_ISAPP.toLowerCase(), isAppStr);
					newUserDspJobj.put(UserDspConstants.PROPERTY_NEED_STATISTICS, UserDspConstants.NEED_STATISTICS_YES);
					sm.addDsp(newUserDspJobj);
				}
			}
			sm.addSsp(userSsp);
		} catch (Exception e) {
			logger.error("BuildUserSsp.bulid() error", e);
		}

	}
	
	public static JSONObject extractInfo(JSONObject logJsonObjct) throws Exception {

		// JSONObject logJsonObjct = JSONObject.parseObject(logJson);
		String sspCode = logJsonObjct.getString("sspCode");

		String isAppStr = logJsonObjct.getString(PROPERTY_ISAPP);
		boolean isApp = BidLogExtract.ISAPP_ISAPPMARK.equalsIgnoreCase(isAppStr);

		List<JSONObject> impList = logJsonObjct.getObject("imp", List.class);

		long currentTime = System.currentTimeMillis();

		// 优先解析 userSsp，如果 ID 无效，整条数据抛弃
		JSONObject userSsp = null;
		try {
			userSsp = BuildUserSsp.bulidJSONObject(logJsonObjct, null, currentTime, isAppStr, isApp);
			if (userSsp != null) {
				String scid = userSsp.getString(BuildUserSsp.USER_DMP_PROPERTY_PK);
				if (isUserSspIdInvalid(scid)) { // ID 无效，整条数据抛弃
					logger.warn("Invalid userSsp Id : [{}], JsonStr : [{}]", scid, logJsonObjct.toString());
					return null;
				}

/*				if (isApp) {
					JSONObject newUserDspJobj = new JSONObject(true);
					newUserDspJobj.put(UserDspConstants.PROPERTY_DSP_PK, scid);
					newUserDspJobj.put(BidLogExtract.PROPERTY_ISAPP.toLowerCase(), isAppStr);
					newUserDspJobj.put(UserDspConstants.PROPERTY_NEED_STATISTICS, UserDspConstants.NEED_STATISTICS_YES);
					sm.addDsp(newUserDspJobj);
				}*/
			}
			//sm.addSsp(userSsp);
		} catch (Exception e) {
			logger.error("BuildUserSsp.bulid() error", e);
		}
		return userSsp;

	}
}
