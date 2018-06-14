package com.adrich.bidlog.extract;

import org.apache.log4j.Logger;

import com.adrich.bidlog.extract.build.BuildBidAdzone;
import com.adrich.bidlog.extract.build.BuildBidApp;
import com.adrich.bidlog.extract.build.BuildBidImp;
import com.adrich.bidlog.extract.build.BuildBidSite;
import com.adrich.bidlog.extract.build.BuildUserSsp;

public class CollectionDefine {
	private static Logger logger = Logger.getLogger(CollectionDefine.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long s = System.currentTimeMillis();
		for(int i=0;i<10000;i++){
			getId(CollectionType.Item.BID_ADZONE);
			getId(CollectionType.Item.BID_APP);
			getId(CollectionType.Item.USER_SSP);
		}
		long e = System.currentTimeMillis();
		System.out.println(e-s);
	}
	public static String getId(CollectionType.Item collection){
		String id = "";
		if(CollectionType.Item.BID_ADZONE == collection){
			id = BuildBidAdzone.BID_ADZONE_PROPERTY_PK;
		}else if(CollectionType.Item.BID_APP == collection){
			id = BuildBidApp.BID_APP_PROPERTY_PK;
		}else if(CollectionType.Item.BID_SITE == collection){
			id = BuildBidSite.BID_SITE_PROPERTY_PK_ES;
		}else if(CollectionType.Item.BID_IMP == collection){
			id = BuildBidImp.BID_IMP_PROPERTY_PK;
		}else if(CollectionType.Item.USER_SSP == collection){
			//移动和pc对应的_id不一样
			id = null;
		}else if(CollectionType.Item.USER_DSP == collection){
			id = UserDspConstants.PROPERTY_DSP_PK;
		}
		return id;
	}
	
	/**
	 * os 区分:操作系统,1:IOS,2Android,3Windows Phone,0或-1,其它
	 * 020 所属dmp 004
	 * 010 011 038 039  所属dmp 002
	 */
	public static String checkOS(String os){
		if(BuildUserSsp.OS_ANDROID.equalsIgnoreCase(os) || BuildUserSsp.OS_ANDROID_NUMBER.equals(os)){
			return BuildUserSsp.OS_ANDROID_NUMBER;
		}else if(BuildUserSsp.OS_iOS.equalsIgnoreCase(os) || BuildUserSsp.OS_iOS_NUMBER.equals(os)){
			return BuildUserSsp.OS_iOS_NUMBER;
		}else if(BuildUserSsp.OS_WINPHONE.equalsIgnoreCase(os) || BuildUserSsp.OS_WINPHONE_NUMBER.equals(os)){
			return BuildUserSsp.OS_WINPHONE_NUMBER;
		}else{
			return "-1";
		}
	}
	/**
	 * os 区分:操作系统,1:IOS,2Android,3Windows Phone,0或-1,其它
	 * 020 所属dmp 004
	 * 010 011 038 039  所属dmp 002
	 */
	public static String checkOS(String sspCode,String os){
		String result = os;
		if("020".equals(sspCode) || "011".equals(sspCode) || "038".equals(sspCode) || "010".equals(sspCode) || "039".equals(sspCode)){
			if("Android".equalsIgnoreCase(os)){
				result = "2";
			}else if("iOS".equalsIgnoreCase(os)){
				result = "1";
			}else if("Windows Phone".equalsIgnoreCase(os)){
				result = "3";
			}else if("other".equalsIgnoreCase(os)){
				result = "-1";
			}else{
				logger.warn("os is not analyze. sspcode:"+sspCode+" os:"+os);
				result = "-1";
			}
		}
		if(!"1".equals(result) && !"2".equals(result) && !"3".equals(result)){
			result = "-1";
		}
		return result;
	}
}
