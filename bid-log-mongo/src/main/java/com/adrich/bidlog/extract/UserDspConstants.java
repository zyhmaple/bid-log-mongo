package com.adrich.bidlog.extract;

/**
 * USER_DSP 集合常量类。
 * 
 * @author adrich_chc
 *
 */
public enum UserDspConstants {
	;
	/**
	 * 主键属性名
	 */
	public static final String PROPERTY_DSP_PK = "dcid";
	/**
	 * 是否需要统计
	 */
	public static final String PROPERTY_NEED_STATISTICS = "need_statistics";
	/**
	 * 需要统计：1
	 */
	public static final Integer NEED_STATISTICS_YES = 1;
	/**
	 * 不需要统计：0
	 */
	public static final Integer NEED_STATISTICS_NO = 0;
}
