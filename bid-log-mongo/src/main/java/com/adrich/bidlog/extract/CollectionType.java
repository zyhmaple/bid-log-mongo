package com.adrich.bidlog.extract;


/**
 * 集合。
 * 
 * @author adrich_chc
 *
 */
public interface CollectionType {

	String getIdPropertyName();

	public enum Item implements CollectionType {
		/**
		 * 
		 */
		BID_IMP("bid_imp", ""),

		/**
		 * 
		 */
		BID_SITE("bid_site", ""),

		/**
		 * 
		 */
		BID_APP("bid_app", ""),

		/**
		 * 广告位
		 */
		BID_ADZONE("bid_adzone", ""),

		/**
		 * 
		 */
		USER_SSP("user_ssp", ""),

		/**
		 * 
		 */
		USER_DSP("user_dsp", "");

		private Item(String name, String idPropertyName) {
			this.name = name;
			this.idPropertyName = idPropertyName;
		}

		private final String name;
		private final String idPropertyName;

		public String getName() {
			return name;
		}

		public String getIdPropertyName() {
			return idPropertyName;
		}

	}
}
