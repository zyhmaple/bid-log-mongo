package com.adrich.bidlog.mongodb;

import java.io.File;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream.Filter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adrich.bidlog.consume.KafkaCosumeObject;
import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.model.BuildLogModel;
import com.adrich.bidlog.mongodb.MongoDBBatchCURD.UpdateField.Operator;
import com.adrich.bidlog.product.logProductor;
import com.adrich.bidlog.util.LogConstants;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

/**
 * win,click,expos,summary,bid mongo日志批量保存，更新
 * 
 * @author zyh
 *
 */
public enum MongoDBBatchCURD {

	;
	private static final Logger logger = LoggerFactory.getLogger(MongoDBBatchCURD.class);

	public static final String placeHolder_Key = "placeholder";
	public static final Byte placeHolder_Bytes = (byte) 'x';
	public static int placeHolder_ByteCount = 1024;
	public static long processLogSumCount;
	public static boolean threadOnOff = false;

	// 配置参数默认值
	public static int batchProcessReqCount = 5000;
	public static int blockQueueFactor = 8;
	public static int threadCountInSameTime = 8;
	public static int avgThreadCountOfQueue = 2;
	public static int reqSleep = 1000;
	public static int threadSleep = 1000;
	public static int startIndex = 0;

	/**
	 * MongoDB错误代码：主键重复
	 */
	public static final String ERROR_CODE_DUPLICATE_KEY = "E11000";

	// 配置
	public static final String MONGODB_URI_KEY = "mongodb.uri";
	public static final String MONGODB_DATABASENAME = "mongodb.databasename";
	// 一次批处理数据量
	public static final String MONGODB_PROCESSREQ_COUNT = "mongodb.batchProcessReqCount";
	// 队列长度因子：队列长度 = 批处理量*队列因子
	public static final String MONGODB_BQUEUE_FACTOR = "mongodb.blockQueueFactor";
	// 同一时间容许最大线程数量。各线程总和不超过最大值；
	// 各日志行程启动线程数=总和/4；summary享有分配后剩余线程
	public static final String MONGODB_ALL_THREAD_COUNT = "mongodb.threadCountInSameTime";
	public static final String MONGODB_AVG_THREAD_COUNT = "mongodb.avgThreadCountOfQueue";

	public static final String MONGODB_THREAD_REQSLEEP = "mongodb.reqSleep";
	public static final String MONGODB_THREAD_SLEEP = "mongodb.threadSleep";
	public static final String MONGODB_START_INDEX = "mongodb.startIndex";

	private static MongoClient MONGO_CLIENT;
	private static MongoDatabase DATABASE;
	private static Map<String, MongoCollection<Document>> COLLECTION_MAP;

	// 读取配置文件、初始化客户端、数据库、集合连接
	static {

		InputStream resourceStream = null;
		try {
			// 读取配置文件
			resourceStream = MongoDBBatchCURD.class.getResourceAsStream("/mongodb-config.properties");

			Properties p = new Properties();
			p.load(resourceStream);

			// MongoDB 连接字符串
			String uri = p.getProperty(MONGODB_URI_KEY);
			MongoClientURI mongoClientURI = new MongoClientURI(uri);
			MONGO_CLIENT = new MongoClient(mongoClientURI);

			// 要连接的数据库
			String databaseName = p.getProperty(MONGODB_DATABASENAME);
			DATABASE = MONGO_CLIENT.getDatabase(databaseName);

			// 批量处理请求数量
			batchProcessReqCount = Integer.parseInt(p.getProperty(MONGODB_PROCESSREQ_COUNT));
			// 阻塞队列长度因子 ；队列长度 = batchProcessReqCount * blockQueueFactor
			blockQueueFactor = Integer.parseInt(p.getProperty(MONGODB_BQUEUE_FACTOR));
			// 同一时间最大线程数
			threadCountInSameTime = Integer.parseInt(p.getProperty(MONGODB_ALL_THREAD_COUNT));
			avgThreadCountOfQueue = Integer.parseInt(p.getProperty(MONGODB_AVG_THREAD_COUNT));

			// 测试用，请求发送延迟
			reqSleep = Integer.parseInt(p.getProperty(MONGODB_THREAD_REQSLEEP));
			// 工作线程批处理延迟
			threadSleep = Integer.parseInt(p.getProperty(MONGODB_THREAD_SLEEP));

			startIndex = Integer.parseInt(p.getProperty(MONGODB_THREAD_SLEEP));
			// 将库中已存在的集合放入 Map 中缓存
			COLLECTION_MAP = new ConcurrentHashMap<String, MongoCollection<Document>>();

			MongoIterable<String> collectionNames = DATABASE.listCollectionNames();

			for (String collectionName : collectionNames) {
				MongoCollection<Document> collection = DATABASE.getCollection(collectionName);

				COLLECTION_MAP.put(collectionName, collection);
			}

			ExecutorService service = Executors.newCachedThreadPool(); // 缓存线程池

			// 创建队列监视线程
			service.execute(new QueueWatcher(service));

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (resourceStream != null) {
				try {
					resourceStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public synchronized static void closeMongoClient() {

		if (MONGO_CLIENT != null) {
			MONGO_CLIENT.close();
		}
	}

	/**
	 * 从集合缓存中获取指定的集合连接。
	 * 
	 * @param dmpCollection
	 *            要获取的集合。
	 * @return 集合连接。
	 */
	private static MongoCollection<Document> getCollection(String collectionName) {

		MongoCollection<Document> collection = COLLECTION_MAP.get(collectionName);
		if (collection == null) {
			collection = DATABASE.getCollection(collectionName);
			COLLECTION_MAP.put(collectionName, collection);
		}
		return collection;
	}

	/**
	 * 根据查询条件查询指定集合文档数量。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 指定集合文档数量。
	 */
	public static long countByAndEquals(String collectionName, Map<String, Object> filterMap) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		long count = 0;
		if (filter == null) {
			count = collection.count();
		} else {
			count = collection.count(filter);
		}
		return count;
	}

	/**
	 * 根据查询条件查询指定集合文档是否存在。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 指定集合文档是否存在。
	 */
	public static boolean existsByAndEquals(String collectionName, Map<String, Object> filterMap) {
		long count = countByAndEquals(collectionName, filterMap);
		return count > 0;
	}

	/**
	 * 根据查询条件查询指定集合文档多条内容。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果游标。
	 */
	public static FindIterable<Document> findByAndEquals(String collectionName, Map<String, Object> filterMap) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		FindIterable<Document> found = null;
		if (filter == null) {
			found = collection.find();
		} else {
			found = collection.find(filter);
		}
		return found;
	}

	/**
	 * 根据查询条件查询指定集合文档单条内容，如果查出多条内容，将返回第一条。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param dmpCollection
	 *            要查询的集合。
	 * @param filterMap
	 *            查询条件。
	 * @return 查询结果文档。
	 */
	public static Document findOneByAndEquals(String collectionName, Map<String, Object> filterMap) {
		FindIterable<Document> found = findByAndEquals(collectionName, filterMap);
		if (found != null) {
			MongoCursor<Document> cursor = found.iterator();
			try {
				if (cursor.hasNext()) {
					Document first = cursor.next();
					return first;
				}
			} finally {
				cursor.close();
			}
		}
		return null;
	}

	/**
	 * 根据查询条件 Map 构建查询条件对象。<br />
	 * 查询条件是 Equals 比较符。<br />
	 * 多个查询条件之间用 And 逻辑连接。
	 * 
	 * @param filterMap
	 *            查询条件 Map。
	 * @return 查询条件对象。
	 */
	private static Bson buildFilterByAndEquals(Map<String, Object> filterMap) {
		Bson filter = null;
		if (filterMap != null) {
			for (Entry<String, Object> entry : filterMap.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				Bson filterTemp = Filters.eq(fieldName, value);
				if (filter == null) {
					filter = filterTemp;
					continue;
				}
				filter = Filters.and(filter, filterTemp);
			}
		}
		return filter;
	}

	/**
	 * 根据更新 Map 构建更新对象。
	 * 
	 * @param updateMap
	 *            更新 Map。
	 * @return 更新对象。
	 */
	private static Bson buildUpdate(Map<String, Object> updateMap) {
		List<Bson> updates = null;
		if (updateMap != null) {
			updates = new ArrayList<Bson>();
			for (Entry<String, Object> entry : updateMap.entrySet()) {
				String fieldName = entry.getKey();
				Object value = entry.getValue();
				Bson updateTemp = Updates.set(fieldName, value);
				updates.add(updateTemp);
			}
		}

		Bson update = null;
		if (updates != null) {
			update = Updates.combine(updates);
		}
		return update;
	}

	/**
	 * 根据更新 Map 构建更新Summary对象。
	 * 
	 * @param updateMap
	 *            更新 Map。
	 * @return 更新对象。
	 * @author zyh
	 */
	private static Bson buildUpdate2(Map<String, Object> updateMap) {

		List<Bson> updates = null;
		if (updateMap != null) {
			updates = new ArrayList<Bson>();
			for (Entry<String, Object> entry : updateMap.entrySet()) {
				// String typeName = entry.getKey();
				if (entry.getKey().startsWith("$filter"))
					continue;
				UpdateField typeValue = (UpdateField) entry.getValue();
				Bson updateTemp = null;

				switch (typeValue.operator) {
				case setOnInsert: {
					updateTemp = Updates.setOnInsert(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case set: {
					updateTemp = Updates.set(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case pull: {
					updateTemp = Updates.pull(typeValue.fieldName, typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case unset: {
					updateTemp = Updates.unset(typeValue.fieldName);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
				case addEachToSet: {
					/*
					 * List<String> value = new ArrayList<String>();
					 * value.add(typeValue.value.toString());
					 */
					if (typeValue.value instanceof List)
						updateTemp = Updates.addEachToSet(typeValue.fieldName, (List<String>) typeValue.value);
					if (updateTemp != null)
						updates.add(updateTemp);
					break;
				}
					/*
					 * case addToSet: { updateTemp =
					 * Updates.addToSet(typeValue.fieldName, typeValue.value);
					 * if (updateTemp != null) updates.add(updateTemp); break; }
					 */
				default:
					break;
				}
			}
		}

		Bson update = null;
		if (updates != null) {
			update = Updates.combine(updates);
		}
		return update;
	}

	/**
	 * 更新指定集合的符合条件的单条文档。
	 * 
	 * @param dmpCollection
	 *            要更新的集合。
	 * @param filterMap
	 *            更新条件。
	 * @param update
	 *            更新数据。
	 * @return 更新结果。
	 */
	public static UpdateResult updateOneByAndEquals(String collectionName, Map<String, Object> filterMap,
			Document document) {
		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		Bson update = buildUpdate(document);
		UpdateResult updateResult = collection.updateOne(filter, update);
		return updateResult;
	}

	/**
	 * 更新指定集合的符合条件的单条文档。
	 * 
	 * @param dmpCollection
	 *            要更新的集合。
	 * @param filterMap
	 *            更新条件。
	 * @param update
	 *            更新数据。
	 * @return 更新结果。
	 */
	public static UpdateResult updateOneByAndEqualsUpsert(String collectionName, Map<String, Object> filterMap,
			Document document) {

		long begin = System.currentTimeMillis();

		MongoCollection<Document> collection = getCollection(collectionName);
		Bson filter = buildFilterByAndEquals(filterMap);
		Bson update = buildUpdate(document);
		UpdateOptions uo = new UpdateOptions();
		uo.upsert(true);
		UpdateResult updateResult = null;
		try {
			updateResult = collection.updateOne(filter, update, uo);
		} catch (Exception e) {
			// 如果出错，则尝试在调用一次。
			// 在高并发的情况下，会出现主键重复的错误
			try {
				updateResult = collection.updateOne(filter, update, uo);
			} catch (Exception e1) {
				// throw e1;
			}
		} finally {
			if (logger.isDebugEnabled()) {
				long cost = System.currentTimeMillis() - begin;
				if (cost > 100) {
					logger.debug("MongoDB updateOneByAndEqualsUpsert slowly cost : {}ms!", cost);
				}
			}
		}
		return updateResult;
	}

	/**
	 * 向指定集合中插入单条文档。
	 * 
	 * @param dmpCollection
	 *            要插入文档的集合。
	 * @param document
	 *            要插入的文档。
	 */
	public static void insertOne(String collectionName, Document document) {

		long begin = System.currentTimeMillis();

		MongoCollection<Document> collection = getCollection(collectionName);
		collection.insertOne(document);

		if (logger.isDebugEnabled()) {
			long cost = System.currentTimeMillis() - begin;
			if (cost > 100) {
				logger.debug("MongoDB insertOne slowly cost : {}ms!", cost);
			}
		}
	}


	/**
	 * 插入log_summary
	 * 
	 * @param doc
	 * @param countType
	 * @author zyh
	 */
	private static void insertLogSummary(Document doc, String countType) {

		Map<String, Object> upfieldList = new HashMap<String, Object>(12);

		String requestID = doc.getString("requestID");
		int timeInt = doc.getInteger("log_time");
		String sspCode = doc.getString("sspCode");
		String orderID = doc.getString("orderID");
		String planID = doc.getString("planID");

		// 过滤条件字段
		upfieldList.put("$filter", Filters.eq("requestTimeID", requestID));

		upfieldList.put("requestTimeID", new UpdateField(Operator.setOnInsert, "requestTimeID", requestID));
		upfieldList.put("sspCode", new UpdateField(Operator.setOnInsert, "sspCode", sspCode));
		upfieldList.put("orderID", new UpdateField(Operator.setOnInsert, "orderID", orderID));
		upfieldList.put("planID", new UpdateField(Operator.setOnInsert, "planID", planID));
		upfieldList.put("log_time", new UpdateField(Operator.setOnInsert, "log_time", timeInt));
		upfieldList.put("log_timehour", new UpdateField(Operator.setOnInsert, "log_timehour", timeInt / 100));
		upfieldList.put("wincount", new UpdateField(Operator.setOnInsert, "wincount", 0));
		upfieldList.put("exposcount", new UpdateField(Operator.setOnInsert, "exposcount", 0));
		upfieldList.put("clickcount", new UpdateField(Operator.setOnInsert, "clickcount", 0));

		if ("exposcount".equals(countType)) {
			upfieldList.put("log_time", new UpdateField(Operator.set, "log_time", timeInt));
			upfieldList.put("log_timehour", new UpdateField(Operator.set, "log_timehour", timeInt / 100));
		}

		upfieldList.put(countType, new UpdateField(Operator.set, countType, 1));

		upsertMany(LogConstants.MONGODB_SUMMARY, new Document(upfieldList));
	}

	/**
	 * 插入log_summary
	 * 
	 * @param doc
	 * @param countType
	 * @author zyh
	 */
	public static void insertSet(JSONObject kfJson) {

		if (kfJson == null)
			return;

		String scid_dmpcode = kfJson.getString("scid_dmpcode");
		String sspcode = kfJson.getString("sspcode");
		String dmp_code = kfJson.getString("dmp_code");
		long updatetime = 0;

		boolean placeHolderBL = false;
		if (placeHolderBL) {
			// 构建占位json
			JSONObject insertObject = new JSONObject();
			insertObject.put("scid_dmpcode", scid_dmpcode);
			insertObject.put("sspcode", sspcode);
			insertObject.put("dmp_code", dmp_code);
			insertObject.put("updatetime", updatetime);
			byte[] tempPlaceHolder = new byte[placeHolder_ByteCount - insertObject.toString().getBytes().length];
			Map<String, Object> upfieldDefaultList = new LinkedHashMap<String, Object>(12);
			// 部分如果一次插入，不再修改字段，可以直接用真值插入
			upfieldDefaultList.put("scid_dmpcode", new UpdateField(Operator.setOnInsert, "scid_dmpcode", scid_dmpcode));
			upfieldDefaultList.put("sspCode", new UpdateField(Operator.setOnInsert, "sspcode", sspcode));
			upfieldDefaultList.put("dmp_code", new UpdateField(Operator.setOnInsert, "dmp_code", dmp_code));
			// 必须有
			upfieldDefaultList.put("updatetime", new UpdateField(Operator.setOnInsert, "updatetime", updatetime));

			upfieldDefaultList.put(placeHolder_Key,
					new UpdateField(Operator.setOnInsert, placeHolder_Key, tempPlaceHolder));
			upfieldDefaultList.put("$filter", Filters.eq("scid_dmpcode", scid_dmpcode));
			// 插入默认值，占位
			upsertMany(LogConstants.MONGODB_BIDUSER, new Document(upfieldDefaultList));
		}
		
		//Log 更新 归并
		
		Map<String, Object> upfieldSetList = new LinkedHashMap<String, Object>(25);
		// 对非空进行修改赋值
		for (Entry<String, Object> entry : kfJson.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if(isNullOrEmpty(value))continue;
			
			if (isNeedSaveAsListField(key)) {
				
				List valueList = null;
				if(value instanceof String)
					{
						valueList = new ArrayList<String>();
						valueList.add(value);
					}
				else if((value instanceof List))
					valueList = (List) value;
				// 合并
				upfieldSetList.put(key, new UpdateField(Operator.addEachToSet, key, valueList));
			} else {
				Bson tempFilter = null;
				if (key == "scid_dmpcode")
					tempFilter = Filters.eq(key, value);
				else if (key == "updatetime")
					tempFilter = Filters.lt(key, value);
				if (tempFilter != null) {
					if (upfieldSetList.containsKey("$filter"))
						upfieldSetList.put("$filter", Filters.and((Bson) upfieldSetList.get("$filter"), tempFilter));
					else
						upfieldSetList.put("$filter", tempFilter);
				}

				if (isNotNeedUpdateForBid(key))
					upfieldSetList.put(key, new UpdateField(Operator.setOnInsert, key, value));
					// 部分如果不发生字段，这里可以不做更新
				else
					// 真正需要更新的字段
					upfieldSetList.put(key, new UpdateField(Operator.set, key, value));

			}
		}

		// 清空 占位 数组
		upfieldSetList.put(placeHolder_Key, new UpdateField(Operator.unset, placeHolder_Key, 1));

		upsertMany(LogConstants.MONGODB_BIDUSER, new Document(upfieldSetList));
	}

	/*
	 * 不需要进行频繁更新的字段；一般是一些设备、媒体等固定的信息
	 */
	public static boolean isNotNeedUpdateForBid(String str) {
		Set<String> fields = new HashSet<String>();
		fields.add("scid_dmpcode");
		fields.add("sspcode");
		fields.add("dmp_code");
		fields.add("isApp");
		if (fields.contains(str))
			return true;
		else
			return false;
	}

	/*
	 * 需要采取数组保存的字段
	 */
	public static boolean isNeedSaveAsListField(String str){
		
		Set<String> fields = new HashSet<String>();
		fields.add("ips");
		fields.add("city");
		fields.add("userattributelist");
		fields.add("userattrnamelist");
		if (fields.contains(str))
			return true;
		else
			return false;
	}
	public static boolean isNullOrEmpty(Object req) {
		if (req == null)
			return true;
		if (req instanceof String)
			return "".equals(req);
		if (req instanceof Integer)
			return 0 == (Integer) req;
		if (req instanceof List)
			return ((List<?>) req).size() == 0;
		return false;

	}

	/**
	 * 批量插入，放入相应队列
	 * 
	 * @param collectionName
	 * @param document
	 * @author zyh
	 */
	public static void insertMany(String collectionName, Document document) {
		long begin = System.currentTimeMillis();
		MongoBlockQueuePool.initQueueByBidLogType(collectionName);
		BlockingQueue<Document> queue = MongoBlockQueuePool.getQueueByBidLogType(collectionName);
		try {
			queue.put(document);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (logger.isDebugEnabled()) {
			long cost = System.currentTimeMillis() - begin;
			if (cost > 100) {
				logger.debug("MongoDB insertOne slowly cost : {}ms!", cost);
			}
		}
	}

	/**
	 * 批量更新文档
	 * 
	 * @param collectionName
	 * @param document
	 * @author zyh
	 */
	public static void upsertMany(String collectionName, Document document) {
		insertMany(collectionName, document);
	}


	


	/**
	 * 监视队列，负责检索所有日志处理线程
	 * @author zyh
	 *
	 */
	static class QueueWatcher implements Runnable {

		private final ExecutorService service;

		QueueWatcher(ExecutorService service) {
			this.service = service;
		}

		public void run() {
			while (true) {
				doWork();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		void doWork() {
			if (!MongoBlockQueuePool.getAllQueues().isEmpty())
				// 循环创建处理线程；条件队列长度大于5000且 同类线程数 不超过线程平均数
				for (String key : MongoBlockQueuePool.getAllQueues().keySet()) {
					// 队列大于一倍以上批处理量
					int _count = MongoBlockQueuePool.getProcessThreadCountByBidLogType(key);
					int _queueCount = MongoBlockQueuePool.countOfNotEmptyQueueCountInSameTime();
					if (MongoBlockQueuePool.getQueueByBidLogType(key).size() / batchProcessReqCount >= 1
							// 线程分配数不得大于n分之一总线程数
							&& (_count < avgThreadCountOfQueue
									// 汇总数据线程数 可额外分配平分后空余线程
									|| (LogConstants.MONGODB_SUMMARY.equals(key) && _count < (threadCountInSameTime
											- (avgThreadCountOfQueue * _queueCount))))) {

						this.service.execute(new BatchWorker(key));
						System.out.println("Queue[" + key + "] insert is Start!");
					}
				}
		}
	}
	
	/**
	 * 批处理工作线程
	 * @author zyh
	 *
	 */
	static class BatchWorker implements Runnable {
		private String collectionName;
		private ThreadLocal<Integer> countThreadLocal;
		private List<Document> batchList = null;
		private Map<String, Document> summaryMap = null;
		private long sumCount;

		BatchWorker(String collectionName) {
			this.collectionName = collectionName;
			MongoBlockQueuePool.increateProcessThreadCountByBidLogType(collectionName);
			// 汇总日志特殊处理
			if (isNeedSummaryMap(collectionName))
				summaryMap = new HashMap<String, Document>(batchProcessReqCount);

			batchList = new ArrayList<Document>(batchProcessReqCount);

		}

		public void run() {

			MongoCollection<Document> collection = getCollection(this.collectionName);
			BlockingQueue<Document> queue = MongoBlockQueuePool.getQueueByBidLogType(this.collectionName);

			while (true) {
				try {

					doWork(collection, queue);

				} finally {
					if (batchList != null)
						batchList.clear();
					if (summaryMap != null)
						summaryMap.clear();
				}
			}
		}

		void doWork(MongoCollection<Document> collection, BlockingQueue<Document> queue) {

			int size = batchProcessReqCount;
			long begin = System.currentTimeMillis();

			for (int i = 0; i < batchProcessReqCount; i++) {
				try {
					// summary插入前批量汇总，一边出队一边汇总，问题：影响出队效率
					/*
					 * if (summaryMap != null) { Document doc1 = queue.take();
					 * String keyName = doc1.getString("$id");//主键，更新过滤条件
					 * Document onInsert = (Document)
					 * doc1.get("$onInsert");//插入字段 String keyValue =
					 * onInsert.getString(keyName); //汇总后的放入map
					 * summaryMap.put(keyValue, UnionSummary(doc1,
					 * summaryMap.get(keyValue))); } else
					 */
					batchList.add(queue.take());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// 批量处理在出队后处理
			if (isNeedSummaryMap(this.collectionName)) {
				if (summaryMap != null && batchList != null) {
					// 汇总
					for (Document doc1 : batchList) {
						Bson filter = (Bson) doc1.get("$filter");// 过滤
						MyEntry<String, Object> key = getFiltersFirst(filter.toString());
						// 汇总后的放入map
						summaryMap.put(key.getValue().toString(),
								UnionSummary(doc1, summaryMap.get(key.getValue().toString())));
					}

					size = summaryMap.values().toArray().length;
					UpdateOptions upsert = new UpdateOptions();
					if (size != 0) {
						for (Document summary : summaryMap.values()) {

							Bson filter = (Bson) summary.get("$filter");
							try {
								collection.updateOne(filter, buildUpdate2(summary), upsert.upsert(true));
							} catch (Exception e) {
								if (e instanceof MongoWriteException) {
									String message = e.getMessage();
									if (message == null) {
										message = "";
									}
									if (message.startsWith("E11000")) {
										System.out.println("E11000,主键重复，no problem");
									} else {
										e.printStackTrace();
									}
								} else {
									e.printStackTrace();
								}
							}
						}
					}
				}
			} else
				collection.insertMany(batchList);

			sumCount += size;
			System.out.println("[" + new Date(System.currentTimeMillis()) + "] Current Thread["
					+ Thread.currentThread().getName() + "][" + collectionName + "] BatchProcess[" + size
					+ "条文档] SpendTime:" + (System.currentTimeMillis() - begin) + "ms;be Proecssed Count:" + sumCount
					+ "条; left Queue size:" + queue.size());

			try {
				Thread.sleep(threadSleep);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public ThreadLocal<Integer> getCountThreadLocal() {
			return countThreadLocal;
		}

		public void setCountThreadLocal(ThreadLocal<Integer> countThreadLocal) {
			this.countThreadLocal = countThreadLocal;
		}
	}

	private static boolean isNeedSummaryMap(String logName) {
		switch (logName) {
		case LogConstants.MONGODB_SUMMARY:
		case LogConstants.MONGODB_BIDUSER:
			return true;
		default:
			return false;
		}
	}

	// SUMMMARY 需要汇总
	// 情况1：单个字段覆盖
	// 情况2：数组 要归并
	private static Document UnionSummary(Document doc1, Document doc2) {
		// doc1 新插入的；doc2已经整合的
		if (doc2 == null)
			return doc1;

		UpdateField userattributelist = (UpdateField)doc2.get("userattributelist");
		
/*		if(userattributelist!=null){
			UpdateField id = (UpdateField)doc2.get("scid_dmpcode");
			System.out.println("Union \"scid_dmpcode\":\""+id.value+"\"");
			System.out.println("\"userattributelist\":\""+userattributelist.value+"\"");
		}*/

		
		// 将doc1 整合到doc2
		for (Object obj : doc1.values()) {
			
			if (obj instanceof Bson) {
				
				doc2.put("$filter", doc1.get("$filter"));
				
			} else if (obj instanceof UpdateField) {
				
				UpdateField field = (UpdateField) obj;
				//if (isUnionOperator(field.operator)) {
					
					if (Operator.set.equals(field.operator))
						doc2.put(field.fieldName, obj);
					else if (Operator.addEachToSet.equals(field.operator)) {
						UpdateField older = (UpdateField) doc2.get(field.fieldName);
						UpdateField newer = (UpdateField) obj;
						if ((older.value instanceof List) && (newer.value instanceof List)) {
							List oldList = (List) older.value;
							List newList = (List) newer.value;
							oldList.addAll((List) newer.value);
							
							older.value = addToSetForSummary(oldList);
						}
					}

				//}
			}
		}
		return doc2;
	}

	//List 去重
	private static List addToSetForSummary(List arrList){
		
		Set<String> addToSet = new HashSet<String>(arrList);
		
		List<String> result = new ArrayList<>(addToSet);
		
		return result;
	}
	
	/**
	 * 需要合并的操作
	 * 
	 * @param opr
	 * @return
	 */
	public static boolean isUnionOperator(Operator opr) {
		switch (opr) {
		case set:
		case addToSet:;
		case addEachToSet:
			return true;
		default:
			return false;
		}
	}
	/**
	 * 操作域
	 * @author zyh
	 *
	 */
	public static class UpdateField {
		public static enum Operator {
			set, setOnInsert, addEachToSet, unset, addToSet, pull
		}

		public UpdateField(String fieldName, Object value) {
			this(Operator.set, fieldName, value);
		}

		public UpdateField(Operator operator, String fieldName, Object value) {
			this.operator = operator;
			this.fieldName = fieldName;
			this.value = value;
		}

		public Operator operator;
		public String fieldName;
		public Object value;
	}

	public static Map<String, Object> getFilterMap(String FilterStr) {

		String pattern1 = "(fieldName=)[^\\}]*";
		Pattern r = Pattern.compile(pattern1);
		Matcher m = r.matcher(FilterStr);

		if (m.find()) {
			HashMap<String, Object> FilterMap = new HashMap<String, Object>(m.groupCount());

			for (int i = 0; i < m.groupCount(); i++) {

				String[] kvStr = m.group(i).split(",");
				String key = kvStr[0].split("=")[1];
				Object value = kvStr[1].split("=")[1];
				FilterMap.put(key, value);
			}
			return FilterMap;
		} else
			return null;

	}

	/**
	 * 获取第一个过滤条件 KV对象
	 * @param FilterStr
	 * @return
	 */
	public static MyEntry<String, Object> getFiltersFirst(String FilterStr) {

		String pattern1 = "(fieldName=)[^\\}]*";
		Pattern r = Pattern.compile(pattern1);
		Matcher m = r.matcher(FilterStr);

		if (m.find()) {
			String[] kvStr = m.group(0).split(",");
			String key = kvStr[0].split("=")[1];
			Object value = kvStr[1].split("=")[1];
			MyEntry<String, Object> entry = new MyEntry<String, Object>(key, value);
			return entry;
		} else
			return null;

	}

	/**
	 * 获取所有过滤条件的对象List
	 * @author zyh
	 *
	 * @param <K>
	 * @param <V>
	 */
	final static class MyEntry<K, V> implements Map.Entry<K, V> {
		private final K key;
		private V value;

		public MyEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V old = this.value;
			this.value = value;
			return old;
		}
	}

	/**
	 * 测试
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {

		boolean start = false;
		if (start) {//批量更新
			int reqCount = startIndex + 1000000;

			try {
				long begin = System.currentTimeMillis();
				List<DBObject> dbObjects = new ArrayList<DBObject>();
				for (int i = startIndex; i < reqCount; i++) {
					Document dt = new Document();
					dt.putAll((new InsertObject()).getLogClickDt());
					dt.put("requestTimeID", Integer.toString(i));
					dt.put("requestID", Integer.toString(i));
					insertMany(LogConstants.MONGODB_WIN, dt);
					insertMany(LogConstants.MONGODB_EXPOS, dt);
					insertMany(LogConstants.MONGODB_CLICK, dt);

					insertLogSummary(dt, "wincount");
					insertLogSummary(dt, "exposcount");
					insertLogSummary(dt, "clickcount");

					if (i % batchProcessReqCount == 0)
						Thread.sleep(reqSleep);
				}
				System.out.println("[" + new Date(System.currentTimeMillis()) + "] AllReq spend Time:"
						+ (System.currentTimeMillis() - begin));

				System.out.println("[" + new Date(System.currentTimeMillis()) + "] All spend Time:"
						+ (System.currentTimeMillis() - begin));

			} catch (Exception e) {
				if (e instanceof MongoWriteException) {
					String message = e.getMessage();
					if (message == null) {
						message = "";
					}
					if (message.startsWith("E11000")) {
						System.out.println("no problem");
					} else {
						e.printStackTrace();
					}
				} else {
					e.printStackTrace();
				}
			}
		} else {

			//读日志批量跟新
			logProductor.productStart();
		}

	}

}
