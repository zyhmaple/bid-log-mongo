package com.adrich.bidlog.mongodb;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.Document;

import com.adrich.bidlog.util.LogConstants;

public class MongoBlockQueuePool {

	
	private static Map<String, BlockingQueue<Document>> COLLECTION_MQ_MAP = new ConcurrentHashMap<String, BlockingQueue<Document>>();
	
	// 记录各队列消耗线程数
	private static Map<String, Integer> COLLECTION_THREAD_COUNT = new ConcurrentHashMap<String, Integer>();
	
	//各日志阻塞队列
	private static BlockingQueue<Document> Expos_Queue = new LinkedBlockingQueue<Document>();
	private static BlockingQueue<Document> Click_Queue = new LinkedBlockingQueue<Document>();
	private static BlockingQueue<Document> Win_Queue = new LinkedBlockingQueue<Document>();
	private static BlockingQueue<Document> Summary_Queue = new LinkedBlockingQueue<Document>();
	private static BlockingQueue<Document> BidUser_Queue = new LinkedBlockingQueue<Document>();
	
	
	// 读取配置文件、初始化客户端、数据库、集合连接
		static {

		}
		
		public static void initQueueByBidLogType(String logName){
			
			if(COLLECTION_MQ_MAP.containsKey(logName))return;
			switch(logName){
			case LogConstants.MONGODB_WIN:
				COLLECTION_MQ_MAP.put(LogConstants.MONGODB_WIN, Win_Queue);
			case LogConstants.MONGODB_CLICK:
				COLLECTION_MQ_MAP.put(LogConstants.MONGODB_CLICK, Click_Queue);
			case LogConstants.MONGODB_EXPOS:
				COLLECTION_MQ_MAP.put(LogConstants.MONGODB_EXPOS, Expos_Queue);
			case LogConstants.MONGODB_SUMMARY:
				COLLECTION_MQ_MAP.put(LogConstants.MONGODB_SUMMARY, Summary_Queue);
			case LogConstants.MONGODB_BIDUSER:
				COLLECTION_MQ_MAP.put(LogConstants.MONGODB_BIDUSER, BidUser_Queue);
			}
		}
		
		public static BlockingQueue<Document> getQueueByBidLogType(String logName){
			if(COLLECTION_MQ_MAP.isEmpty()||!COLLECTION_MQ_MAP.containsKey(logName))
				MongoBlockQueuePool.initQueueByBidLogType(logName);
			return COLLECTION_MQ_MAP.get(logName);
		}
		
		public static Map<String, BlockingQueue<Document>> getAllQueues(){
			return COLLECTION_MQ_MAP;
		}
		
		public static int getProcessThreadCountByBidLogType(String logName){
			if(COLLECTION_THREAD_COUNT.containsKey(logName))
				return	COLLECTION_THREAD_COUNT.get(logName);
			else{
				 COLLECTION_THREAD_COUNT.put(logName,0);
				 return 0;
			}
		}
		
		public static void increateProcessThreadCountByBidLogType(String logName){
			if(COLLECTION_THREAD_COUNT.containsKey(logName))
				COLLECTION_THREAD_COUNT.put(logName, COLLECTION_THREAD_COUNT.get(logName)+1);
			else
				COLLECTION_THREAD_COUNT.put(logName,0);
		}
		
		public static int countOfNotEmptyQueueCountInSameTime(){
			
			if(COLLECTION_MQ_MAP.isEmpty())return 0;
			
			int _count = 0;
			for(BlockingQueue<Document> queue: COLLECTION_MQ_MAP.values())
			{
				if(!queue.isEmpty())
					_count++;
			}
			return _count;
		}

}
