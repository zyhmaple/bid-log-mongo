package com.adrich.bidlog.consume;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.model.BuildLogModel;


/**
 * 同步，多线程 ，solrCloud
 * <p>
 * Title:
 * </p>
 * <p>
 * Description: 待修改内容： 1、用bulk的话队列可以去掉 2、存储等参数去掉
 * 
 * 批量提交的时候走队列，多次消费一次提交
 * 
 * </p>
 *
 * @Author:wang_baoquan
 * @date 2017年7月24日 下午6:06:55
 */

public enum SspConsumer {
	;
	static {
		//Log4jUtils.configureAndWatch();
	}
	/**
	 * 遗留问题记录： 1、ssp merge的性能测试 2、重启节点重新分配分配？ 3、kafka超时、超出分区范围？ 4、http端口登录验证
	 * 5、如果ssp合并的参数为空，则不执行script 6、片的分布：现在总数来分布，希望索引内部均匀分布
	 */
	private static final Logger logger = LoggerFactory.getLogger(SspConsumer.class);

	private static final AtomicBoolean running = new AtomicBoolean();

	private static final String TOPICS_KEY = "topics";

	private static final String PARTITION_NUMS_KEY = "adrich.partition.nums";

	public static final String SCRIPT_ID = "merge-user-info";

//	public static volatile long starT = 0;
//	public static volatile long insT = 0;
	
	private static Properties getKafkaProperties() throws Throwable {
		InputStream resourceStream = null;
		try {
			resourceStream = SspConsumer.class.getResourceAsStream("/kafka-consume.properties");
			Properties props = new Properties();
			props.load(resourceStream);
			return props;
		} finally {
			if (resourceStream != null) {
				resourceStream.close();
			}
		}
	}

	private static void start() throws Throwable {
		Properties props = getKafkaProperties();

		String topics = props.getProperty(TOPICS_KEY);
		Set<String> topicsList = new HashSet<>(Arrays.asList(topics.split(",")));

		String partitionNums = props.getProperty(PARTITION_NUMS_KEY);
		Set<Integer> partitionNumsList = null;
		if (partitionNums != null) {
			partitionNums = partitionNums.trim();
			if (!partitionNums.isEmpty()) {
				partitionNumsList = new HashSet<>();
				for (String pn : partitionNums.split(",")) {
					partitionNumsList.add(Integer.parseInt(pn));
				}
			}
		}

		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = new KafkaConsumer<>(props);
			
			consumer.subscribe(topicsList);
            
			running.set(true);
			logger.info("Started.");
			
			while (running.get()) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				BuildLogModel sjm = new BuildLogModel();
				/**
				 * 业务处理异常，重新读取
				 */
				// long currOffset = -1;
				// String currTopic = "";
				int partition = 0;
				long currSize = 0;
				long insT = 0;
				for (ConsumerRecord<String, String> record : records) {
					String value = record.value();
					/*
					System.out.println(String.format("topic = %s,  offset = %s, key = %s, value = %s,time = %d", record.topic(),
							record.offset(), record.key(), value,record.timestamp()));*/
					if(currSize == 0){
						insT = record.timestamp();
						partition = record.partition();
					}
					currSize++;
					try {
						BidLogExtract.extractInfo(value, sjm);
                        /**
                        *@TODO业务处理
                        */
					} catch (Exception ex) {
						logger.error("param:[" + value + "]", ex);
					}
				}
				
				if (currSize != 0) {
					if(sjm.getImpList().size()==0){
						logger.error("impList size is zero.");
						continue;
					}
					
				}
			}

		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	

	public static void main(String[] args) throws Throwable {
		
		logger.info("Starting ...");
		try {
			
			start();
		} catch (Throwable e) {
			logger.error("error", e);
			throw e;
		} finally {
			//client.close();
		}
	}
}
