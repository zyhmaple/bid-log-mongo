package com.adrich.bidlog.product;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adrich.bidlog.extract.BidLogExtract;
import com.adrich.bidlog.model.BuildLogModel;
import com.adrich.bidlog.mongodb.MongoDBBatchCURD;
import com.alibaba.fastjson.JSONObject;

/**
 * 根据文件大小实时读取数据
 * 
 * @author admin
 *
 */
public class logProductor {

	private long lastTimeFileSize = 0; // 上次文件大小
	private static final Logger logger = LoggerFactory.getLogger(logProductor.class);
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
	private static String logFilePath;
	private static int startIndex,endIndex;

	
	private static final String ISO8859 = "ISO-8859-1";
	private static final String UTF8 = "utf-8";

	ScheduledExecutorService schExec = Executors.newScheduledThreadPool(1);
	ExecutorService exec = Executors.newSingleThreadExecutor();
	static {
		InputStream resourceStream = null;
		try {
		// 读取配置文件
			resourceStream = logProductor.class.getResourceAsStream("/logrw-config.properties");

			Properties p = new Properties();

			p.load(resourceStream);
			
			logFilePath = p.getProperty("logrw.filepath");
			startIndex = Integer.parseInt(p.getProperty("logrw.startIndex"));
			endIndex = Integer.parseInt(p.getProperty("logrw.endIndex"));
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void realtimeShowLog(final List<File> fileList) throws Exception {

		if (fileList == null || fileList.size() == 0) {
			throw new IllegalStateException("logFile can not be null");
		}

		// 启动一个线程每2秒读取新增的日志信息
		schExec.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {

				// 获得变化部分
				try {
					for (File logFile : fileList) {
						System.out.println("Process File Name:" + logFile.getName());
						long len = logFile.length();
						if (len < lastTimeFileSize) {
							logger.info("Log file was reset. Restarting logging from start of file.");
							lastTimeFileSize = 0;
						} else {

							// 指定文件可读可写
							RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");

							// 获取RandomAccessFile对象文件指针的位置，初始位置是0
							System.out.println("RandomAccessFile文件指针的初始位置:" + lastTimeFileSize);

							randomFile.seek(lastTimeFileSize);// 移动文件指针位置

							String tmp = "";
							int fileCount = 0;
							while ((tmp = randomFile.readLine()) != null) {
								if (tmp.indexOf("\"user\":{}") > 0)
									continue;
								int beginIndex = tmp.indexOf("-[INFO] - ") + "-[INFO] - ".length();
								String temp = new String(tmp.substring(beginIndex, tmp.length()-1).getBytes(ISO8859));
								System.out.println("info : " + temp);
								//LogSvr.logMsg(new File(logFilePath), temp);
								fileCount++;
							}
							lastTimeFileSize = randomFile.length();
							randomFile.close();
							System.out.println(
									"LogFile " + logFile.getName() + " had Completed.Files Count：" + fileCount);
						}
					}

				} catch (Exception e) {
					// 实时读取日志异常，需要记录时间和lastTimeFileSize 以便后期手动补充
					logger.error(
							dateFormat.format(new Date()) + " File read error, lastTimeFileSize: " + lastTimeFileSize);
				} finally {
					// 将lastTimeFileSize 落地以便下次启动的时候，直接从指定位置获取
				}
			}

		}, 0, 10, TimeUnit.SECONDS);

	}


	public void productLog(final List<File> fileList) throws Exception {

		if (fileList == null || fileList.size() == 0) {
			throw new IllegalStateException("logFile can not be null");
		}

		// 启动一个线程日志信息
		exec.execute(new Runnable() {

			@Override
			public void run() {

				// 获得变化部分
				try {
					
					BuildLogModel sjm = new BuildLogModel();
					for (File logFile : fileList) {
						System.out.println("Process File Name:" + logFile.getName());
						long len = logFile.length();
						lastTimeFileSize = 0;
						if (len < lastTimeFileSize) {
							logger.info("Log file was reset. Restarting logging from start of file.");
							lastTimeFileSize = 0;
						} else {

							// 指定文件可读可写
							RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");

							// 获取RandomAccessFile对象文件指针的位置，初始位置是0
							System.out.println("RandomAccessFile文件指针的初始位置:" + lastTimeFileSize);

							randomFile.seek(lastTimeFileSize);// 移动文件指针位置

							String tmp = "";
							int fileCount = 0;
							while ((tmp = randomFile.readLine()) != null) {
/*								if (tmp.indexOf("\"user\":{}") > 0)
									continue;*/
								int beginIndex = tmp.indexOf("-[INFO] - ")>0 ? (tmp.indexOf("-[INFO] - ")+ "-[INFO] - ".length()):0;
								String logJsonStr = new String(tmp.substring(beginIndex, tmp.length()).getBytes(ISO8859));
								//System.out.println("info : " + logJsonStr);
								if(logJsonStr==null)
									continue;
								JSONObject json = JSONObject.parseObject(logJsonStr);
								//BidLogExtract.extractInfo(json, sjm);
								MongoDBBatchCURD.insertSet(BidLogExtract.extractInfo(json));
								fileCount++;
							}
							lastTimeFileSize = randomFile.length();
							randomFile.close();
							System.out.println(
									"LogFile " + logFile.getName() + " had Completed.Files Count：" + fileCount);
						}
					}

				} catch (Exception e) {
					// 实时读取日志异常，需要记录时间和lastTimeFileSize 以便后期手动补充
					logger.error(
							dateFormat.format(new Date()) + " File read error, lastTimeFileSize: " + lastTimeFileSize);
				} finally {
					// 将lastTimeFileSize 落地以便下次启动的时候，直接从指定位置获取
				}
			}

		});

	}
	
	public void stop() {
		if (exec != null) {
			exec.shutdown();
			logger.info("file read stop ！");
		}
	}

	public static void main(String[] args) throws Exception {

		logProductor view = new logProductor();

		List<File> fileList = new ArrayList<File>();
		for (int i = startIndex; i < endIndex; i++) {
			File tmpLogFile = new File(logFilePath+ "." + i);
			fileList.add(tmpLogFile);
		}
		view.lastTimeFileSize = 0;
		view.realtimeShowLog(fileList);

	}
	
	public static void productStart(){
		
		logProductor view = new logProductor();

		List<File> fileList = new ArrayList<File>();
		for (int i = startIndex; i < endIndex; i++) {
			File tmpLogFile = new File(logFilePath+ "." + i);
			fileList.add(tmpLogFile);
		}
		view.lastTimeFileSize = 0;
		try {
			view.productLog(fileList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}