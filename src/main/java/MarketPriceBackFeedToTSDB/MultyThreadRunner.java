package MarketPriceBackFeedToTSDB;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultyThreadRunner {
	static ExecutorService pool = Executors.newFixedThreadPool(8);;
	public static void startRunning(String className,TsBlobDataTypeBaseObject tsType,List<String> perfIdList,int bufferSize){		 
		MultyThread mt = new MultyThread(className, tsType, perfIdList, bufferSize);
		if(mt != null){
			pool.execute(mt);
			System.out.println(">>>TsType="+tsType+" QA job start!");
		}		
	}
	
	public static void shutDownThreadPool(){
		if(!pool.isShutdown()){
			pool.shutdown();
		}
	}
}
