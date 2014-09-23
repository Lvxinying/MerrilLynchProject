package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageDuplicateTest {

	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	private static String testLogNameCase = "duplicateTestResult-" + currentTime + "." + "log";
	
	private static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/StagingENV/20140830/PLP320XD.MSTBONDS";
	
	private static void testBondFileDuplicate(){
//		int lineNum = 0;
		int count = 1;
		int duplicateCount = 0;
		Map<String,Integer> duplicateMap = new HashMap<String,Integer>();
		List<String> fileLineList = new ArrayList<String>();
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataFormat]Begin to test the Bond File's Duplicate content in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Bond File's Data format in each lines,start at:" + startTime);
//Get file data stream from Bond package to a list
				try {
					Long readfileStartTime = System.currentTimeMillis();
					fileLineList = Helper.readFileList(bondFilePath);
					long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
					System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
				} catch (IOException e) {
					e.printStackTrace();
				}
		for(String lineContentStr:fileLineList){
			if(lineContentStr.contains("UHDR")){
				continue;
			}else if(lineContentStr.contains("UTRL")){
				continue;
			}else{
				if(!lineContentStr.isEmpty() || lineContentStr !=null){
//Set a HashMap like duplicate counter
					if(duplicateMap.containsKey(lineContentStr)){
						duplicateMap.put(lineContentStr, duplicateMap.get(lineContentStr).intValue()+1);
					}else{
						duplicateMap.put(lineContentStr, count);
					}
				}
			}
		}
//print duplicate
		for(Entry<String, Integer> entry:duplicateMap.entrySet()){
			duplicateCount = entry.getValue();
			if(duplicateCount > 1){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:Duplicate counts in Bond Package is:" + duplicateCount);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Duplicate original content is:"+entry.getKey());
			}
		}				
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testBondFileDuplicate();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Duplicate test for Bond has finished, total cost: " + endTestTime + " ms");
	}

}
