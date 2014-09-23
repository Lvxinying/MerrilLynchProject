package com.morningstar.FundTeam.ML;

/**
 * @author shou
 *
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class HybridFundPackageDuplicateTest {

	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/HybridFundPackage/";
	private static String testLogNameCase = "duplicateTestResult-" + currentTime + "." + "log";
	
	private static String hybridFundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/HybridDemoFile/StagingENV/20140830/PLP320XB.MSHYBFND";
	
	private static void testFundFileDuplicate(){
		int count = 1;
		int duplicateCount = 0;
		Map<String,Integer> duplicateLineContentMap = new HashMap<String,Integer>();
		Map<String,Integer> duplicatePerformanceIdMap = new HashMap<String,Integer>();
		List<String> fileLineList = new ArrayList<String>();
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataFormat]Begin to test the Hybrid Fund File's Duplicate content in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Hybrid Fund File's Duplicate content in each lines,start at:" + startTime);
//Get file data stream from Fund package to a list
				try {
					Long readfileStartTime = System.currentTimeMillis();
					fileLineList = Helper.readFileList(hybridFundFilePath);
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
//No duplicate line content
//Set a HashMap like duplicate counter
					if(duplicateLineContentMap.containsKey(lineContentStr)){
						duplicateLineContentMap.put(lineContentStr, duplicateLineContentMap.get(lineContentStr).intValue()+1);
					}else{
						duplicateLineContentMap.put(lineContentStr, count);
					}
				}
			}
		}
//print duplicate
		for(Entry<String, Integer> entry:duplicateLineContentMap.entrySet()){
			duplicateCount = entry.getValue();
			if(duplicateCount > 1){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:Duplicate counts in Fund Package is:" + duplicateCount);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Duplicate original content is:"+entry.getKey());
			}
		}
		
//No duplicate PerformanceId
		List<String> PerformanceIdList = new ArrayList<String>();
		try {
			long startLoadPerfIdTime = System.currentTimeMillis();
			PerformanceIdList = loadAssignDataToList(7);
			long endLoadPerfIdTime = System.currentTimeMillis() - startLoadPerfIdTime;
			if(!PerformanceIdList.isEmpty()){
				System.out.println("[INFO]Get PerformanceId list finished,total cost: " + endLoadPerfIdTime + " ms");
			}else{
				System.out.println("[ERROR]Get PerformanceId list failed,because PerformanceIdList is empty!");
			}
			for(String perfIdStr:PerformanceIdList){
				if(perfIdStr != null && !perfIdStr.isEmpty()){
//Set a HashMap like duplicate counter
					if(duplicatePerformanceIdMap.containsKey(perfIdStr)){
						duplicatePerformanceIdMap.put(perfIdStr, duplicatePerformanceIdMap.get(perfIdStr).intValue()+1);
					}else{
						duplicatePerformanceIdMap.put(perfIdStr, count);
					}
				}
			}
//print duplicate
			for(Entry<String, Integer> entry:duplicatePerformanceIdMap.entrySet()){
				duplicateCount = entry.getValue();
				if(duplicateCount > 1){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:Duplicate PerformanceId counts in HybridFund Package is:" + duplicateCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Duplicate PerformanceId is:"+entry.getKey());
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static List<String> loadAssignDataToList(int columnNum) throws Exception{
		List<String> resultList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>();
		String[] element = null;
		String eleStr = null;
		
		try { 
			fileLineList = Helper.readFileList(hybridFundFilePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(String lineContentStr:fileLineList){
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				if(!lineContentStr.isEmpty() || lineContentStr !=null){
					element = lineContentStr.split("~",16);
					eleStr = element[columnNum];
					resultList.add(eleStr);
				}
			}
		}
		return resultList;
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testFundFileDuplicate();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Duplicate test for HybridFund has finished, total cost: " + endTestTime + " ms");
	}
}
