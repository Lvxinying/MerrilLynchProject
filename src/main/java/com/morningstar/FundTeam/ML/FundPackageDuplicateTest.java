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

public class FundPackageDuplicateTest {

	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/FundPackage/";
	private static String testLogNameCase = "duplicateTestResult-" + currentTime + "." + "log";
	
//	private static String fundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/FundDemoFile/ProductENV/20140725/PLP320XA.MSGBLFND";
	private static String fundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/FundDemoFile/StagingENV/20140830/PLP320XA.MSGBLFND";
	private static void testFundFileDuplicate(){
		int count = 1;
		int duplicateCount = 0;
		Map<String,Integer> duplicateLineContentMap = new HashMap<String,Integer>();
		Map<String,Integer> duplicateSEDOLMap = new HashMap<String,Integer>();
		Map<String,Integer> duplicateSecIdMap = new HashMap<String,Integer>();
		List<String> fileLineList = new ArrayList<String>();
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataFormat]Begin to test the Fund File's Duplicate content in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Fund File's Duplicate content in each lines,start at:" + startTime);
//Get file data stream from Fund package to a list
				try {
					Long readfileStartTime = System.currentTimeMillis();
					fileLineList = Helper.readFileList(fundFilePath);
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
		
//No duplicate SEDOL
		List<String> SEDOLList = new ArrayList<String>();
		try {
			long startLoadSEDOLTime = System.currentTimeMillis();
			SEDOLList = loadAssignDataToList(1);
			long endLoadSEDOLTime = System.currentTimeMillis() - startLoadSEDOLTime;
			if(!SEDOLList.isEmpty()){
				System.out.println("[INFO]Get SEDOL list finished,total cost: " + endLoadSEDOLTime + " ms");
			}else{
				System.out.println("[ERROR]Get SEDOL list failed,because SEDOLList is empty!");
			}
			for(String sedolStr:SEDOLList){
				if(!sedolStr.isEmpty() && sedolStr != null){
//Set a HashMap like duplicate counter
					if(duplicateSEDOLMap.containsKey(sedolStr)){
						duplicateSEDOLMap.put(sedolStr, duplicateSEDOLMap.get(sedolStr).intValue()+1);
					}else{
						duplicateSEDOLMap.put(sedolStr, count);
					}
				}
			}
//print duplicate
			for(Entry<String, Integer> entry:duplicateSEDOLMap.entrySet()){
				duplicateCount = entry.getValue();
				if(duplicateCount > 1){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:Duplicate SEDOL counts in Fund Package is:" + duplicateCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Duplicate SEDOL is:"+entry.getKey());
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//No duplicate SecId
		List<String> SecIdList = new ArrayList<String>();
		try {
			long startLoadSecIdTime = System.currentTimeMillis();
			SecIdList = loadAssignDataToList(7);
			long endLoadSecIdTime = System.currentTimeMillis() - startLoadSecIdTime;
			if(!SEDOLList.isEmpty()){
				System.out.println("[INFO]Get SecId list finished,total cost: " + endLoadSecIdTime + " ms");
			}else{
				System.out.println("[ERROR]Get SecId list failed,because SecIdList is empty!");
			}
			for(String secIdStr:SecIdList){
				if(!secIdStr.isEmpty() && secIdStr != null){
//Set a HashMap like duplicate counter
					if(duplicateSecIdMap.containsKey(secIdStr)){
						duplicateSecIdMap.put(secIdStr, duplicateSecIdMap.get(secIdStr).intValue()+1);
					}else{
						duplicateSecIdMap.put(secIdStr, count);
					}
				}
			}
//print duplicate
			for(Entry<String, Integer> entry:duplicateSecIdMap.entrySet()){
				duplicateCount = entry.getValue();
				if(duplicateCount > 1){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:Duplicate SecId counts in Fund Package is:" + duplicateCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Duplicate SecId is:"+entry.getKey());
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
			fileLineList = Helper.readFileList(fundFilePath);
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
		System.out.println("[INFO]Duplicate test for Fund has finished, total cost: " + endTestTime + " ms");
	}
}
