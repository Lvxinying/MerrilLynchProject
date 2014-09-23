package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageContainCheck {
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	private static String testLogNameCase = "containCheckTestResult-" + currentTime + "." + "log";
	
	private static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/20140314/PLP320XD.MSTBONDS";
	
	/**
	 * @param args
	 */
	private static void containCheck(){
		String startTime = Base.currentSysTime();
		List<String> perfIdListInfile = new ArrayList<String>();
		List<String> perfIdListInDb = new ArrayList<String>();
		int duplicatePerfIdCount = 0;
		String sqlToGetPerfIdList = "SELECT PerformanceId from CurrentData.dbo.MLBetaPerformanceAttributes WHERE MLUniverseType = '3'";
		String sqlToGetBondIdCount = "SELECT COUNT(PerformanceId) from CurrentData.dbo.MLBetaPerformanceAttributes WHERE MLUniverseType = '3'";
		String bondIdCountOriginal = null;
		int acturalBondIdCount = 0;
				
		System.out.println("[TestForCalculatedPerformanceIdCompletment]Begin to check all calculated PerformanceId which can't be generated in Bond sample file,please wait.......");
		System.out.println("[TestForCalculatedPerformanceIdCompletment]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForCalculatePerformanceIdCompletment]Begin to check PerformanceId which can't be calculated and generated in Bond sample file,start at:" + startTime);
		try {
			bondIdCountOriginal = DBCommons.getData(sqlToGetBondIdCount, Database.MsSQL3);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		int bondIdCountInDb = Integer.parseInt(bondIdCountOriginal);
		try {
			acturalBondIdCount = Helper.getTotalLinesOfFile(bondFilePath)-2;
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Long startTimeLoadAssignedData = System.currentTimeMillis();
			perfIdListInfile = loadAssignDataToList(7);
			Long endTimeLoadAssignedData = System.currentTimeMillis()-startTimeLoadAssignedData;
			System.out.println("[INFO]Load InvestmentId from BondFile has finished, total cost: " + endTimeLoadAssignedData + " ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			Long startTimeGetDataFromDB = System.currentTimeMillis();
			perfIdListInDb = DBCommons.getDataList(sqlToGetPerfIdList, Database.MsSQL3);
			Long endTimeGetDataFromDB = System.currentTimeMillis()-startTimeGetDataFromDB;
			System.out.println("[INFO]Get data from DB has finished, total cost: " + endTimeGetDataFromDB/1000 + " s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(bondIdCountInDb > acturalBondIdCount){
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]Bond Sample file contains less amount of PerformanceId refer to the count that calculated in CurrentData.dbo.MLBetaPerformanceAttributes!");
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]The count of PerformanceId in DB is:" + bondIdCountInDb);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]The count of PerformanceId in Bond Sample file is:" + acturalBondIdCount);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
			for(String perfIdInDb :perfIdListInDb){
				duplicatePerfIdCount = Collections.frequency(perfIdListInfile, perfIdInDb);
				if(duplicatePerfIdCount == 0){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[WARNING]PerformanceId= " + perfIdInDb + " isn't generated in Bond sample file!");
				}
			}
		}
		
		if(bondIdCountInDb < acturalBondIdCount){
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]Bond Sample file contains more amount of PerformanceId! refer to the count that calculated in CurrentData.dbo.MLBetaPerformanceAttributes!");
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]The count of PerformanceId in DB is:" + bondIdCountInDb);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[NOTICIFICATION]The count of PerformanceId in Bond Sample file is:" + acturalBondIdCount);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
			for(String perfIdInFile :perfIdListInfile){
				duplicatePerfIdCount = Collections.frequency(perfIdListInDb, perfIdInFile);
				if(duplicatePerfIdCount == 0){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[WARNING]PerformanceId= " + perfIdInFile + " isn't contained in DB side!");
				}
			}
		}
	}
	
//将指定列数据Load到一个List中
	private static List<String> loadAssignDataToList(int columnNum) throws Exception{
		List<String> resultList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>();
		String[] element = null;
		String eleStr = null;
		
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
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
		containCheck();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Format test for Bond has finished, total cost: " + endTestTime + " ms");
	}

}
