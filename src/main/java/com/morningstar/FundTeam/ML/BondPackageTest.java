package com.morningstar.FundTeam.ML;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.testng.annotations.Test;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageTest{

	/**
	 * @author stefan.hou
	 * @param args
	 */
	
	static String currentTime = Base.currentSysTime();
	
	static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	
	static String testLogNameCase1 = "completenessTestResult-" + currentTime + "." + "log";
	static String testLogNameCase2 = "formatTestResult-" + currentTime + "." + "log";
	static String testLogNameCase3 = "uncompleteCalculationPerfIdTestResult-" + currentTime + "." + "log";
	static String testLogNameCase4 = "contentTestResult-" + currentTime + "." + "log";
	static String testLogNameCase5 = "duplicateTestResult-" + currentTime + "." + "log";
		
	static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/20140314/PLP320XD.MSTBONDS";
	
	@Test(description = "Testing:The completeness of Bond sample file")
	public static void testBondFileComplete()throws Exception{
//统计Bond文件中有多少个即没有CUSIP也没有ISIN的行
		int lineRange = 0;
		int separatorCount = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileCUSIP = "";
		String fileISIN = "";
		String fileYieldRate = "";
		String fileYieldAsAtDate = "";
		
		String startTime1 = Base.currentSysTime();
		System.out.println("[TestForCompletment]Begin to test no CUSIP and ISIN in each lines,please wait.......");
		System.out.println("[TestForCompletment]Test at:" + startTime1);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test no CUSIP and ISIN in each lines,start at:" + startTime1);
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		for(int lineNum=2;lineNum<lineRange;lineNum++){
			fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
			element = fileLineContent.split("~",16);
			fileCUSIP = element[0];
			fileISIN = element[2];
			if(fileCUSIP.isEmpty()==true&&fileISIN.isEmpty()==true){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[NOTICE]" + "Line number is:" + lineNum + "   " + "No CUSIP and ISIN in this line!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
			}
		}
		
//统计Bond文件中是否有某一行无Yield数据
		String startTime2 = Base.currentSysTime();
		System.out.println("[TestForCompletment]Begin to test no Yield and Related Date in each lines,please wait.......");
		System.out.println("[TestForCompletment]Test at:" + startTime2);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test no Yield and Related Date in each lines,start at:" + startTime2);
		fileYieldAsAtDate = element[8];
		fileYieldRate = element[9];
		for(int lineNum = 2;lineNum<lineRange;lineNum++){
			if(fileYieldAsAtDate != null){
				if(fileYieldAsAtDate.equals("")){
					if(!fileYieldRate.equals("")){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldAsAtDate while YieldRate isn't empty in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+fileLineContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
					}
				}
				if(fileYieldRate.equals("")){
					if(!fileYieldAsAtDate.equals("")){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldRate while YieldAsAtDate isn't empty in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+fileLineContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
					}
				}
				if(fileYieldAsAtDate.equals("") && fileYieldRate.equals("")){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldRate and YieldAsAtDate in this line!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
				}
			}			
		}
		
//测试每行是否有多余的"~"
		String startTime3 = Base.currentSysTime();
		System.out.println("[TestForCompletment]Begin to test every line must have 15 '~',please wait.......");
		System.out.println("[TestForCompletment]Test at:" + startTime3);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test every line must has 16 '~',start at:" + startTime3);
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		for(int lineNum = 2;lineNum<lineRange;lineNum++){
			fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
			separatorCount = Helper.getMatchCount("~",fileLineContent);
			if(separatorCount != 15){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "There has no 16 '~' in this line!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Actural count of separator is: "+separatorCount);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
			}
		}			
		String endTime = Base.currentSysTime();
		System.out.println("[FINISHED]BondFileComplete Test has finished,end at: " + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FINISHED]BondFileComplete Test has finished,end at: " + endTime);
	}
		
	@Test(description = "Testing:The format of all datas in each line!")
	public static void testBondFileFormat() throws Exception{
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileCUSIP = "";
		String fileISIN = "";
		String fileSecId = "";
		String fileYieldRate = "";
		String fileYieldAsAtDate = "";
		String fileBetaRate = "";
		String fileBetaAsAtDate = "";
		
		String startTime = Base.currentSysTime();
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		System.out.println("[TestForDataFormat]Begin to test the Bond File's Data format in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestForDataFormat]Begin to test the Bond File's Data format in each lines,start at:" + startTime);
		for(int lineNum=2;lineNum<lineRange;lineNum++){
			fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
			element = fileLineContent.split("~",16);
			fileCUSIP = element[0];
			fileISIN = element[2];
			fileSecId = element[7];
			fileBetaAsAtDate = element[10];
			fileBetaRate = element[11];
			
//测试CUSIP
			if(fileCUSIP != null){
				if(fileCUSIP.length() != 0 && fileCUSIP.length() != 9 ){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of CUSIP in stock file isn't 9 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of CUSIP in Stock file is:" + fileCUSIP.length());
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");				
				}
			}
//测试ISIN
			if(fileISIN != null){
				if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of ISIN in stock file isn't 12 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ISIN in Stock file is:" + fileISIN.length());
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}
			}
//测试SecId
			if(fileSecId != null){
				if(fileSecId.length() != 0 && fileSecId.length() != 10){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of SecId in stock file isn't 10 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ISIN in Stock file is:" + fileISIN.length());
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}				
			}
//测试YieldRate，精度为(9,5)
			if(fileYieldRate != null){
				if(fileYieldRate.length() != 0 && Helper.isDecimal(fileYieldRate) == true && Helper.getDecimalScale(fileYieldRate) != 5){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of YieldRate in stock file isn't 5 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Actural Yield Rate is: " + fileYieldRate);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual Decimal size of YieldRate in Stock file is:" + Helper.getDecimalScale(fileYieldRate));
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}
			}
//测试YieldAsAtDate
			if(fileYieldAsAtDate != null){
				if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of YieldAsAtDate in stock file isn't 8 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of YieldAsAtDate in Stock file is:" + fileYieldAsAtDate.length());
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}
			}
//测试BetaAsAtDate
			if(fileBetaAsAtDate != null){
				if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of BetaAsAtDate in stock file isn't 8 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of BetaAsAtDate in Stock file is:" + fileYieldAsAtDate.length());
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}
			}
//测试BetaRate，精度为(9,3)
			if(fileBetaRate != null){
				if(fileBetaRate.length() != 0 && Helper.isDecimal(fileBetaRate) == true && Helper.getDecimalScale(fileBetaRate) != 3){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of BetaRate in stock file isn't 3 bytes!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Content In Stock file is:" + fileLineContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Actural Yield Rate is: " + fileBetaRate);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual Decimal size of BetaRate in Stock file is:" + Helper.getDecimalScale(fileBetaRate));
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"===========================================================");
				}
			}
		}
		String endTime =Base.currentSysTime();
		System.out.println("[FINISHED]BondFileFormat Test has finished,end at: " + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[FINISHED]BondFileFormat Test has finished,end at: " + endTime);
	}

//查找所有已知的Bond Id list不在生成的Sample file中的情况
	@Test(description = "Testing:Checking all PerformanceId need to be calculated and generated in Bond sample file")
	public static void containCheck() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForCalculatePerformanceIdCompletment]Begin to check PerformanceId which can't be calculated and generated in Bond sample file,please wait.......");
		System.out.println("[TestForCalculatePerformanceIdCompletment]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestForCalculatePerformanceIdCompletment]Begin to check PerformanceId which can't be calculated and generated in Bond sample file,start at:" + startTime);
		List<String> perfIdListInfile = new ArrayList<String>();
		List<String> perfIdListInDb = new ArrayList<String>();
		int duplicatePerfIdCount = 0;
		String sqlToGetPerfIdList = "SELECT PerformanceId from CurrentData.dbo.MLBetaPerformanceAttributes WHERE MLUniverseType = '3'";
		String sqlToGetBondIdCount = "SELECT COUNT(PerformanceId) from CurrentData.dbo.MLBetaPerformanceAttributes WHERE MLUniverseType = '3'";
		String bondIdCountOriginal = DBCommons.getData(sqlToGetBondIdCount, Database.MsSQL3);
		int bondIdCountInDb = Integer.parseInt(bondIdCountOriginal);
		int acturalBondIdCount = Helper.getTotalLinesOfFile(bondFilePath)-2;
		perfIdListInfile = loadAssignDataToList(7);
		perfIdListInDb = DBCommons.getDataList(sqlToGetPerfIdList, Database.MsSQL3);

//开始测试判断(只有当生成在BondFile中的PerfId的数量小于DB端时才开始测试)
		if(bondIdCountInDb > acturalBondIdCount){
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]Bond Sample file contains less amount of PerformanceId!");
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]The count of PerformanceId in DB is:" + bondIdCountInDb);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]The count of PerformanceId in Bond Sample file is:" + acturalBondIdCount);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3,"===========================================================");
			for(String perfIdInDb :perfIdListInDb){
				duplicatePerfIdCount = Collections.frequency(perfIdListInfile, perfIdInDb);
				if(duplicatePerfIdCount == 0){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[WARNING]PerformanceId= " + perfIdInDb + " isn't generated in Bond sample file!");
				}
			}
		}
		
		if(bondIdCountInDb < acturalBondIdCount){
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]Bond Sample file contains more amount of PerformanceId!");
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]The count of PerformanceId in DB is:" + bondIdCountInDb);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[NOTICIFICATION]The count of PerformanceId in Bond Sample file is:" + acturalBondIdCount);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3,"===========================================================");
			for(String perfIdInFile :perfIdListInfile){
				duplicatePerfIdCount = Collections.frequency(perfIdListInDb, perfIdInFile);
				if(duplicatePerfIdCount == 0){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[WARNING]PerformanceId= " + perfIdInFile + " isn't contained in DB side!");
				}
			}
		}
		
		String endTime = Base.currentSysTime();
		System.out.println("Checking has finished,end at: " + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Checking has finished,end at: " + endTime);
	}
	
	
	@Test(description = "Testing:The content of all datas in each line!")
	public static void testBondFileContent() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataContent]Begin to test the Bond File's Data content in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's Data content in each lines,start at:" + startTime);
		
		testBondFileContent4CUSIP();
		testBondFileContent4ISIN();
		testBondFileContent4YieldAsAtDate();
		testBondFileContent4YieldRate();
		testBondFileContent4BetaAsAtDate();
		testBondFileContent4BetaRate();
		
		String endTime = Base.currentSysTime();
		System.out.println("[TestForDataContent]Bond File's Data content has finished.......");
		System.out.println("[TestForDataContent]End at:" + endTime);
	}
	
//	@Test(description = "Testing:The content of all datas in each line!")
//	public static void testBondFileContent() throws Exception{
//		String startTime = Base.currentSysTime();
//		int lineRange = 0;
//		String[] element = null;
//		String fileLineContent = "";
//		String fileCUSIP = "";
//		String fileISIN = "";
//		String fileSecId = "";
//		String fileYieldRate = "";
//		String fileYieldAsAtDate = "";
//		String fileBetaRate = "";
//		String fileBetaAsAtDate = "";
//		
//		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
//		System.out.println("[TestForDataContent]Begin to test the Bond File's Data content in each lines,please wait.......");
//		System.out.println("[TestForDataContent]Test at:" + startTime);
//		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's Data content in each lines,start at:" + startTime);
//
//		
//		HashMap<String,String> secId2CUSIP = new HashMap<String,String>();
//		HashMap<String,String> secId2ISIN = new HashMap<String,String>();
//		HashMap<String,String> secId2YieldAsAtDate = new HashMap<String,String>();
//		HashMap<String,String> secId2SecurityYieldRate = new HashMap<String,String>();
//		HashMap<String,String> secId2BetaAsAtDate = new HashMap<String,String>();
//		HashMap<String,String> secId2BetaRate = new HashMap<String,String>();
//		
//		List<String> secIdBuilderList = new ArrayList<String>();
//		secIdBuilderList = buildSecIdBuffer(7,100000);
//				
//		for(int lineNum = 2;lineNum<lineRange;lineNum++){
//			fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
//			element = fileLineContent.split("~",16);
//			fileCUSIP = element[0];
//			fileISIN = element[2];
//			fileSecId = element[7];
//			fileYieldAsAtDate = element[8];
//			fileYieldRate = element[9];
//			fileBetaAsAtDate = element[10];
//			fileBetaRate = element[11];
//			
////测试CUSIP
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetCUSIP = "SELECT InvestmentId,CUSIP FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secIdList + ")";
//				secId2CUSIP = DBCommons.getDataHashMap(sqlToGetCUSIP, Database.Vertica2);
//				String actCUSIPInDb = secId2CUSIP.get(fileSecId);
//				if(actCUSIPInDb != null){
//					String realCUSIPInDb = actCUSIPInDb.trim();
//					if(realCUSIPInDb.equals(fileCUSIP)){
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "CUSIP not mapping!");
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual CUSIP in DB is:" + realCUSIPInDb);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual CUSIP in BondFile is:" + fileCUSIP);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//					}
//				}
////释放secId2CUSIP内存空间
//				if(!secId2CUSIP.isEmpty()){
//					secId2CUSIP.clear();
//					System.out.println("MAP secId2CUSIP is cleared... ...");
//				}
//			}
//			
////测试ISIN
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetISIN = "SELECT InvestmentId,ISIN FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secIdList + ")" ;
//				secId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
//				String actISINInDb = secId2ISIN.get(fileSecId);
//				if(actISINInDb != null){
//					String realISINInDb = actISINInDb.trim();
//					if(realISINInDb.equals(fileISIN)){
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual ISIN in DB is:" + realISINInDb);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual ISIN in BondFile is:" + fileISIN);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//					}
//				}
////释放secId2ISIN内存空间
//				if(!secId2ISIN.isEmpty()){
//					secId2ISIN.clear();
//					System.out.println("MAP secId2ISIN is cleared... ...");
//				}
//			}
//						
////测试YieldAsAtDate
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
//				secId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica3);
//				String actYieldAsAtDateInDb = secId2YieldAsAtDate.get(fileSecId);
//				if(actYieldAsAtDateInDb != null){
//					if(!actYieldAsAtDateInDb.isEmpty() && !fileYieldAsAtDate.isEmpty()){
//						Date realYieldAsAtDateInDb = Helper.dateParse(actYieldAsAtDateInDb, "yyyy-MM-dd");
//						Date realYieldAsAtDateInFile = Helper.dateParse(fileYieldAsAtDate, "yyyyMMdd");
//						if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldAsAtDate not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldAsAtDate in DB is:" + actYieldAsAtDateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldAsAtDate in BondFile is:" + fileYieldAsAtDate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//						}
//					}				
//				}
////释放secId2YieldAsAtDate内存空间
//				if(!secId2YieldAsAtDate.isEmpty()){
//					secId2YieldAsAtDate.clear();
//					System.out.println("MAP secId2YieldAsAtDate is cleared... ...");
//				}
//			}
//			
//			
////测试YieldRate
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
//				secId2SecurityYieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica3);
//				String actYieldRateInDb = secId2SecurityYieldRate.get(fileSecId);
//				if(actYieldRateInDb != null){
//					double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
//					String realYieldRateInDb = Helper.addZeroForDouble(DactYieldRateInDb, "0.00000");
//					if(!realYieldRateInDb.equals(fileYieldRate)){
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in DB is:" + actYieldRateInDb);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in BondFile is:" + fileYieldRate);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//					}
//				}
////释放secId2SecurityYieldRate内存空间
//				if(!secId2SecurityYieldRate.isEmpty()){
//					secId2SecurityYieldRate.clear();
//					System.out.println("MAP secId2SecurityYieldRate is cleared... ...");
//				}
//			}
//			
//			
////测试BetaAsAtDate
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
//				secId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica3);
//				String actBetaAsAtDateInDb = secId2BetaAsAtDate.get(fileSecId);
//				if(actBetaAsAtDateInDb != null){
//					if(!actBetaAsAtDateInDb.isEmpty() && !fileBetaAsAtDate.isEmpty()){
//						Date realBetaAsAtDateInDb = Helper.dateParse(actBetaAsAtDateInDb, "yyyy-MM-dd");
//						Date realBetaAsAtDateInFile = Helper.dateParse(fileBetaAsAtDate, "yyyyMMdd");
//						if(!realBetaAsAtDateInDb.equals(realBetaAsAtDateInFile)){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "BateAsAtDate not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BateAsAtDate in DB is:" + actBetaAsAtDateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BateAsAtDate in BondFile is:" + fileBetaAsAtDate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//						}
//					}				
//				}
////释放secId2BetaAsAtDate内存空间
//				if(!secId2BetaAsAtDate.isEmpty()){
//					secId2BetaAsAtDate.clear();
//					System.out.println("MAP secId2BetaAsAtDate is cleared... ...");
//				}
//			}
//						
////测试BateRate
//			for(String secIdList :secIdBuilderList){
//				String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
//				secId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica3);
//				String actBetaRateInDb = secId2BetaRate.get(fileSecId);
//				if(actBetaRateInDb != null){
//					double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
//					String realBetaRateInDb = Helper.addZeroForDouble(DactBetaRateInDb, "0.000");
//					if(!realBetaRateInDb.equals(fileBetaRate)){
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaRate not mapping!");
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BetaRate in DB is:" + actBetaRateInDb);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BetaRate in BondFile is:" + fileBetaRate);
//						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
//					}
//				}
////释放secId2BetaRate内存空间
//				if(!secId2BetaRate.isEmpty()){
//					secId2BetaRate.clear();
//					System.out.println("MAP secId2BetaRate is cleared... ...");
//				}
//			}						
//		}		
//	}
	
	@Test(description = "Testing:The CUSIP content of all datas in each line!")
	public static void testBondFileContent4CUSIP() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileCUSIP = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's CUSIP in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's CUSIP in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2CUSIP = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetCUSIP = "SELECT InvestmentId,CUSIP FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secIdList + ")";
			secId2CUSIP = DBCommons.getDataHashMap(sqlToGetCUSIP, Database.Vertica1);
//测试CUSIP			
			for(int lineNum=2;lineNum<lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileCUSIP = element[0];
				fileSecId = element[7];
				if(secId2CUSIP.containsKey(fileSecId)){
					String actCUSIPInDb = secId2CUSIP.get(fileSecId);
					if(actCUSIPInDb != null){
						String realCUSIPInDb = actCUSIPInDb.trim();
						if(realCUSIPInDb.equals(fileCUSIP)){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "CUSIP not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual CUSIP in DB is:" + realCUSIPInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual CUSIP in BondFile is:" + fileCUSIP);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
						}
					}
				}
			}
//释放secId2CUSIP内存空间
			secId2CUSIP.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File CUSIP testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File CUSIP testing has finished,end at:" + endTime);
	}
	
	@Test(description = "Testing:The ISIN content of all datas in each line!")
	private static void testBondFileContent4ISIN() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileISIN = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's ISIN in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's ISIN in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2ISIN = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetISIN = "SELECT InvestmentId,ISIN FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secIdList + ")" ;
			secId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica1);
//测试ISIN			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileISIN = element[2];
				fileSecId = element[7];
				if(secId2ISIN.containsKey(fileSecId)){
					String actISINInDb = secId2ISIN.get(fileSecId);
					if(actISINInDb != null){
						String realISINInDb = actISINInDb.trim();
						if(realISINInDb.equals(fileISIN)){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual ISIN in DB is:" + realISINInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual ISIN in BondFile is:" + fileISIN);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
						}
					}
				}
			}
//释放secId2ISIN内存空间
			secId2ISIN.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File ISIN testing has finished,end at:" + endTime);
	}
	
	@Test(description = "Testing:The YieldAsAtDate content of all datas in each line!")
	private static void testBondFileContent4YieldAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileYieldAsAtDate = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's YieldAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's YieldAsAtDate in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2YieldAsAtDate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
			secId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica1);
//测试YieldAsAtDate			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileSecId = element[7];
				fileYieldAsAtDate = element[8];				
				if(secId2YieldAsAtDate.containsKey(fileSecId)){
					String actYieldAsAtDateInDb = secId2YieldAsAtDate.get(fileSecId);
					if(actYieldAsAtDateInDb != null){
						if(!actYieldAsAtDateInDb.isEmpty() && !fileYieldAsAtDate.isEmpty()){
							Date realYieldAsAtDateInDb = Helper.dateParse(actYieldAsAtDateInDb, "yyyy-MM-dd");
							Date realYieldAsAtDateInFile = Helper.dateParse(fileYieldAsAtDate, "yyyyMMdd");
							if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldAsAtDate not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldAsAtDate in DB is:" + actYieldAsAtDateInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldAsAtDate in BondFile is:" + fileYieldAsAtDate);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
							}
						}				
					}
				}
			}
//释放secId2YieldAsAtDate内存空间
			secId2YieldAsAtDate.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File YieldAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File YieldAsAtDate testing has finished,end at:" + endTime);
	}
	
	@Test(description = "Testing:The YieldRate content of all datas in each line!")
	private static void testBondFileContent4YieldRate() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileYieldRate = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's YieldRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's YieldRate in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2SecurityYieldRate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
			secId2SecurityYieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica1);
//测试YieldRate			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileSecId = element[7];
				fileYieldRate = element[9];				
				if(secId2SecurityYieldRate.containsKey(fileSecId)){
					String actYieldRateInDb = secId2SecurityYieldRate.get(fileSecId);
					if(actYieldRateInDb != null){
						Double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
						Double DrealYieldRateInDb = Helper.setDoublePrecision(DactYieldRateInDb, 5, BigDecimal.ROUND_HALF_DOWN);
						Double DrealYieldRateInFile = Double.parseDouble(fileYieldRate);
						if(!DrealYieldRateInDb.equals(DrealYieldRateInFile)){
							if(DrealYieldRateInFile-DrealYieldRateInDb != 9.999999999621423E-6){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in DB is:" + actYieldRateInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in BondFile is:" + fileYieldRate);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
							}
						}				
					}
				}
			}
//释放secId2SecurityYieldRate内存空间
			secId2SecurityYieldRate.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File YieldRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File YieldRate testing has finished,end at:" + endTime);
	}
	
	@Test(description = "Testing:The BetaAsAtDate content of all datas in each line!")
	private static void testBondFileContent4BetaAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileBetaAsAtDate = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's BetaAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's BetaAsAtDate in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2BetaAsAtDate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
			secId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica1);
//测试BetaAsAtDate			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileSecId = element[7];
				fileBetaAsAtDate = element[10];				
				if(secId2BetaAsAtDate.containsKey(fileSecId)){
					String actBetaAsAtDateInDb = secId2BetaAsAtDate.get(fileSecId);
					if(actBetaAsAtDateInDb != null){
						if(!actBetaAsAtDateInDb.isEmpty() && !fileBetaAsAtDate.isEmpty()){
							Date realYieldAsAtDateInDb = Helper.dateParse(actBetaAsAtDateInDb, "yyyy-MM-dd");
							Date realYieldAsAtDateInFile = Helper.dateParse(fileBetaAsAtDate, "yyyyMMdd");
							if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldAsAtDate not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BetaAsAtDate in DB is:" + actBetaAsAtDateInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual BetaAsAtDate in BondFile is:" + fileBetaAsAtDate);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
							}
						}				
					}
				}
			}
//释放secId2BetaAsAtDate内存空间
			secId2BetaAsAtDate.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File BetaAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File BetaAsAtDate testing has finished,end at:" + endTime);
	}
	
	@Test(description = "Testing:The BetaRate content of all datas in each line!")
	private static void testBondFileContent4BetaRate() throws Exception{
		final String startTime = Base.currentSysTime();
		int lineRange = 0;
		String[] element = null;
		String fileLineContent = "";
		String fileBetaRate = "";
		String fileSecId = "";
		
		lineRange = Helper.getTotalLinesOfFile(bondFilePath);
		
		System.out.println("[TestForDataContent]Begin to test the Bond File's BetaRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDataContent]Begin to test the Bond File's BetaRate in each lines,start at:" + startTime);
		
		
		HashMap<String,String> secId2BetaRate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		secIdBuilderList = buildSecIdBuffer(7,100000);
		
		for(String secIdList :secIdBuilderList){
			String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secIdList + ")";
			secId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica1);
//测试BetaRate			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileLineContent = Helper.readFileInLines(bondFilePath, lineNum);
				element = fileLineContent.split("~",16);
				fileSecId = element[7];
				fileBetaRate = element[11];				
				if(secId2BetaRate.containsKey(fileSecId)){
					String actBetaRateInDb = secId2BetaRate.get(fileSecId);
					if(actBetaRateInDb != null){
						Double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
						Double DrealBetaRateInDb = Helper.setDoublePrecision(DactBetaRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
						Double DrealBetaRateInFile = Double.parseDouble(fileBetaRate);
						if(!DrealBetaRateInDb.equals(DrealBetaRateInFile)){
							if(DrealBetaRateInFile - DrealBetaRateInDb != 0.001000000000000334){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Original Bond file is:" + fileLineContent);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in DB is:" + actBetaRateInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Actual YieldRate in BondFile is:" + fileBetaRate);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "==============================================");
							}							
						}				
					}
				}
			}
//释放secId2BetaRate内存空间
			secId2BetaRate.clear();
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File BetaRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Bond File BetaRate testing has finished,end at:" + endTime);
	}
	
//测试Duplicate	
	private static void testBondFileDuplicate() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDuplicateData]Begin to test no duplicate lines in Fund sample file,please wait.......");
		System.out.println("[TestForDuplicateData]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "[TestForDuplicateData]Begin to test no duplicate lines in Fund sample file,start at:" + startTime);
		
		List<String> fileContentList = new ArrayList<String>();
		fileContentList = loadLineDataToList();
		for(String fileLineContent : fileContentList){
			int duplicateLineCount = Collections.frequency(fileContentList, fileLineContent);
			if(duplicateLineCount>1){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:Duplicate lines number is:" + duplicateLineCount);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Failed original content is:" + fileLineContent);
			}
		}
		
		String endTime = Base.currentSysTime();
		System.out.println("Testing has finished,end at: " + endTime);
	}
	
//将指定列数据Load到一个List中
	private static List<String> loadAssignDataToList(int columnNum) throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(bondFilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(bondFilePath, i);
			String[] element = fileLineContent.split("~",16);
			String str = element[columnNum];
			list.add(str);			
		}
		return list;
	}
		
//因为Bond数据过多，因此需要批量执行SQL
	private static List<String> buildSecIdBuffer(int columnNum,int bufferSize) throws Exception{
		List<String> list = new ArrayList<String>();
		List<String> perfIdList = new ArrayList<String>();
		perfIdList = loadAssignDataToList(columnNum);
		
		int flag = 0;
		String rebuildStr = "";
		for(String perfId : perfIdList){
			flag++;
			if(flag%bufferSize != 0){
				if(flag == bufferSize-1){
					rebuildStr += "'" + perfId + "'";
				}
				else{
					rebuildStr += "'" + perfId + "',";
				}
			}
			else{
				list.add(rebuildStr);
				flag=0;
				rebuildStr = "'" + perfId + "',";
			}
		}
		return list;
	}

			
//将每行数据LOAD到List中	
	public static List<String> loadLineDataToList() throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(bondFilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(bondFilePath, i);
			list.add(fileLineContent);
		}
		return list;
	}
	
	public static void main(String[] args) throws Exception{
		testBondFileComplete();
		testBondFileFormat();
		containCheck();
		testBondFileContent();
		testBondFileDuplicate();
	}

}
