package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class HybridFundPackageContentTest {
	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/HybridFundPackage/";
	private static String testLogNameCase = "contentTestResult-" + currentTime + "." + "log";
	
	private static String hybridFundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/HybridDemoFile/StagingENV/20140830/PLP320XB.MSHYBFND";
		
	private static void testFundFileContent() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's Data content in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's Data content in each lines,start at:" + startTime);
		
		testHybridFundFileContent4ISIN();
		testHybridFundFileContent4YieldAsAtDate();
		testHybridFundFileContent4YieldRate();
		testHybridFundFileContent4BetaAsAtDate();
		testHybridFundFileContent4BetaRate();
		testHybridFundFileContent4ProxyId();
//		testHybridFundFileContent4ProxyName();(不测试ProxyName)
		
		String endTime = Base.currentSysTime();
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Content test has finished,end at:" + endTime);
	}	
	
	private static void testHybridFundFileContent4ISIN() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileISIN = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's ISIN in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's ISIN in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> perfId2ISIN = new HashMap<String,String>();
		String perfIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		perfIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!perfIdBuildStr.isEmpty()){
			System.out.println("[INFO]PerformanceId String has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building PerformanceId String failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(hybridFundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetISIN = "SELECT PerformanceId,ISIN FROM dbo.PerformanceIdDimension WHERE PerformanceId IN ("+ perfIdBuildStr + ")";
		long startTimePerfId2ISIN= System.currentTimeMillis();
		perfId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
		long endTimePerfId2ISIN = System.currentTimeMillis()-startTimePerfId2ISIN;
		if(!perfId2ISIN.isEmpty()){
			System.out.println("[INFO]Get PerfId->ISIN map finished, total cost: " + endTimePerfId2ISIN/1000+" s");
		}else{
			System.out.println("[ERROR]Get PerfId->ISIN map failed!Because this Map is empty");
		}			
//测试ISIN	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileISIN = element[2];
				fileSecId = element[7];
				if(!fileISIN.isEmpty() && !fileSecId.isEmpty()){
					if(perfId2ISIN.containsKey(fileSecId)){
						String actISINInDb = perfId2ISIN.get(fileSecId);
						if(actISINInDb != null){
							String realISINInDb = actISINInDb.trim();
							if(!realISINInDb.equals(fileISIN)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in DB is:" + actISINInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in Hybrid FundFile is:" + fileISIN);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
							}
						}
					}
				}
			}
		}
//释放secId2ISIN内存空间
		perfId2ISIN.clear();
		if(perfId2ISIN.isEmpty()){
			System.out.println("[INFO]Map perfId2ISIN has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("[INFO]Hybrid Fund File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
	}	
		
	private static void testHybridFundFileContent4YieldAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		SimpleDateFormat dbDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMdd");
		String[] element = null;
		String fileYieldAsAtDate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's YieldAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's YieldAsAtDate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> perfId2YieldAsAtDate = new HashMap<String,String>();
		String perfIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		perfIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!perfIdBuildStr.isEmpty()){
			System.out.println("[INFO]PerfId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building PerfId string failed!");
		}
		
//Get file data stream from Hybrid Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(hybridFundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEndForHybridFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ perfIdBuildStr + ")";
		long startTimePerfId2YieldAsAtDate= System.currentTimeMillis();
		perfId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica2);
		long endTimePerfId2YieldAsAtDate = System.currentTimeMillis()-startTimePerfId2YieldAsAtDate;
		if(!perfId2YieldAsAtDate.isEmpty()){
			System.out.println("[INFO]Get PerfId->YieldAsAtDate map finished, total cost: " + endTimePerfId2YieldAsAtDate/1000+" s");
		}else{
			System.out.println("[ERROR]Get PerfId->YieldAsAtDate map failed!Because this Map is empty");
		}			
//测试YieldAsAtDate	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileYieldAsAtDate = element[8];
				fileSecId = element[7];
				if(!fileYieldAsAtDate.isEmpty() && !fileSecId.isEmpty()){
					if(perfId2YieldAsAtDate.containsKey(fileSecId)){
						String performanceId = perfId2YieldAsAtDate.get(fileSecId);
						if(perfId2YieldAsAtDate.containsKey(performanceId)){
							String actYieldAsAtDateInDb = perfId2YieldAsAtDate.get(performanceId);
							Date dbDate = dbDateFormat.parse(actYieldAsAtDateInDb);
							if(actYieldAsAtDateInDb != null && fileYieldAsAtDate != null){
								Date fileDate = fileDateFormat.parse(fileYieldAsAtDate);
								if(!fileDate.equals(dbDate)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldAsAtDate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldAsAtDate in DB is:" + actYieldAsAtDateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldAsAtDate in FundFile is:" + fileYieldAsAtDate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
		}
//释放secId2YieldAsAtDate内存空间
		perfId2YieldAsAtDate.clear();
		if(perfId2YieldAsAtDate.isEmpty()){
			System.out.println("[INFO]Map perfId2YieldAsAtDate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("[INFO]Hybrid Fund File YieldAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File YieldAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
	}
	
	private static void testHybridFundFileContent4YieldRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileYieldRate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's YieldRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's YieldRate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> perfId2YieldRate = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]PerformanceId String has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building PerformanceId String failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(hybridFundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEndForHybridFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ secIdBuildStr + ")";
		long startTimePerfId2SEDOL= System.currentTimeMillis();
		perfId2YieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica2);
		long endTimePerfId2SEDOL = System.currentTimeMillis()-startTimePerfId2SEDOL;
		if(!perfId2YieldRate.isEmpty()){
			System.out.println("[INFO]Get PerfId->SEDOL map finished, total cost: " + endTimePerfId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get PerfId->SEDOL map failed!Because this Map is empty");
		}			
//测试YieldRate	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileYieldRate = element[9];
				fileSecId = element[7];
				if(!fileYieldRate.isEmpty() && !fileSecId.isEmpty()){
					if(perfId2YieldRate.containsKey(fileSecId)){
						String actYieldRateInDb = perfId2YieldRate.get(fileSecId);
						if(actYieldRateInDb != null){
							Double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
							Double DrealYieldRateInDb = Helper.setDoublePrecision(DactYieldRateInDb, 5, BigDecimal.ROUND_HALF_DOWN);
							Double DrealYieldRateInFile = Double.parseDouble(fileYieldRate);
							if(!DrealYieldRateInDb.equals(DrealYieldRateInFile)){
								if(DrealYieldRateInFile-DrealYieldRateInDb != 9.999999999621423E-6){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldRate in DB is:" + actYieldRateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldRate in FundFile is:" + fileYieldRate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
		}
//释放secId2YieldRate内存空间
		perfId2YieldRate.clear();
		if(perfId2YieldRate.isEmpty()){
			System.out.println("[INFO]Map perfId2YieldRate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("[INFO]Hybrid Fund File YieldRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File YieldRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
	}
	
	private static void testHybridFundFileContent4BetaAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		SimpleDateFormat dbDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMdd");
		String[] element = null;
		String fileBetaAsAtDate = "";
		String filePerfId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's BetaAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's BetaAsAtDate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> perfId2BetaAsAtDate = new HashMap<String,String>();
		String perfIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildPerfIdStartTime = System.currentTimeMillis();
		perfIdBuildStr = buildString(7);
		long buildPerfIdEndTime = System.currentTimeMillis() - buildPerfIdStartTime;
		if(!perfIdBuildStr.isEmpty()){
			System.out.println("[INFO]PerformanceId String has built up,total cost: " + buildPerfIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building PerformanceId String failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(hybridFundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEndForHybridFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ perfIdBuildStr + ")";
		long startTimePerfId2BetaAsAtDate= System.currentTimeMillis();
		perfId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica2);
		long endTimePerfId2BetaAsAtDate = System.currentTimeMillis()-startTimePerfId2BetaAsAtDate;
		if(!perfId2BetaAsAtDate.isEmpty()){
			System.out.println("[INFO]Get PerfId->BetaAsAtDate map finished, total cost: " + endTimePerfId2BetaAsAtDate/1000+" s");
		}else{
			System.out.println("[ERROR]Get PerfId->BetaAsAtDate map failed!Because this Map is empty");
		}			
//测试BetaAsAtDate	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileBetaAsAtDate = element[10];
				filePerfId = element[7];
				if(!fileBetaAsAtDate.isEmpty() && !filePerfId.isEmpty()){
					if(perfId2BetaAsAtDate.containsKey(filePerfId)){
							String actBetaAsAtDateInDb = perfId2BetaAsAtDate.get(filePerfId);
							Date dbDate = dbDateFormat.parse(actBetaAsAtDateInDb);
							if(actBetaAsAtDateInDb != null && fileBetaAsAtDate != null){
								Date fileDate = fileDateFormat.parse(fileBetaAsAtDate);
								if(!fileDate.equals(dbDate)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaAsAtDate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaAsAtDate in DB is:" + actBetaAsAtDateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaAsAtDate in FundFile is:" + fileBetaAsAtDate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
					}
				}
			}
		}
//释放secId2BetaAsAtDate内存空间
		perfId2BetaAsAtDate.clear();
		if(perfId2BetaAsAtDate.isEmpty()){
			System.out.println("[INFO]Map perfId2BetaAsAtDate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("[INFO]Hybrid Fund File BetaAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File BetaAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
	}
	
	private static void testHybridFundFileContent4BetaRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileBetaRate = "";
		String filePerfId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's BetaRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's BetaRate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> perfId2BetaRate = new HashMap<String,String>();
		String perIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		perIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!perIdBuildStr.isEmpty()){
			System.out.println("[INFO]PerformanceId String has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building PerformanceId String failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(hybridFundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEndForHybridFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ perIdBuildStr + ")";
		long startTimePerfId2SEDOL= System.currentTimeMillis();
		perfId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica2);
		long endTimePerfId2SEDOL = System.currentTimeMillis()-startTimePerfId2SEDOL;
		if(!perfId2BetaRate.isEmpty()){
			System.out.println("[INFO]Get PerfId->BetaRate map finished, total cost: " + endTimePerfId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get PerfId->BetaRate map failed!Because this Map is empty");
		}			
//测试BetaRate	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileBetaRate = element[11];
				filePerfId = element[7];
				if(!fileBetaRate.isEmpty() && !filePerfId.isEmpty()){
					if(perfId2BetaRate.containsKey(filePerfId)){
						String actBetaRateInDb = perfId2BetaRate.get(filePerfId);
						if(actBetaRateInDb != null){
							Double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
							Double DrealBetaRateInDb = Helper.setDoublePrecision(DactBetaRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
							Double DrealBetaRateInFile = Double.parseDouble(fileBetaRate);
							if(!DrealBetaRateInDb.equals(DrealBetaRateInFile)){
								if(DrealBetaRateInFile - DrealBetaRateInDb != 0.001000000000000334){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaRate in DB is:" + actBetaRateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaRate in FundFile is:" + fileBetaRate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}							
							}
						}
					}
				}
			}
		}
//释放secId2BetaRate内存空间
		perfId2BetaRate.clear();
		if(perfId2BetaRate.isEmpty()){
			System.out.println("[INFO]Map perfId2BetaRate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("[INFO]Hybrid Fund File BetaRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File BetaRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
	}
	
		private static void testHybridFundFileContent4ProxyId() throws Exception{
			final String startTime = Base.currentSysTime();
			String[] element = null;
			String fileProxyId = "";
			String filePerformanceId = "";
					
			System.out.println("[TestForDataContent]Begin to test the Hybrid Fund File's ProxyId in each lines,please wait.......");
			System.out.println("[TestForDataContent]Test at:" + startTime);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Hybrid Fund File's ProxyId in each lines,start at:" + startTime);
			
			int lineNum = 0;
			HashMap<String,String> perfId2ProxyId = new HashMap<String,String>();
			String perfIdBuildStr;
			List<String> fileLineList = new ArrayList<String>();
			long buildSecIdStartTime = System.currentTimeMillis();
			perfIdBuildStr = buildString(7);
			long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
			if(!perfIdBuildStr.isEmpty()){
				System.out.println("[INFO]PerformanceId String has built up,total cost: " + buildSecIdEndTime + " ms");
			}else{
				System.out.println("[ERROR]Building PerformanceId String failed!");
			}
			
	//Get file data stream from Fund package to a list
			try {
				Long readfileStartTime = System.currentTimeMillis(); 
				fileLineList = Helper.readFileList(hybridFundFilePath);
				long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
				System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			String sqlToGetISIN = "SELECT PerformanceId,ProxyId FROM dbo.ML3YearBetaForMonthEndForHybridFund WHERE PerformanceId IN ("+ perfIdBuildStr + ")";
			long startTimePerfId2ISIN= System.currentTimeMillis();
			perfId2ProxyId = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
			long endTimePerfId2ISIN = System.currentTimeMillis()-startTimePerfId2ISIN;
			if(!perfId2ProxyId.isEmpty()){
				System.out.println("[INFO]Get PerfId->ProxyId map finished, total cost: " + endTimePerfId2ISIN/1000+" s");
			}else{
				System.out.println("[ERROR]Get PerfId->ProxyId map failed!Because this Map is empty");
			}			
	//测试ProxyId	
			for(String lineContentStr:fileLineList){
				lineNum++;
				if(lineContentStr.contains("UHDR ")){
					continue;
				}else if(lineContentStr.contains("UTRL ")){
					continue;
				}else{
					element = lineContentStr.split("~",16);
					fileProxyId = element[14];
					filePerformanceId = element[7];
					if(!fileProxyId.isEmpty() && !filePerformanceId.isEmpty()){
						if(perfId2ProxyId.containsKey(filePerformanceId)){
							String actProxyIdInDb = perfId2ProxyId.get(filePerformanceId);
							if(actProxyIdInDb != null){
								String realProxyIdInDb = actProxyIdInDb.trim();
								if(!realProxyIdInDb.equals(fileProxyId)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "ProxyId not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in DB is:" + actProxyIdInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in Hybrid FundFile is:" + fileProxyId);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
	//释放perfId2ProxyId内存空间
			perfId2ProxyId.clear();
			if(perfId2ProxyId.isEmpty()){
				System.out.println("[INFO]Map perfId2ISIN has cleared up!");
			}
			
			final String endTime = Base.currentSysTime();
			System.out.println("[INFO]Hybrid Fund File ProxyId testing has finished,end at:" + endTime);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Hybrid Fund File ProxyId testing has finished,end at:" + endTime);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "<------------------------------------------------------------------------------>");
		}
					
		private static String buildString(int columnNum) throws Exception{
			StringBuilder sb = new StringBuilder();
			List<String> fileLineList = new ArrayList<String>();
			int LineCount = 0;
			String[] element = null;
			String eleStr = null;
			int lineNum = Helper.getTotalLinesOfFile(hybridFundFilePath);
			
			try { 
				fileLineList = Helper.readFileList(hybridFundFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}
			for(String lineContentStr:fileLineList){
				LineCount++;
				if(lineContentStr.contains("UHDR ")){
					continue;
				}else if(lineContentStr.contains("UTRL ")){
					continue;
				}else{
					if(!lineContentStr.isEmpty() || lineContentStr !=null){
						element = lineContentStr.split("~",16);
						eleStr = element[columnNum];
						sb.append("'"+eleStr+"',");
						if(LineCount == lineNum-1){
							sb.append("'"+eleStr+"'");
						}
					}
				}
			}
			return sb.toString();
		}				
		
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		try {
			testFundFileContent();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Content test for Hybrid Fund has finished, total cost: " + endTestTime/(1000*60) + " min");
	}
}
