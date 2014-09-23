package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageContentTest {

	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	private static String testLogNameCase = "contentTestResult-" + currentTime + "." + "log";
	
	private static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/StagingENV/20140830/PLP320XD.MSTBONDS";
		
	private static void testBondFileContent() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataContent]Begin to test the Bond File's Data content in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's Data content in each lines,start at:" + startTime);
		
		testBondFileContent4CUSIP();
		testBondFileContent4ISIN();
		testBondFileContent4YieldAsAtDate();
		testBondFileContent4YieldRate();
		testBondFileContent4BetaAsAtDate();
		testBondFileContent4BetaRate();
		
		String endTime = Base.currentSysTime();
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Content test has finished,end at:" + endTime);
	}
	
	private static void testBondFileContent4CUSIP() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileCUSIP = "";
		String fileSecId = "";
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's CUSIP in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's CUSIP in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2CUSIP = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetCUSIP = "SELECT InvestmentId,CUSIP FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secId + ")";
			long startTimeSecId2CUSIP = System.currentTimeMillis();
			secId2CUSIP = DBCommons.getDataHashMap(sqlToGetCUSIP, Database.Vertica2);
			long endTimeSecId2CUSIP = System.currentTimeMillis()-startTimeSecId2CUSIP;
			if(!secId2CUSIP.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SetId->CUSIP map finished, total cost: " + endTimeSecId2CUSIP/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SetId->CUSIP map failed!Because this Map is empty");
			}			
//测试CUSIP	
			for(String lineContentStr:fileLineList){
				lineNum++;
				if(lineContentStr.contains("UHDR ")){
					continue;
				}else if(lineContentStr.contains("UTRL ")){
					continue;
				}else{
					element = lineContentStr.split("~",16);
					fileCUSIP = element[0];
					fileSecId = element[7];
					if(!fileCUSIP.isEmpty() && !fileSecId.isEmpty()){
						if(secId2CUSIP.containsKey(fileSecId)){
							String actCUSIPInDb = secId2CUSIP.get(fileSecId);
							if(actCUSIPInDb != null){
								String realCUSIPInDb = actCUSIPInDb.trim();
								if(!realCUSIPInDb.equals(fileCUSIP)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "CUSIP not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in DB is:" + actCUSIPInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in BondFile is:" + fileCUSIP);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
//释放secId2CUSIP内存空间
			secId2CUSIP.clear();
			if(secId2CUSIP.isEmpty()){
				System.out.println("[INFO]Map secId2CUSIP has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File CUSIP testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File CUSIP testing has finished,end at:" + endTime);
	}
	
	private static void testBondFileContent4ISIN() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileISIN = "";
		String fileSecId = "";
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's ISIN in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's ISIN in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2ISIN = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>(); 
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetISIN = "SELECT InvestmentId,ISIN FROM dbo.InvestmentIdDimension WHERE VendorId = '101' AND InvestmentId IN ("+ secId + ")" ;
			long startTimeSecId2ISIN = System.currentTimeMillis();
			secId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
			long endTimeSecId2ISIN = System.currentTimeMillis() - startTimeSecId2ISIN;
			if(!secId2ISIN.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SetId->ISIN map finished, total cost: " + endTimeSecId2ISIN/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SetId->ISIN map failed!Because this Map is empty");
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
						if(secId2ISIN.containsKey(fileSecId)){
							String actISINInDb = secId2ISIN.get(fileSecId);
							if(actISINInDb != null){
								String realISINInDb = actISINInDb.trim();
								if(!realISINInDb.equals(fileISIN)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in DB is:" + actISINInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in BondFile is:" + fileISIN);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
//释放secId2CUSIP内存空间
			secId2ISIN.clear();
			if(secId2ISIN.isEmpty()){
				System.out.println("[INFO]Map secId2ISIN has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File ISIN testing has finished,end at:" + endTime);
	}
	
	@SuppressWarnings("null")
	private static void testBondFileContent4YieldAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileYieldAsAtDate;
		String fileSecId;
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's Yield As At Date in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's Yield As At Date in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2YieldAsAtDate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secId + ")";
			long startTimeSecId2YieldAsAtDate = System.currentTimeMillis();
			secId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica2);
			long endTimeSecId2YieldAsAtDate = System.currentTimeMillis()-startTimeSecId2YieldAsAtDate;
			if(!secId2YieldAsAtDate.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SetId->Yield As At Date map finished, total cost: " + endTimeSecId2YieldAsAtDate/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SetId->Yield As At Date map failed!Because this Map is empty");
			}
//测试YieldAsAtDate
			for(String lineContentStr:fileLineList){
				lineNum++;
				if(lineContentStr.contains("UHDR ")){
					continue;
				}else if(lineContentStr.contains("UTRL ")){
					continue;
				}else{
					fileSecId = element[7];
					fileYieldAsAtDate = element[8];
					if(!fileSecId.isEmpty() && !fileYieldAsAtDate.isEmpty()){
						if(secId2YieldAsAtDate.containsKey(fileSecId)){
							String actYieldAsAtDateInDb = secId2YieldAsAtDate.get(fileSecId);
							if(actYieldAsAtDateInDb != null){
								if(!actYieldAsAtDateInDb.isEmpty() && !fileYieldAsAtDate.isEmpty()){
									Date realYieldAsAtDateInDb = Helper.dateParse(actYieldAsAtDateInDb, "yyyy-MM-dd");
									Date realYieldAsAtDateInFile = Helper.dateParse(fileYieldAsAtDate, "yyyyMMdd");
									if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
										CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldAsAtDate not mapping!");
										CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
										CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldAsAtDate in DB is:" + actYieldAsAtDateInDb);
										CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldAsAtDate in BondFile is:" + fileYieldAsAtDate);
										CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
									}
								}				
							}
						}
					}
				}
			}
//释放secId2YieldAsAtDate内存空间
			secId2YieldAsAtDate.clear();
			if(secId2YieldAsAtDate.isEmpty()){
				System.out.println("[INFO]Map secId2YieldAsAtDate has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File Yield As At Date testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File Yield As At Date testing has finished,end at:" + endTime);
	}
	
	private static void testBondFileContent4YieldRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileYieldRate = "";
		String fileSecId = "";
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's Yield Rate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's Yield Rate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2SecurityYieldRate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>(); 
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secId + ")";
			long startTimeSecId2SecurityYieldRate = System.currentTimeMillis();
			secId2SecurityYieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica2);
			long endTimeSecId2SecurityYieldRate = System.currentTimeMillis()-startTimeSecId2SecurityYieldRate;
			if(!secId2SecurityYieldRate.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SecId->YieldRate map finished, total cost: " + endTimeSecId2SecurityYieldRate/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SecId->YieldRate map failed!Because this Map is empty");
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
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldRate in DB is:" + actYieldRateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual YieldRate in BondFile is:" + fileYieldRate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}				
						}
					}
				}
			}
//释放secId2SecurityYieldRate内存空间
			secId2SecurityYieldRate.clear();
			if(secId2SecurityYieldRate.isEmpty()){
				System.out.println("[INFO]Map secId2SecurityYieldRate has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File Yield Rate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File Yield Rate testing has finished,end at:" + endTime);
	}
	
	private static void testBondFileContent4BetaAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileBetaAsAtDate = "";
		String fileSecId = "";
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's Beta As At Date in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Bond File's Beta As At Date in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2BetaAsAtDate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>(); 
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secId + ")";
			long startTimeSecId2BetaAsAtDate = System.currentTimeMillis();
			secId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica2);
			long endTimeSecId2BetaAsAtDate = System.currentTimeMillis()-startTimeSecId2BetaAsAtDate;
			if(!secId2BetaAsAtDate.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SecId->BetaAsAtDate map finished, total cost: " + endTimeSecId2BetaAsAtDate/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SecId->BetaAsAtDate map failed!Because this Map is empty");
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
					fileSecId = element[7];
					fileBetaAsAtDate = element[10];				
					if(secId2BetaAsAtDate.containsKey(fileSecId)){
						String actBetaAsAtDateInDb = secId2BetaAsAtDate.get(fileSecId);
						if(actBetaAsAtDateInDb != null){
							if(!actBetaAsAtDateInDb.isEmpty() && !fileBetaAsAtDate.isEmpty()){
								Date realYieldAsAtDateInDb = Helper.dateParse(actBetaAsAtDateInDb, "yyyy-MM-dd");
								Date realYieldAsAtDateInFile = Helper.dateParse(fileBetaAsAtDate, "yyyyMMdd");
								if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaAsAtDate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaAsAtDate in DB is:" + actBetaAsAtDateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaAsAtDate in BondFile is:" + fileBetaAsAtDate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}				
						}
					}
				}
			}
//释放BetaAsAtDate内存空间
			secId2BetaAsAtDate.clear();
			if(secId2BetaAsAtDate.isEmpty()){
				System.out.println("[INFO]Map BetaAsAtDate has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File Beta As At Date testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File Beta As At Date testing has finished,end at:" + endTime);
	}
	
	private static void testBondFileContent4BetaRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileBetaRate = "";
		String fileSecId = "";
		int bufferSize = 100000;
				
		System.out.println("[TestForDataContent]Begin to test the Bond File's Beta Rate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Beta Rate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2BetaRate = new HashMap<String,String>();
		List<String> secIdBuilderList = new ArrayList<String>();
		List<String> fileLineList = new ArrayList<String>(); 
		long buildSecIdBufferListStartTime = System.currentTimeMillis();
		secIdBuilderList = buildSecIdBuffer(7,bufferSize);
		long buildSecIdBufferListEndTime = System.currentTimeMillis() - buildSecIdBufferListStartTime;
		if(!secIdBuilderList.isEmpty()){
			System.out.println("[INFO]SecId buffer list has built ,total cost: " + buildSecIdBufferListEndTime/(1000*60) + " min");
		}else{
			System.out.println("[ERROR]Building Buffered SecId failed!");
		}
		
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String secId :secIdBuilderList){
			String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3' AND PerformanceId IN (" + secId + ")";
			Long startTimeSecId2BetaRate = System.currentTimeMillis();
			secId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica2);
			long endTimeSecId2BetaRate = System.currentTimeMillis()-startTimeSecId2BetaRate;
			if(!secId2BetaRate.isEmpty()){
				System.out.println("[INFO]Get "+ bufferSize + " SecId->BetaRate map finished, total cost: " + endTimeSecId2BetaRate/1000+" s");
			}else{
				System.out.println("[ERROR]Get "+ bufferSize + " SecId->BetaRate map failed!Because this Map is empty");
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
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Bond file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaRate in DB is:" + actBetaRateInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaRate in BondFile is:" + fileBetaRate);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}							
							}				
						}
					}
				}
			}
//释放BetaAsAtDate内存空间
			secId2BetaRate.clear();
			if(secId2BetaRate.isEmpty()){
				System.out.println("[INFO]Map secId2BetaRate has cleared up!");
			}
		}
		final String endTime = Base.currentSysTime();
		System.out.println("Bond File Beta Rate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Bond File Beta Rate testing has finished,end at:" + endTime);
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

		//将指定列数据Load到一个List中
		private static List<String> loadAssignDataToList(int columnNum) throws Exception{
			List<String> resultList = new ArrayList<String>();
			List<String> fileLineList = new ArrayList<String>();
			String[] element = null;
			String eleStr = null;
			
			try { 
				fileLineList = Helper.readFileList(bondFilePath);
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
		try {
			testBondFileContent();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Content test for Bond has finished, total cost: " + endTestTime/(1000*60) + " min");
	}

}
