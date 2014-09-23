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

public class FundPackageContentTest {
	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/FundPackage/";
	private static String testLogNameCase = "contentTestResult-" + currentTime + "." + "log";
	
	private static String fundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/FundDemoFile/StagingENV/20140830/PLP320XA.MSGBLFND";
		
	private static void testFundFileContent() throws Exception{
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataContent]Begin to test the Fund File's Data content in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's Data content in each lines,start at:" + startTime);
		
//		testFundFileContent4CUSIP();
		testFundFileContent4SEDOL();
		testFundFileContent4ISIN();
//		testFundFileContent4ExchangeId();
		testFundFileContent4Symbol();
		testFundFileContent4YieldAsAtDate();
		testFundFileContent4YieldRate();
		testFundFileContent4BetaAsAtDate();
		testFundFileContent4BetaRate();
//		testFundFileContent4BetaBackfillIndexCode();
//		testFundFileContent4MorningstarBetaProxyName();
		
		String endTime = Base.currentSysTime();
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Content test has finished,end at:" + endTime);
	}

	private static void testFundFileContent4CUSIP() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileCUSIP = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's CUSIP in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's CUSIP in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2CUSIP = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetCUSIP = "SELECT InvestmentId,CUSIP FROM dbo.InvestmentIdDimension WHERE InvestmentId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2CUSIP = System.currentTimeMillis();
		secId2CUSIP = DBCommons.getDataHashMap(sqlToGetCUSIP, Database.Vertica2);
		long endTimeSecId2CUSIP = System.currentTimeMillis()-startTimeSecId2CUSIP;
		if(!secId2CUSIP.isEmpty()){
			System.out.println("[INFO]Get SecId->CUSIP map finished, total cost: " + endTimeSecId2CUSIP/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->CUSIP map failed!Because this Map is empty");
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
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in DB is:" + actCUSIPInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual CUSIP in FundFile is:" + fileCUSIP);
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
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File CUSIP testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File CUSIP testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4SEDOL() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileSEDOL = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's SEDOL in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's SEDOL in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2SEDOL = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetSEDOL = "SELECT InvestmentId,Identifier FROM dbi.InvestmentPrimaryIdentifiers WHERE IdentifierType = 3 and Identifier is not null AND InvestmentId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2SEDOL = DBCommons.getDataHashMap(sqlToGetSEDOL, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2SEDOL.isEmpty()){
			System.out.println("[INFO]Get SecId->SEDOL map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->SEDOL map failed!Because this Map is empty");
		}			
//测试SEDOL	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileSEDOL = element[1];
				fileSecId = element[7];
				if(!fileSEDOL.isEmpty() && !fileSecId.isEmpty()){
					if(secId2SEDOL.containsKey(fileSecId)){
						String actSEDOLInDb = secId2SEDOL.get(fileSecId);
						if(actSEDOLInDb != null){
							String realCUSIPInDb = actSEDOLInDb.trim();
							if(!realCUSIPInDb.equals(fileSEDOL)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "SEDOL not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual SEDOL in DB is:" + actSEDOLInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual SEDOL in FundFile is:" + fileSEDOL);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
							}
						}
					}
				}
			}
		}
//释放secId2SEDOL内存空间
		secId2SEDOL.clear();
		if(secId2SEDOL.isEmpty()){
			System.out.println("[INFO]Map secId2SEDOL has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File SEDOL testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File SEDOL testing has finished,end at:" + endTime);		
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4ISIN() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileISIN = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's ISIN in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's ISIN in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2ISIN = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetISIN = "SELECT InvestmentId,ISIN FROM dbo.PerformanceIdDimension WHERE InvestmentId IN ("+ secIdBuildStr + ") AND IsPrimary = 1";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2ISIN.isEmpty()){
			System.out.println("[INFO]Get SecId->ISIN map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->ISIN map failed!Because this Map is empty");
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
							String realCUSIPInDb = actISINInDb.trim();
							if(!realCUSIPInDb.equals(fileISIN)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in DB is:" + actISINInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ISIN in FundFile is:" + fileISIN);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
							}
						}
					}
				}
			}
		}
//释放secId2ISIN内存空间
		secId2ISIN.clear();
		if(secId2ISIN.isEmpty()){
			System.out.println("[INFO]Map secId2ISIN has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File ISIN testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4ExchangeId() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileExchangeId = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's ExchangeId in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's ExchangeId in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2ExchangeId = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		String sqlToGetExchangeId = "SELECT InvestmentId,ExchangeId FROM dbo.PerformanceIdDimension WHERE InvestmentId IN ("+ secIdBuildStr + ")";
		String sqlToGetExchangeId = "SELECT InvestmentId,ExchangeId FROM dbi.investmentexchangelisting where investmenttype = 1 and idtype = 10 and InvestmentId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2ExchangeId= System.currentTimeMillis();
		secId2ExchangeId = DBCommons.getDataHashMap(sqlToGetExchangeId, Database.Vertica2);
		long endTimeSecId2ExchangeId = System.currentTimeMillis()-startTimeSecId2ExchangeId;
		if(!secId2ExchangeId.isEmpty()){
			System.out.println("[INFO]Get SecId->ExchangeId map finished, total cost: " + endTimeSecId2ExchangeId/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->ExchangeId map failed!Because this Map is empty");
		}			
//测试ExchangeId	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileExchangeId = element[3];
				fileSecId = element[7];
				if(!fileExchangeId.isEmpty() && !fileSecId.isEmpty()){
					if(secId2ExchangeId.containsKey(fileSecId)){
						String actExchangeIdInDb = secId2ExchangeId.get(fileSecId);
						if(actExchangeIdInDb != null){
							String realCUSIPInDb = actExchangeIdInDb.trim();
							if(!realCUSIPInDb.equals(fileExchangeId)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "ExchangeId not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ExchangeId in DB is:" + actExchangeIdInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual ExchangeId in FundFile is:" + fileExchangeId);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
							}
						}
					}
				}
			}
		}
//释放secId2ExchangeId内存空间
		secId2ExchangeId.clear();
		if(secId2ExchangeId.isEmpty()){
			System.out.println("[INFO]Map secId2ExchangeId has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File ExchangeId testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File ExchangeId testing has finished,end at:" + endTime);		
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4Symbol() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileSymbol = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's Symbol in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's Symbol in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2Symbol = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetSymbol = "SELECT InvestmentId,Symbol FROM dbo.PerformanceIdDimension WHERE PerformanceId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2Symbol = DBCommons.getDataHashMap(sqlToGetSymbol, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2Symbol.isEmpty()){
			System.out.println("[INFO]Get SecId->Symbol map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->Symbol map failed!Because this Map is empty");
		}			
//测试Symbol	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileSymbol = element[4];
				fileSecId = element[7];
				if(!fileSymbol.isEmpty() && !fileSecId.isEmpty()){
					if(secId2Symbol.containsKey(fileSecId)){
						String actSymbolInDb = secId2Symbol.get(fileSecId);
						if(actSymbolInDb != null){
							String realCUSIPInDb = actSymbolInDb.trim();
							if(!realCUSIPInDb.equals(fileSymbol)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "Symbol not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual Symbol in DB is:" + actSymbolInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual Symbol in FundFile is:" + fileSymbol);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
							}
						}
					}
				}
			}
		}
//释放secId2Symbol内存空间
		secId2Symbol.clear();
		if(secId2Symbol.isEmpty()){
			System.out.println("[INFO]Map secId2Symbol has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File Symbol testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File Symbol testing has finished,end at:" + endTime);		
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4YieldAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		SimpleDateFormat dbDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMdd");
		String[] element = null;
		String fileYieldAsAtDate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's YieldAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's YieldAsAtDate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2YieldAsAtDate = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		String sqlToGetInvestmentId2PerformanceId = "SELECT InvestmentId,PerformanceId FROM dbo.PerformanceIdDimension WHERE InvestmentId IN (" + secIdBuildStr + ")";
		long startTimeToBuildPerformanceId = System.currentTimeMillis();
		HashMap<String,String> investmentId2PerfIdMap = new HashMap<String,String>();
		investmentId2PerfIdMap = DBCommons.getDataHashMap(sqlToGetInvestmentId2PerformanceId, Database.Vertica2);		
		String performanceIdStr = buildPerformanceId(investmentId2PerfIdMap);
		long endTimeToBuildPerformanceId = System.currentTimeMillis() - startTimeToBuildPerformanceId;
		System.out.println("[INFO]PerformanceId string has built up, total cost: "+endTimeToBuildPerformanceId/1000+" s");
		String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEndForFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ performanceIdStr + ")";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2YieldAsAtDate.isEmpty()){
			System.out.println("[INFO]Get PerfId->YieldAsAtDate map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
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
					if(investmentId2PerfIdMap.containsKey(fileSecId)){
						String performanceId = investmentId2PerfIdMap.get(fileSecId);
						if(secId2YieldAsAtDate.containsKey(performanceId)){
							String actYieldAsAtDateInDb = secId2YieldAsAtDate.get(performanceId);
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
		secId2YieldAsAtDate.clear();
		if(secId2YieldAsAtDate.isEmpty()){
			System.out.println("[INFO]Map secId2YieldAsAtDate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File YieldAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File YieldAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4YieldRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileYieldRate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's YieldRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's YieldRate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2YieldRate = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEndForFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2YieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2YieldRate.isEmpty()){
			System.out.println("[INFO]Get SecId->SEDOL map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->SEDOL map failed!Because this Map is empty");
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
					if(secId2YieldRate.containsKey(fileSecId)){
						String actYieldRateInDb = secId2YieldRate.get(fileSecId);
						if(actYieldRateInDb != null){
							Double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
							Double DrealYieldRateInDb = Helper.setDoublePrecision(DactYieldRateInDb, 5, BigDecimal.ROUND_HALF_DOWN);
							Double DrealYieldRateInFile = Double.parseDouble(fileYieldRate);
							if(!DrealYieldRateInDb.equals(DrealYieldRateInFile)){
								if(DrealYieldRateInFile-DrealYieldRateInDb != 9.999999999621423E-6){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "YieldRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
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
		secId2YieldRate.clear();
		if(secId2YieldRate.isEmpty()){
			System.out.println("[INFO]Map secId2YieldRate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File YieldRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File YieldRate testing has finished,end at:" + endTime);
	}
	
	private static void testFundFileContent4BetaAsAtDate() throws Exception{
		final String startTime = Base.currentSysTime();
		SimpleDateFormat dbDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMdd");
		String[] element = null;
		String fileBetaAsAtDate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's BetaAsAtDate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's BetaAsAtDate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2BetaAsAtDate = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		String sqlToGetInvestmentId2PerformanceId = "SELECT InvestmentId,PerformanceId FROM dbo.PerformanceIdDimension WHERE InvestmentId IN (" + secIdBuildStr + ")";
		long startTimeToBuildPerformanceId = System.currentTimeMillis();
		HashMap<String,String> investmentId2PerfIdMap = new HashMap<String,String>();
		investmentId2PerfIdMap = DBCommons.getDataHashMap(sqlToGetInvestmentId2PerformanceId, Database.Vertica2);		
		String performanceIdStr = buildPerformanceId(investmentId2PerfIdMap);
		long endTimeToBuildPerformanceId = System.currentTimeMillis() - startTimeToBuildPerformanceId;
		System.out.println("[INFO]PerformanceId string has built up, total cost: "+endTimeToBuildPerformanceId/1000+" s");
		String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEndForFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ performanceIdStr + ")";
		long startTimePerfId2BetaAsAtDate= System.currentTimeMillis();
		secId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica2);
		long endTimePerfId2BetaAsAtDate = System.currentTimeMillis()-startTimePerfId2BetaAsAtDate;
		if(!secId2BetaAsAtDate.isEmpty()){
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
				fileSecId = element[7];
				if(!fileBetaAsAtDate.isEmpty() && !fileSecId.isEmpty()){
					if(investmentId2PerfIdMap.containsKey(fileSecId)){
						String performanceId = investmentId2PerfIdMap.get(fileSecId);
						if(secId2BetaAsAtDate.containsKey(performanceId)){
							String actBetaAsAtDateInDb = secId2BetaAsAtDate.get(performanceId);
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
		}
//释放secId2BetaAsAtDate内存空间
		secId2BetaAsAtDate.clear();
		if(secId2BetaAsAtDate.isEmpty()){
			System.out.println("[INFO]Map secId2BetaAsAtDate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File BetaAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File BetaAsAtDate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4BetaRate() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileBetaRate = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's BetaRate in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's BetaRate in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2BetaRate = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEndForFund WHERE MLUniverseType = '1' AND PerformanceId IN ("+ secIdBuildStr + ")";
		long startTimeSecId2SEDOL= System.currentTimeMillis();
		secId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica2);
		long endTimeSecId2SEDOL = System.currentTimeMillis()-startTimeSecId2SEDOL;
		if(!secId2BetaRate.isEmpty()){
			System.out.println("[INFO]Get SecId->SEDOL map finished, total cost: " + endTimeSecId2SEDOL/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->SEDOL map failed!Because this Map is empty");
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
				fileSecId = element[7];
				if(!fileBetaRate.isEmpty() && !fileSecId.isEmpty()){
					if(secId2BetaRate.containsKey(fileSecId)){
						String actBetaRateInDb = secId2BetaRate.get(fileSecId);
						if(actBetaRateInDb != null){
							Double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
							Double DrealBetaRateInDb = Helper.setDoublePrecision(DactBetaRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
							Double DrealBetaRateInFile = Double.parseDouble(fileBetaRate);
							if(!DrealBetaRateInDb.equals(DrealBetaRateInFile)){
								if(DrealBetaRateInFile - DrealBetaRateInDb != 0.001000000000000334){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "BetaRate not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
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
		secId2BetaRate.clear();
		if(secId2BetaRate.isEmpty()){
			System.out.println("[INFO]Map secId2BetaRate has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File BetaRate testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File BetaRate testing has finished,end at:" + endTime);
	}

	private static void testFundFileContent4BetaBackfillIndexCode() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileBetaBackfillIndexCode = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's BetaBackfillIndexCode in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's BetaBackfillIndexCode in each lines,start at:" + startTime);
		
		int lineNum = 0;
		HashMap<String,String> secId2BetaBackfillIndexCode = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String sqlToGetInvestmentId2PerformanceId = "SELECT InvestmentId,PerformanceId FROM dbo.PerformanceIdDimension WHERE InvestmentId IN (" + secIdBuildStr + ")";
		long startTimeToBuildPerformanceId = System.currentTimeMillis();
		HashMap<String,String> investmentId2PerfIdMap = new HashMap<String,String>();
		investmentId2PerfIdMap = DBCommons.getDataHashMap(sqlToGetInvestmentId2PerformanceId, Database.Vertica2);		
		String performanceIdStr = buildPerformanceId(investmentId2PerfIdMap);
		long endTimeToBuildPerformanceId = System.currentTimeMillis() - startTimeToBuildPerformanceId;
		System.out.println("[INFO]PerformanceId string has built up, total cost: "+endTimeToBuildPerformanceId/1000+" s");		
		String sqlToGetBetaBackfillIndexCode = "SELECT PerformanceId,ProxyId FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN ("+ performanceIdStr + ")";
		long startTimeSecId2BetaBackfillIndexCode= System.currentTimeMillis();
		secId2BetaBackfillIndexCode = DBCommons.getDataHashMap(sqlToGetBetaBackfillIndexCode, Database.Vertica2);
		long endTimeSecId2BetaBackfillIndexCode = System.currentTimeMillis()-startTimeSecId2BetaBackfillIndexCode;
		if(!secId2BetaBackfillIndexCode.isEmpty()){
			System.out.println("[INFO]Get SecId->BetaBackfillIndexCode map finished, total cost: " + endTimeSecId2BetaBackfillIndexCode/1000+" s");
		}else{
			System.out.println("[ERROR]Get SecId->BetaBackfillIndexCode map failed!Because this Map is empty");
		}			
//测试BetaBackfillIndexCode	
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileBetaBackfillIndexCode = element[14];
				fileSecId = element[7];
				if(!fileBetaBackfillIndexCode.isEmpty() && !fileSecId.isEmpty()){
					if(investmentId2PerfIdMap.containsKey(fileSecId)){
						String performanceId = investmentId2PerfIdMap.get(fileSecId);
						if(secId2BetaBackfillIndexCode.containsKey(performanceId)){
							String actBetaBackfillIndexCodeInDb = secId2BetaBackfillIndexCode.get(fileSecId);
							if(actBetaBackfillIndexCodeInDb != null){
								String realCUSIPInDb = actBetaBackfillIndexCodeInDb.trim();
								if(!realCUSIPInDb.equals(fileBetaBackfillIndexCode)){
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + " BetaBackfillIndexCode not mapping!");
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaBackfillIndexCode in DB is:" + actBetaBackfillIndexCodeInDb);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual BetaBackfillIndexCode in FundFile is:" + fileBetaBackfillIndexCode);
									CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
								}
							}
						}
					}
				}
			}
		}
//释放secId2BetaBackfillIndexCode内存空间
		secId2BetaBackfillIndexCode.clear();
		if(secId2BetaBackfillIndexCode.isEmpty()){
			System.out.println("[INFO]Map secId2BetaBackfillIndexCode has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File BetaBackfillIndexCode testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File BetaBackfillIndexCode testing has finished,end at:" + endTime);		
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
	
	private static void testFundFileContent4MorningstarBetaProxyName() throws Exception{
		final String startTime = Base.currentSysTime();
		String[] element = null;
		String fileMorningstarBetaProxyName = "";
		String fileSecId = "";
				
		System.out.println("[TestForDataContent]Begin to test the Fund File's MorningstarBetaProxyName in each lines,please wait.......");
		System.out.println("[TestForDataContent]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataContent]Begin to test the Fund File's MorningstarBetaProxyName in each lines,start at:" + startTime);
		
		int lineNum = 0;
		List<String> MorningstarBetaProxyNameList = new ArrayList<String>();
		HashMap<String,String> secId2MorningstarBetaProxyName = new HashMap<String,String>();
		String secIdBuildStr;
		List<String> fileLineList = new ArrayList<String>();
		long buildSecIdStartTime = System.currentTimeMillis();
		secIdBuildStr = buildString(7);
		long buildSecIdEndTime = System.currentTimeMillis() - buildSecIdStartTime;
		if(!secIdBuildStr.isEmpty()){
			System.out.println("[INFO]SecId string has built up,total cost: " + buildSecIdEndTime + " ms");
		}else{
			System.out.println("[ERROR]Building SecId string failed!");
		}
		
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String proxyIdBuildStr = buildProxyId(secIdBuildStr);
//用ProxyId来作为InvestmentId查询对应的BetaProxyName		
		String sqlToGetMorningstarBetaProxyName = "SELECT InvestmentName FROM dbo.InvestmentIdDimension WHERE InvestmentId IN ("+proxyIdBuildStr+")";
		long startTimeSecId2MorningstarBetaProxyName= System.currentTimeMillis();
//		secId2MorningstarBetaProxyName = DBCommons.getDataHashMap(sqlToGetMorningstarBetaProxyName, Database.Vertica2);
		MorningstarBetaProxyNameList = DBCommons.getDataList(sqlToGetMorningstarBetaProxyName, Database.Vertica2);
		long endTimeSecId2MorningstarBetaProxyName = System.currentTimeMillis()-startTimeSecId2MorningstarBetaProxyName;
		if(!MorningstarBetaProxyNameList.isEmpty()){
			System.out.println("[INFO]Get MorningstarBetaProxyName list finished, total cost: " + endTimeSecId2MorningstarBetaProxyName/1000+" s");
		}else{
			System.out.println("[ERROR]Get MorningstarBetaProxyName list failed!Because this list is empty");
		}			
//测试MorningstarBetaProxyName
		int count = 0;
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				element = lineContentStr.split("~",16);
				fileMorningstarBetaProxyName = element[15];
				fileSecId = element[7];
				if(!fileMorningstarBetaProxyName.isEmpty() && !fileSecId.isEmpty()){
					String actMorningstarBetaProxyNameInDb = MorningstarBetaProxyNameList.get(count);
					if(actMorningstarBetaProxyNameInDb != null){
						String realCUSIPInDb = actMorningstarBetaProxyNameInDb.trim();
						if(!realCUSIPInDb.equals(fileMorningstarBetaProxyName)){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Testing failed:" + "Line number is:" + lineNum + "   " + "MorningstarBetaProxyName not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual MorningstarBetaProxyName in DB is:" + actMorningstarBetaProxyNameInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actual MorningstarBetaProxyName in FundFile is:" + fileMorningstarBetaProxyName);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "==============================================");
						}
					}
				}
				count++;
			}
		}
//释放secId2MorningstarBetaProxyName内存空间
		secId2MorningstarBetaProxyName.clear();
		if(secId2MorningstarBetaProxyName.isEmpty()){
			System.out.println("[INFO]Map secId2MorningstarBetaProxyName has cleared up!");
		}
		
		final String endTime = Base.currentSysTime();
		System.out.println("Fund File MorningstarBetaProxyName testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Fund File MorningstarBetaProxyName testing has finished,end at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "================================================================================");
	}
		
		private static String buildString(int columnNum) throws Exception{
			StringBuilder sb = new StringBuilder();
			List<String> fileLineList = new ArrayList<String>();
			int LineCount = 0;
			String[] element = null;
			String eleStr = null;
			int lineNum = Helper.getTotalLinesOfFile(fundFilePath);
			
			try { 
				fileLineList = Helper.readFileList(fundFilePath);
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
		
		private static String buildPerformanceId (HashMap valueMap) throws Exception{
			HashMap<String,String> investmentId2PerfIdMap = new HashMap<String,String>();
			investmentId2PerfIdMap = (HashMap<String, String>) valueMap.clone();
			String perfIdStr;
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for(Entry entry:investmentId2PerfIdMap.entrySet()){
				count++;
				perfIdStr = entry.getValue().toString();
				if(perfIdStr != null || !perfIdStr.isEmpty()){
					if(count == investmentId2PerfIdMap.size()){
						sb.append("'"+perfIdStr+"'");
					}else{
						sb.append("'"+perfIdStr+"',");
					}										
				}
			}
			return sb.toString();
		}
		
		private static String buildProxyId (String investmentIdBuildStr) throws Exception{
			List<String> proxyIdList = new ArrayList<String>();
			String sqlToGetProxyIdList = "SELECT ProxyId FROM dbo.ML3YearBetaForMonthEnd WHERE PerformanceId IN ("
					+ "SELECT PerformanceId FROM dbo.PerformanceIdDimension WHERE InvestmentId IN (" + investmentIdBuildStr + "))";
			proxyIdList = DBCommons.getDataList(sqlToGetProxyIdList, Database.Vertica2);
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for(String proxyId:proxyIdList){
				count++;
				if(proxyId != null || !proxyId.isEmpty()){
					if(count == proxyIdList.size()){
						sb.append("'"+proxyId+"'");
					}else{
						sb.append("'"+proxyId+"',");
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
		System.out.println("[INFO]Content test for Fund has finished, total cost: " + endTestTime/(1000*60) + " min");
	}
}
