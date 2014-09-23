package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.ibm.icu.text.SimpleDateFormat;
import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.DBFreshpool;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class StockPackageTest{

	/**
	 * @param args
	 */
	
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	public static String currenTime = df.format(new Date());
	
	public static String testLogPath = "../TestLog/StockPackage/";
	public static String testLogNameCase1 = "completenessTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase2 = "formatTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase3 = "contentTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase4 = "fileDuplicateDataTestResult-" + currenTime + "." + "log";
	
	public static String testLogTopic1 = "Stock Sample file for Merrill Lynch completeness testing";
	public static String testLogTopic2 = "Stock Sample file for Merrill Lynch format testing";
	public static String testLogTopic3 = "Stock Sample file for Merrill Lynch content verify testing";
	public static String testLogTopic4 = "Stock Sample file for Merrill Lynch duplicate data checking";
	public static String StockfilePath = "../Package/PLP320XC.MSSTOCKS";
	
	
	@BeforeClass(description = "Testing preparing!")
	public static void testPrepare() throws IOException{
//生成自定义测试结果日志文件
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase1, testLogTopic1);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase2, testLogTopic2);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase3, testLogTopic3);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase4, testLogTopic4);
	}
	
	@Test(description = "Testing:Stock sample file completment testing!")
	public static void testStockFileComplete() throws Exception{
		String startTime = df.format(new Date());
		System.out.println("[TestForFormat]Begin to test at least only one Yield or Beta rate in each lines,please wait.......");
		System.out.println("[TestForFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestForCompletment]Begin to test at least only one Yield or Beta rate in each lines,start at:" + startTime);
		int lineRange = Helper.getTotalLinesOfFile(StockfilePath);
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileLineContent = Helper.readFileInLines(StockfilePath, lineNum);
			String[] element = fileLineContent.split("~",16);
			String fileYieldAsAtDate = element[8];
			String fileSecurityYieldRate = element[9];
			String fileBetaAsAtDate = element[10];
			String fileSecurityBetaRate = element[11];
//保证YieldRate与BetaRate必须至少存在一个值
			if(fileSecurityYieldRate.isEmpty() == true && fileSecurityBetaRate.isEmpty() == true){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]" + "Line number is:" + lineNum + "   " + "Both Yield and Beta Rate is empty!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original Stock file is:" + fileLineContent);
			}
//若有YieldRate，则YieldAsAtDate必须不为空			
			if(fileSecurityYieldRate.isEmpty() == false){
				if(fileYieldAsAtDate.isEmpty() == true){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]" + "Line number is:" + lineNum + "   " + "Yield data is NULL while Yield Rate isn't empty!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original Stock file is:" + fileLineContent);
				}
			}
//若有BetaRate，则BetaAsAtDate必须不为空
			if(fileSecurityBetaRate.isEmpty() == false){
				if(fileBetaAsAtDate.isEmpty() == true){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]" + "Line number is:" + lineNum + "   " + "Beta data is NULL while Beta Rate isn't empty!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original Stock file is:" + fileLineContent);
				}
			}			
		}
		String endTime = df.format(new Date());
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestCase1]Test has finished,end at:" + endTime);
		System.out.println("[TestCase1]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
	}
	@Test(description = "Testing:Stock sample file for Merrill Lynch data format/size verifying testing!")
	public static void testStockFileFormat() throws Exception{
//读取测试Stock Sample文件，按行遍历测试
		String startTime = df.format(new Date());
		System.out.println("[TestForFormat]Begin to test every data's size in each lines,please wait.......");
		System.out.println("[TestForFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestForFormat]Begin to test every data's size in each lines,start at:" + startTime);																																												
		int lineRange = Helper.getTotalLinesOfFile(StockfilePath);
		for(int lineNum = 2;lineNum < lineRange-1;lineNum++){
			String fileLineContent = Helper.readFileInLines(StockfilePath, lineNum);
			String[] element = fileLineContent.split("~",16);
//File端数据准备
			String fileCUSIP = element[0];
			String fileSEDOL = element[1];
			String fileISIN = element[2];
			String fileExchangeId = element[3];
			String fileTickerSymbol = element[4];
			String fileDomicile = element[5];
			String filePrimaryExchangeId = element[6];
			String fileMorningstarSecurityId = element[7];
			String fileYieldAsAtDate = element[8];
			String fileSecurityYieldRate = element[9];
			String fileBetaAsAtDate = element[10];
			String fileSecurityBetaRate = element[11];
			String fileBetaBackfillIndexCode = element[14];
			String fileMorningstarBetaProxyName = element[15];
//新增需求：每行必须有15个"~"
			int columnNumberCount = Helper.getMatchCount("~",fileLineContent);
			if(columnNumberCount != 15){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "There aren't 15 '~' in this line!");
			}
			
//测试数据元素大小是否满足需求规定			
			if(fileCUSIP.length() != 0 && fileCUSIP.length() != 9 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of CUSIP in stock file isn't 9 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of CUSIP in Stock file is:" + fileCUSIP.length());
			}			
			
			if(fileSEDOL.length() != 0 && fileSEDOL.length() != 7 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of SEDOL in stock file isn't 7 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of SEDOL in Stock file is:" + fileSEDOL.length());
			}
			
			if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of ISIN in stock file isn't 12 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ISIN in Stock file is:" + fileISIN.length());
			}
			
			if(fileExchangeId.length() != 0 && fileExchangeId.length() !=10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of ExchangeId in stock file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ExchangeId in Stock file is:" + fileExchangeId.length());
			}
			
			if(fileTickerSymbol.length() != 0 && fileTickerSymbol.length() > 20 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Primary Ticker Symbol in stock file isn't 20 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Symbol in Stock file is:" + fileTickerSymbol.length());
			}
			
			if(fileDomicile.length() != 0 && fileDomicile.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Domicile in stock file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Domicile in Stock file is:" + fileDomicile.length());
			}
			
			if(filePrimaryExchangeId.length() != 0 && filePrimaryExchangeId.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Primary Exchange Id in stock file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Primary ExchangeId in Stock file is:" + filePrimaryExchangeId.length());
			}
			
			if(fileMorningstarSecurityId.length() != 0 && fileMorningstarSecurityId.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Morningstar Security in stock file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of InvestmentId in Stock file is:" + fileMorningstarSecurityId.length());
			}
			
			if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Yield As At date in stock file isn't 8 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Yield As At Date in Stock file is:" + fileYieldAsAtDate.length());
			}
			
//精度为（9,5）
			if(fileSecurityYieldRate.length() != 0 && Helper.isDecimal(fileSecurityYieldRate) == true && Helper.getDecimalScale(fileSecurityYieldRate) != 5 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The decimal scale of Security Yield Rate in stock file isn't 5 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Security Yield Rate's decimal in Stock file is:" + Helper.getDecimalScale(fileSecurityYieldRate));
			}
			
			if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Beta As At date in stock file isn't 8 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Beta As At Date in Stock file is:" + fileBetaAsAtDate.length());
			}
			
//精度为（9,3）			
			if(fileSecurityBetaRate.length() != 0 && Helper.isDecimal(fileSecurityBetaRate) == true && Helper.getDecimalScale(fileSecurityBetaRate) != 3 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The decimal scale of Security Beta Rate in stock file isn't 3 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Security Beta Rate's decimal in Stock file is:" + Helper.getDecimalScale(fileSecurityBetaRate));
			}
					
			if(fileBetaBackfillIndexCode.length() != 0 && fileBetaBackfillIndexCode.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Beta Backfill Index Code in stock file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Beta Backfill Index Code in Stock file is:" + fileBetaBackfillIndexCode.length());
			}
			if(fileMorningstarBetaProxyName.length() != 0 && fileMorningstarBetaProxyName.length() > 40 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Morningstar Beta Proxy Name in stock file isn't 40 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Stock file is:" + fileLineContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Morningstar Beta Proxy Name in Stock file is:" + fileMorningstarBetaProxyName.length());
			}
		}
		String endTime = df.format(new Date());
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestCase2]Test has finished,end at:" + endTime);
		System.out.println("[TestCase2]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
	}

//content test	
	@Test(description = "Testing:Stock sample file")
	public static void testStockFileContent() throws Exception{
//读取测试Fund Sample文件，按行遍历测试
		String startTime = df.format(new Date());
		HashMap<String,String> perfId2CUSIP = new HashMap<String,String>();
		HashMap<String,String> perfId2SEDOL = new HashMap<String,String>();
		HashMap<String,String> perfId2ISIN = new HashMap<String,String>();
		HashMap<String,String> perfId2ExchangeId = new HashMap<String,String>();
		HashMap<String,String> perfId2Symbol = new HashMap<String,String>();
//		HashMap<String,String> perfId2CompanyId = new HashMap<String,String>();
//		HashMap<String,String> perfId2InvestmentId = new HashMap<String,String>();
		HashMap<String,String> perfId2YieldAsAtDate = new HashMap<String,String>();
		HashMap<String,String> perfId2SecurityYieldRate = new HashMap<String,String>();
		HashMap<String,String> perfId2BetaAsAtDate = new HashMap<String,String>();
		HashMap<String,String> perfId2BetaRate = new HashMap<String,String>();
		HashMap<String,String> perfId2ProxyId = new HashMap<String,String>();
		
		System.out.println("[TestForContent]Begin to test every data's contents in each lines,please wait.......");																																												
		System.out.println("Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestForContent]Begin to test every data's contents in each lines,start at:" + startTime);
		int lineRange = Helper.getTotalLinesOfFile(StockfilePath);
//获取Stock Sample File中的所有PerformanceId的组合
		String performanceIdBuild = buildPerformanceId(7);

//DB端数据准备		
		String sqlToGetCUSIP = "SELECT PerformanceId,CUSIP FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetSEDOL = "SELECT PerformanceId,SEDOL FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetISIN  = "SELECT PerformanceId,ISIN FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetExchangeId = "SELECT PerformanceId,ExchangeId FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetTickerSymbol = "SELECT PerformanceId,Symbol FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
//		String sqlToGetCompanyId = "SELECT PerformanceId,CompanyId FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
//		String sqlToGetInvestmenId = "SELECT PerformanceId,InvestmentId FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '2' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '2' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '2' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '2' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetProxyId = "SELECT PerformanceId,ProxyId FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '2' AND PerformanceId IN (" + performanceIdBuild + ")";
			
		
//File端数据准备
		String fileCUSIP = null;
		String fileSEDOL = null;
		String fileISIN = null;
		String fileExchangeId = null;
		String fileTickerSymbol = null;
//		String fileDomicile = null;				
//		String filePrimaryExchangeId = null;
		String fileMorningstarSecurityId = null;
		String fileYieldAsAtDate = null;
		String fileSecurityYieldRate = null;
		String fileBetaAsAtDate = null;
		String fileSecurityBetaRate = null;
		String fileBetaBackfillIndexCode = null;
		String fileMorningstarBetaProxyName = null;
					
		List<String> secIdList = new ArrayList<String>();
		HashMap<String,String> perfId2ProxyIdMap = new HashMap<String,String>();
		secIdList = loadAssignDataToList(7);		
		String sqlToGetProxyName = "SELECT InvestmentId,InvestmentName FROM dbo.InvestmentIdDimension WHERE InvestmentId IN(" + performanceIdBuild + ")";
		perfId2ProxyIdMap = DBCommons.getDataHashMap(sqlToGetProxyName, Database.Vertica2);

//只有Equity有
//			String sqlToGetPrimaryExchangeId = "";
//			String sqlToGetMorningstarSecurityId = "SELECT InvestmentId FROM dbo.PerformanceIdDimension WHERE PerformanceId = " + "'" + PerfId.get(0) + "'";
//		String sqlToGetYieldAsAtDate = "SELECT EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE PerformanceId = ?";

		perfId2CUSIP = DBCommons.getDataHashMap(sqlToGetCUSIP, Database.Vertica2);
		perfId2SEDOL = DBCommons.getDataHashMap(sqlToGetSEDOL, Database.Vertica2);
		perfId2ISIN = DBCommons.getDataHashMap(sqlToGetISIN, Database.Vertica2);
		perfId2ExchangeId = DBCommons.getDataHashMap(sqlToGetExchangeId, Database.Vertica2);
		perfId2Symbol = DBCommons.getDataHashMap(sqlToGetTickerSymbol, Database.Vertica2);
		perfId2YieldAsAtDate = DBCommons.getDataHashMap(sqlToGetYieldAsAtDate, Database.Vertica2);
		perfId2SecurityYieldRate = DBCommons.getDataHashMap(sqlToGetYieldRate, Database.Vertica2);
		perfId2BetaAsAtDate = DBCommons.getDataHashMap(sqlToGetBetaAsAtDate, Database.Vertica2);
		perfId2BetaRate = DBCommons.getDataHashMap(sqlToGetBetaRate, Database.Vertica2);
		perfId2ProxyId = DBCommons.getDataHashMap(sqlToGetProxyId, Database.Vertica2);		
//		perfId2CompanyId = DBCommons.getDataHashMap(sqlToGetCompanyId, Database.Vertica2);
//		perfId2InvestmentId = DBCommons.getDataHashMap(sqlToGetInvestmenId, Database.Vertica2);
		
		String fileContent = null;
		String[] element = null;
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileContent = Helper.readFileInLines(StockfilePath, lineNum);
				element = fileContent.split("~",16);
//File端数据准备
				fileCUSIP = element[0];
				fileSEDOL = element[1].trim();
				fileISIN = element[2].trim();
				fileExchangeId = element[3].trim();
				fileTickerSymbol = element[4].trim();
//				fileDomicile = element[5];				
//				filePrimaryExchangeId = element[6];
				fileMorningstarSecurityId = element[7].trim();
				fileYieldAsAtDate = element[8].trim();
				fileSecurityYieldRate = element[9].trim();
				fileBetaAsAtDate = element[10].trim();
				fileSecurityBetaRate = element[11].trim();
				fileBetaBackfillIndexCode = element[14].trim();
				fileMorningstarBetaProxyName = element[15].trim();
//测试file中的每一个数据的准确性
	
//测试CUSIP
				String actCUSIPInDb = perfId2CUSIP.get(fileMorningstarSecurityId);
				if(actCUSIPInDb != null){
					String realCUSIPInDb = actCUSIPInDb.trim();
					if( realCUSIPInDb.equals(fileCUSIP) == false){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "CUSIP not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual CUSIP in DB is:" + realCUSIPInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual CUSIP in StockFile is:" + fileCUSIP);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
					}
				}
				
//测试SEDOL	
					String actSEDOLInDb = perfId2SEDOL.get(fileMorningstarSecurityId);
					if(actSEDOLInDb != null){
						String realSEDOLInDb = actSEDOLInDb.trim();
						if(!realSEDOLInDb.equals(fileSEDOL)){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "SEDOL not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual SEDOL in DB is:" + realSEDOLInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual SEDOL in StockFile is:" + fileSEDOL);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
						}							
					}
										
				
//测试ISIN
				String actISINInDb = perfId2ISIN.get(fileMorningstarSecurityId);
				if(actISINInDb != null){
					String realISINInDb = actISINInDb.trim();
					if( realISINInDb.equals(fileISIN) == false ){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ISIN in DB is:" + realISINInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ISIN in StockFile is:" + fileISIN);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
					}
				}

				
//测试ExchangeId
				String actExchangeIdInDb = perfId2ExchangeId.get(fileMorningstarSecurityId);
				if(actExchangeIdInDb != null){
					String realExchangeIdInDb = actExchangeIdInDb.trim();
					if(realExchangeIdInDb.equals(fileExchangeId) == false ){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "ExchangeId not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ExchangeId in DB is:" + realExchangeIdInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ExchangeId in StockFile is:" + fileExchangeId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");		
					}
				}
				
//测试Symbol
				String actTicherSymbolInDb = perfId2Symbol.get(fileMorningstarSecurityId);
				if(actTicherSymbolInDb != null){
					String realTicherSymbolInDb = actTicherSymbolInDb.trim();
					if(realTicherSymbolInDb.equalsIgnoreCase(fileTickerSymbol) == false ){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Symbol not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Symbol in DB is:" + realTicherSymbolInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Symbol in StockFile is:" + fileTickerSymbol);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");					
					}
				}
				
//测试Domicile/Country of Business Id
	//1.	Use performanceId to get CompanyId from dbo.PerformanceIdDimension.
	//2.	Use CompanyId from dbo.CompanyIdDimension to get CountryId
	//			String companyId = perfId2CompanyId.get(fileMorningstarSecurityId);			
	//			String sqlToGetDomicile = "SELECT CountryId FROM CurrentData.dbo.CompanyIdDimension WHERE CompanyId = '" + companyId + "'"; 
	//			String actDomicile = DBCommons.getData(sqlToGetDomicile, Database.MsSQL1);
	//			System.out.println("开始测试Domicile");
	//			if(actDomicile.equalsIgnoreCase(fileDomicile) == false ){
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Domicile not mapping!");
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in DB is:" + actDomicile);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in StockFile is:" + fileDomicile);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");					
	//			}
	//测试Primary Exchange Id		
	//1.	Use performanceId to get InvestmentId from dbo.PerformanceIdDimension.
	//2.	Use InvestmentId to get exchangeId from dbi.PerformanceSearch
	//			String investmentId = perfId2InvestmentId.get(fileMorningstarSecurityId);
	//			String sqlToGetPrimaryExchangeId = "SELECT ExchangeId FROM dbi.PerformanceSearch WHERE IsPrimary = '1' AND Status = '1' AND investmentId = '" + investmentId + "'";
	//			String actPrimaryExchangeId = DBCommons.getData(sqlToGetPrimaryExchangeId, Database.MsSQL1);
	//			System.out.println("开始测试Primary Exchange Id");
	//			if(actPrimaryExchangeId.equalsIgnoreCase(filePrimaryExchangeId) == false){
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Domicile not mapping!");
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in DB is:" + actDomicile);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in StockFile is:" + fileDomicile);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");										
	//			}
				
//测试Yield As At Date
//				String actyieldAsAtDateInDb = perfId2YieldAsAtDate.get(fileMorningstarSecurityId);
//				if(actyieldAsAtDateInDb!=null){
//					if(!actyieldAsAtDateInDb.isEmpty() && !fileYieldAsAtDate.isEmpty()){
//						Date realYieldAsAtDateInDb = Helper.dateParse(actyieldAsAtDateInDb, "yyyy-MM-dd");
//						Date realYieldAsAtDateInFile = Helper.dateParse(actyieldAsAtDateInDb, "yyyyMMdd");
//						if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Yield As At Date not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Stock file is:" + fileContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Yield As At Date in DB is:" + actyieldAsAtDateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Yield As At Date in StockFile is:" + fileYieldAsAtDate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
//						}
//					}														
//				}
				
//测试Security Yield Rate
//				String actYieldRateInDb = perfId2SecurityYieldRate.get(fileMorningstarSecurityId);
//				if(actYieldRateInDb != null){
//					Double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
//					Double DrealYieldRateInDb = Helper.setDoublePrecision(DactYieldRateInDb, 5, BigDecimal.ROUND_DOWN);
//					Double DrealYieldRateInFile = Double.parseDouble(fileSecurityYieldRate);
//					if(!DrealYieldRateInDb.equals(DrealYieldRateInFile)){
//						if(DrealYieldRateInFile - DrealYieldRateInDb != 9.999999999621423E-6){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Security Yield Rate not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Stock file is:" + fileContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Security Yield Rate in DB is:" + actYieldRateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Security Yield Rate in StockFile is:" + fileSecurityYieldRate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
//						}						
//					}
//				}
				
//测试Beta As At Date
//				String betaAsAtDateInDb = perfId2BetaAsAtDate.get(fileMorningstarSecurityId);
//				if(betaAsAtDateInDb!=null){
//					Date actBetaAsAtDateInDb = Helper.dateParse(betaAsAtDateInDb, "yyyyMMdd");
//					if(fileBetaAsAtDate!=null){
//						Date actFileBetaAsAtDate = Helper.dateParse(fileBetaAsAtDate, "yyyyMMdd");
//						if(actBetaAsAtDateInDb.compareTo(actFileBetaAsAtDate) !=0){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Beta As At Date not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Stock file is:" + fileContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Beta As At Date in DB is:" + betaAsAtDateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Beta As At Date in StockFile is:" + fileBetaAsAtDate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");											
//						}
//					}					
//				}
				
//测试Beta Rate
//				String actBetaRateInDb = perfId2BetaRate.get(fileMorningstarSecurityId);
//				if(actBetaRateInDb != null){
//					Double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
//					Double DrealBetaRateInDb = Helper.setDoublePrecision(DactBetaRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
//					Double DrealBetaRateInFile = Double.parseDouble(fileSecurityBetaRate);
//					if(DrealBetaRateInDb.equals(DrealBetaRateInFile) == false){
//						if(DrealBetaRateInFile - DrealBetaRateInDb != 0.001000000000000334){
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Beta Rate not mapping!");
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Stock file is:" + fileContent);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Rate in DB is:" + actBetaRateInDb);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Rate in StockFile is:" + fileSecurityBetaRate);
//							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
//						}																	
//					}
//				}
				
//测试Beta Backfill Index Code
				String actProxyIdInDb = perfId2ProxyId.get(fileMorningstarSecurityId);
				if(actProxyIdInDb !=null && actProxyIdInDb.equals(fileBetaBackfillIndexCode) == false){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Beta Backfill Index Code not mapping!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Backfill Index Code in DB is:" + actProxyIdInDb);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Backfill Index Code in FundFile is:" + fileBetaBackfillIndexCode);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");											
				}
//测试Morningstar Beta Proxy Name
//get it via ProxyId
			for(String proxyId : secIdList){
				String actInvestmentName = perfId2ProxyIdMap.get(proxyId);
				if(actInvestmentName!=null && actInvestmentName.equalsIgnoreCase(fileMorningstarBetaProxyName) == false){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Morningstar Beta Proxy Name not mapping!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Morningstar Beta Proxy Name in DB is:" + actInvestmentName);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Morningstar Beta Proxy Name in StockFile is:" + fileMorningstarBetaProxyName);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");											
				}
			}
		}
		String endTime = Base.currentSysTime();
		System.out.println("[TestCase3]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestCase3]Test has finished,please check log file for results,end at:" + endTime);
//关闭之前所有的数据库连接		
		DBFreshpool.closeConnection();
	}
	
//duplicate test
		@Test(description = "Testing:Stock sample file for Merrill Lynch data duplicate testing!")
		public static void testStockFileDuplicateData() throws Exception{
			String startTime1 = df.format(new Date());
			System.out.println("[TestForDuplicateData]Begin to test no duplicate lines in Stock sample file,please wait.......");
			System.out.println("[TestForDuplicateData]Test at:" + startTime1);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDuplicateData]Begin to test no duplicate lines in Stock sample file,start at:" + startTime1);
			
			List<String> listFullDuplicateDataContainer = new ArrayList<String>();
			List<String> listDuplicateDatawithNoSedolContainer = new ArrayList<String>();
			List<String> listSecIdContainer = new ArrayList<String>();
			List<String> listSedolContainer = new ArrayList<String>();
			List<String> listCusipContainer = new ArrayList<String>();
			List<String> listIsinContainer = new ArrayList<String>();
			listFullDuplicateDataContainer = loadLineDataToList();
			listDuplicateDatawithNoSedolContainer = loadDataToListNoSedol();
			listSecIdContainer = loadAssignDataToList(7);
			listSedolContainer = loadAssignDataToList(1);
			listCusipContainer = loadAssignDataToList(0);
			listIsinContainer = loadAssignDataToList(2);
			int lineRange = Helper.getTotalLinesOfFile(StockfilePath);
			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				String fileContent = Helper.readFileInLines(StockfilePath, lineNum);
				int lineDuplicateCount = Collections.frequency(listFullDuplicateDataContainer, fileContent);
				if(lineDuplicateCount > 1){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case1]Duplicate lines number is:" + lineDuplicateCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Failed line number is:" + lineNum + "\t" + "\nFailed original content is:" + fileContent);				
				}
			}
	//释放listFullDuplicateDataContainer的内存空间
			if(listFullDuplicateDataContainer.isEmpty() == false){
				listFullDuplicateDataContainer.removeAll(listFullDuplicateDataContainer);
				System.out.println("[Notice]The listFullDuplicateDataContainer has been removed!");
			}
		
			String startTime2 = df.format(new Date());
			System.out.println("[TestForDuplicateData]Begin to test Sedol rules if Stock sample file has duplicate datas beside Sedol,please wait.......");
			System.out.println("[TestForDuplicateData]Test at:" + startTime2);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDuplicateData]Begin to test Sedol rules if Stock sample file has duplicate datas beside Sedol,start at:" + startTime2);
			
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				String fileLineContent = Helper.readFileInLines(StockfilePath, lineNum);
	//去除每行的SEDOL和SecId(保证索引值不包含1的就可以了)
				String[] element = fileLineContent.split("~",16);
				String fileSedol = element[1];
				String fileRealLineContent = element[0]+element[2]+element[3]+element[4]+element[5]+element[6]+element[7]+element[8]+element[9]+element[10]+element[11]+element[12]+element[13]+element[14]+element[15];
				int lineDuplicateCountWithNoSedol = Collections.frequency(listDuplicateDatawithNoSedolContainer,fileRealLineContent);
	//不包含Sedol的其余数据相同则开始判断测试结果
				if(lineDuplicateCountWithNoSedol > 1){								
	//检查当前行的Sedol是否有重复的,若Sedol唯一，则测试失败
					int duplicateSedolCount = Collections.frequency(listSedolContainer, fileSedol);
					if(duplicateSedolCount > 1){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case2]Having same SEDOL while other column datas is same beside SecId,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SEDOL is:" + fileSedol + "\nFailed original content is:" + fileLineContent);							
					}
				}
			}
	//释放listDuplicateDatawithNoSedolContainer的内存空间
			if(listDuplicateDatawithNoSedolContainer.isEmpty() == false){
				listDuplicateDatawithNoSedolContainer.removeAll(listDuplicateDatawithNoSedolContainer);
				System.out.println("[Notice]The listDuplicateDatawithNoSedolContainer has been removed!");
			}
			
			String startTime3 = df.format(new Date());
			System.out.println("[TestForDuplicateData]Begin to test one SecId only map to one SEDOL/ISIN/CUSIP,please wait.......");
			System.out.println("[TestForDuplicateData]Test at:" + startTime3);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestForDuplicateData]Begin to test one SecId only map to one SEDOL/ISIN/CUSIP,start at:" + startTime3);
			for(int lineNum = 2;lineNum < lineRange;lineNum++){
				String fileLineContent = Helper.readFileInLines(StockfilePath, lineNum);
				String[] ele = fileLineContent.split("~", 16);
				String fileCUSIP = ele[0];
				String fileISIN = ele[2];
				String fileSEDOL = ele[1];
				String fileSecId = ele[7];
				int duplicateSecId = Collections.frequency(listSecIdContainer,fileSecId);
				int duplicateSedolCount = Collections.frequency(listSedolContainer, fileSEDOL);			
				int duplicateCUSIP = Collections.frequency(listCusipContainer, fileCUSIP);
				int duplicateISIN = Collections.frequency(listIsinContainer, fileISIN);

				
	//当SEDOL唯一时			
				if(duplicateSedolCount == 1){				
	//对应的SecId也必须唯一				
					if(duplicateSecId > 1){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]Mapping SecId isn't single while SEDOL has only one record,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping SEDOL is:" +fileSEDOL + "\nFailed original content is:" + fileLineContent);
					}
				}
	//当SEDOL不唯一时，对应的SecId数目必须和SEDOL数目相等
				if(fileSEDOL.isEmpty() == false && duplicateSedolCount > 1){
					if(duplicateSecId != duplicateSedolCount){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]The count of mapping SecId isn't the same with SEDOL,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping SEDOL is:" +fileSEDOL + "\nFailed original content is:" + fileLineContent);				
					}
				}
	//当CUSIP唯一时
				if(duplicateCUSIP == 1){
	//对应的SecId也必须唯一
					if(duplicateSecId > 1){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]Mapping SecId isn't single while CUSIP has only one record,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping CUSIP is:" +fileCUSIP + "\nFailed original content is:" + fileLineContent);				
					}
				}
	//当CUSIP不唯一时
				if(fileCUSIP.isEmpty() == false && duplicateCUSIP >1){
	//对应的SecId数目必须与CUSIP数目相等
					if(duplicateCUSIP != duplicateSecId){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]The count of mapping SecId isn't the same with CUSIP,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping CUSIP is:" +fileCUSIP + "\nFailed original content is:" + fileLineContent);				
					}
	//当ISIN唯一时
				if(duplicateISIN == 1){
	//对应的SecId也必须唯一				
					if(duplicateSecId > 1){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]Mapping SecId isn't single while ISIN has only one record,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping ISIN is:" +fileISIN + "\nFailed original content is:" + fileLineContent);				
					}
	//当ISIN不唯一时
				if(fileISIN.isEmpty() == false && duplicateISIN >1){
	//对应的SecId数目必须与ISIN数目相等
					if(duplicateISIN != duplicateSecId){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Testing failed:[Case3]The count of mapping SecId isn't the same with ISIN,line number is:" + lineNum);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "Invalid SecId is:" + fileSecId + "\tMapping ISIN is:" +fileISIN + "\nFailed original content is:" + fileLineContent);				
					}				
				}
			}
	//释放listSecIdContainer，listSedolContainer，listCusipContainer，listIsinContainer
				if(listSecIdContainer.isEmpty() == false){
					listSecIdContainer.removeAll(listSecIdContainer);
					System.out.println("[Notice]The listSecIdContainer has been removed!");
				}
				if(listSedolContainer.isEmpty() == false){
					listSedolContainer.removeAll(listSedolContainer);
					System.out.println("[Notice]The listSedolContainer has been removed!");
				}
				if(listCusipContainer.isEmpty() == false){
					listCusipContainer.removeAll(listCusipContainer);
					System.out.println("[Notice]The listCusipContainer has been removed!");
				}
				if(listIsinContainer.isEmpty() == false){
					listIsinContainer.removeAll(listIsinContainer);
					System.out.println("[Notice]The listIsinContainer has been removed!");
				}
		   }
	    }			
			String endTime = df.format(new Date());
			System.out.println("[TestCase4]Test has finished,please check log file for results");
			System.out.println("End at:" + endTime);
			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase4, "[TestCase4]Test has finished,please check log file for results,end at:" + endTime);	
		}
//将每行数据LOAD到List中	
		public static List<String> loadLineDataToList() throws Exception{
			List<String> list = new ArrayList<String>();
			int lineNum = Helper.getTotalLinesOfFile(StockfilePath);
			for(int i = 2;i < lineNum; i++){
				String fileLineContent =Helper.readFileInLines(StockfilePath, i);
				list.add(fileLineContent);
			}
			return list;
		}
		
//将除了SEDOL和SecId的数据LOAD到List中
	private static List<String> loadDataToListNoSedol() throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(StockfilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(StockfilePath, i);
//去除每行的SEDOL(保证索引值不包含1的就可以了)
			String[] element = fileLineContent.split("~",16);
			String fileRealLineContent = element[0]+element[2]+element[3]+element[4]+element[5]+element[6]+element[7]+element[8]+element[9]+element[10]+element[11]+element[12]+element[13]+element[14]+element[15];
			list.add(fileRealLineContent);
		}
		return list;
	}
	
//将指定列数据Load到一个List中
	private static List<String> loadAssignDataToList(int columnNum) throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(StockfilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(StockfilePath, i);
			String[] element = fileLineContent.split("~",16);
			String str = element[columnNum];
			list.add(str);
			
		}
		return list;
	}	
	
//将指定列数据Load到一个List中(增加规则)
		private static String buildPerformanceId(int columnNum) throws Exception{
			StringBuilder sb = new StringBuilder();
			int lineNum = Helper.getTotalLinesOfFile(StockfilePath);
			for(int i = 2;i < lineNum; i++){
				String fileLineContent =Helper.readFileInLines(StockfilePath, i);
				String[] element = fileLineContent.split("~",16);
				String str = element[columnNum];
				sb.append("'"+str+"',");
				if(i == lineNum-1){
					sb.append("'"+str+"'");
				}				
			}
			return sb.toString();
		}
		
//将指定列数据Load到一个List中(增加规则)
		private static List<String> buildPerformanceIdBuffer(int columnNum,int bufferSize) throws Exception{
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
	
	public static void main(String[] args) throws Exception {
		testPrepare();
		testStockFileComplete();
		testStockFileFormat();
		testStockFileContent();
		testStockFileDuplicateData();
	}
}
