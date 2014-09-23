package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.morningstar.FundAutoTest.commons.*;
import com.morningstar.FundAutoTest.commons.testbase.Base;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
//import org.testng.Assert;

import com.ibm.icu.text.SimpleDateFormat;

public class FundPakcageDummy {

	/**
	 * @author Stefan.hou
	 * @throws IOException 
	 * @throws SQLException 
	 */
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	public static String currenTime = df.format(new Date());
	
	public static String testLogPath = "./log/TestLog/MerrillLynch/ML-22/";
	public static String testLogNameCase1 = "completenessTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase2 = "formatTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase3 = "contentTestResult-" + currenTime + "." + "log";
	public static String testLogNameCase4 = "contentTestManualCheckList-" + currenTime + "." + "log";
	public static String testLogNameCase5 = "fileDuplicateDataTestResult-" + currenTime + "." + "log";
	
	public static String testLogTopic1 = "Fund Sample file for Merrill Lynch completeness testing";
	public static String testLogTopic2 = "Fund Sample file for Merrill Lynch format testing";
	public static String testLogTopic3 = "Fund Sample file for Merrill Lynch content verify testing";
	public static String testLogTopic4 = "Fund Sample file for Merrill Lynch content need manual checking list";
	public static String testLogTopic5 = "Fund Sample file for Merrill Lynch duplicate data checking";
	public static String FundfilePath = "C:/HJG_WORK/HJG_Project/ML_Project/FundDemoFile/20140318/FundPackage.txt";
		
	
	@BeforeClass(description = "Testing preparing!")
	public static void testPrepare() throws IOException{
//生成自定义测试结果日志文件
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase1, testLogTopic1);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase2, testLogTopic2);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase3, testLogTopic3);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase4, testLogTopic4);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase5, testLogTopic5);
	}
	
	
	@Test(description = "Testing:FUND sample file for Merrill Lynch completeness testing!")	
	public static void testFundFileCompletement() throws IOException, SQLException {
//		String StartTime1 = df.format(new Date());
//		System.out.println("[TestForCompletment]Begin to test file records count numbers,please wait... ...");
//		System.out.println("[TestForCompletment]Test at:" + StartTime1);
//		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test file records count numbers,start at:" + StartTime1);
////基本测试用例数据准备
//        String sql_getCountForBothHaveYieldAndBeta = Helper.readFileByLines("./config/Merrill_Lynch/ML-22/getCountForBothHaveYieldAndBeta.sql");
//        String sql_getCountForOnlyHaveBeta = Helper.readFileByLines("./config/Merrill_Lynch/ML-22/getCountForOnlyHaveBeta.sql");
//        String sql_getCountForOnlyHaveYield = Helper.readFileByLines("./config/Merrill_Lynch/ML-22/getCountForOnlyHaveYield.sql");
//
////从Oidevdb81\oibtaoutputdb81获取总记录数
//		String countOnlyBeta = DBCommons.getData(sql_getCountForOnlyHaveBeta, Database.MsSQL1);
//		String countOnlyYield = DBCommons.getData(sql_getCountForOnlyHaveYield, Database.MsSQL1);
//		String countBothYieldBeta = DBCommons.getData(sql_getCountForBothHaveYieldAndBeta, Database.MsSQL1);
//		int totalSQLRecords = (Integer.parseInt(countOnlyBeta) - Integer.parseInt(countBothYieldBeta)) + (Integer.parseInt(countOnlyYield) - Integer.parseInt(countBothYieldBeta)) + Integer.parseInt(countBothYieldBeta);
//		
////从Fund sample file中获取总记录数(新要求，至少samplefile这面的记录数要大于DB端的记录数)
//		int totalFileRecords = Helper.getTotalLinesOfFile(FundfilePath) - 2;
//		if (totalFileRecords <= totalSQLRecords){
//			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]totalFileRecords is less than totalSQLRecords!");
//			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Total records number in DataBase is:" + totalSQLRecords);
//			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Total records number in Fund Sample file is:" + totalFileRecords);			
//		}
//		else{
//			System.out.println("[TestForCompletment]NOTICE:totalFileRecords is larger than totalSQLRecords!");
//		}
//		Assert.assertTrue(recordsEquals(totalSQLRecords, totalFileRecords),"Records number are not same!");	
		
//2013-11-01 新增QA需求，若一条记录中即不包含Yield rate数据也不包含Beta rate数据，也视为测试失败(新增加：InvestmentId也必须存在)
		String StartTime2 = df.format(new Date());
		System.out.println("[TestForCompletment]Begin to test no yield and no beta rate in sample file,please wait... ...");
		System.out.println("[TestForCompletment]Test at:" + StartTime2);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test no yield and no beta rate in sample file,start at:" + StartTime2);
		int lineRange = Helper.getTotalLinesOfFile(FundfilePath);
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileContent = Helper.readFileInLines(FundfilePath, lineNum);
			String[] element = fileContent.split("~",16);
			String fileMorningstarSecurityId = element[7];
			String fileSecurityYieldRate = element[9];
			String fileSecurityBetaRate = element[11];
			if(fileSecurityYieldRate == null && fileSecurityBetaRate == null){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]" + "Line number is:" + lineNum + "   " + "No yield and beta data found in this line!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original Fund file is:" + fileContent);			
			}
			if(fileMorningstarSecurityId == null){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Testing failed:[Case1]" + "Line number is:" + lineNum + "   " + "No SecId in this line!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original Fund file is:" + fileContent);						
			}
		}
		
		String endTime = df.format(new Date());
		System.out.println("[FINISH]Test case1 has finished,please check log file for results");
		System.out.println("End at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestCase1]Test has finished,please check log file for results,end at:" + endTime);
	
	}


	@Test(description = "Testing:FUND sample file for Merrill Lynch data format/size verifying testing!")
	public static void testFundFileFormat() throws Exception{
//读取测试Fund Sample文件，按行遍历测试
		String startTime = df.format(new Date());
		System.out.println("[TestForFormat]Begin to test every data's size in each lines,please wait.......");
		System.out.println("[TestForFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestForFormat]Begin to test every data's size in each lines,start at:" + startTime);																																												
		int lineRange = Helper.getTotalLinesOfFile(FundfilePath);
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileContent = Helper.readFileInLines(FundfilePath, lineNum);
//注意该方法的第二个参数，设置为16，表示正则模式将被应用15次
			String[] element = fileContent.split("~",16);
//File端数据准备
			String fileCUSIP = element[0];
			String fileSEDOL = element[1];
			String fileISIN = element[2];
			String fileExchangeId = element[3];
			String fileTickerSymbol = element[4];
			String fileDomicile = element[5];
			String fileMorningstarSecurityId = element[7];
			String fileYieldAsAtDate = element[8];
			String fileSecurityYieldRate = element[9];
			String fileBetaAsAtDate = element[10];
			String fileSecurityBetaRate = element[11];
			String fileBetaBackfillIndexCode = element[14];
			String fileMorningstarBetaProxyName = element[15];
//新增需求：1 每行必须有15个"~"
			int columnNumberCount = Helper.getMatchCount("~",fileContent);
			if(columnNumberCount != 15){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "There aren't 15 '~' in this line!");
			}
			
//测试数据元素大小是否满足需求规定			
			if(fileCUSIP.length() != 0 && fileCUSIP.length() != 9 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of CUSIP in fund file isn't 9 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of CUSIP in FUND file is:" + fileCUSIP.length());
			}			
//SEDOL没确定，暂时不测试			
			if(fileSEDOL.length() != 0 && fileSEDOL.length() != 7 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of SEDOL in fund file isn't 7 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of SEDOL in FUND file is:" + fileSEDOL.length());
			}
			if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of ISIN in fund file isn't 12 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ISIN in FUND file is:" + fileISIN.length());
			}
			if(fileExchangeId.length() != 0 && fileExchangeId.length() !=10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of ExchangeId in fund file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of ExchangeId in FUND file is:" + fileExchangeId.length());
			}
			if(fileTickerSymbol.length() != 0 && fileTickerSymbol.length() > 20 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Primary Ticker Symbol in fund file isn't 20 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Symbol in FUND file is:" + fileTickerSymbol.length());
			}
			if(fileDomicile.length() != 0 && fileDomicile.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Domicile in fund file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Domicile in FUND file is:" + fileDomicile.length());
			}
			
			if(fileMorningstarSecurityId.length() != 0 && fileMorningstarSecurityId.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Morningstar Security in fund file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of InvestmentId in FUND file is:" + fileMorningstarSecurityId.length());
			}
			if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Yield As At date in fund file isn't 8 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Yield As At Date in FUND file is:" + fileYieldAsAtDate.length());
			}
			
//精度为（9,5）
			if(fileSecurityYieldRate.length() != 0 && Helper.isDecimal(fileSecurityYieldRate) == true && Helper.getDecimalScale(fileSecurityYieldRate) != 5 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The decimal scale of Security Yield Rate in fund file isn't 6 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Security Yield Rate's decimal in FUND file is:" + Helper.getDecimalScale(fileSecurityYieldRate));
			}
			
			if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Beta As At date in fund file isn't 8 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Beta As At Date in FUND file is:" + fileBetaAsAtDate.length());
			}
			
//精度为（9,3）			
			if(fileSecurityBetaRate.length() != 0 && Helper.isDecimal(fileSecurityBetaRate) == true && Helper.getDecimalScale(fileSecurityBetaRate) != 3 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The decimal scale of Security Beta Rate in fund file isn't 3 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Security Beta Rate's decimal in FUND file is:" + Helper.getDecimalScale(fileSecurityBetaRate));
			}			
			
			if(fileBetaBackfillIndexCode.length() != 0 && fileBetaBackfillIndexCode.length() != 10 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Beta Backfill Index Code in fund file isn't 10 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Beta Backfill Index Code in FUND file is:" + fileBetaBackfillIndexCode.length());
			}
			if(fileMorningstarBetaProxyName.length() != 0 && fileMorningstarBetaProxyName.length() > 40 ){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Testing failed:[Case2]" + "Line number is:" + lineNum + "   " + "The size of Morningstar Beta Proxy Name in fund file isn't 40 bytes!");
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Original Fund file is:" + fileContent);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2,"Actual size of Morningstar Beta Proxy Name in FUND file is:" + fileMorningstarBetaProxyName.length());
			}
		}
		String endTime = df.format(new Date());
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestCase2]Test has finished,end at:" + endTime);
		System.out.println("[TestCase2]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
	}
	

	@Test(description = "Testing:FUND sample file for Merrill Lynch data content verifying testing!")
	public static void testFundFileContent() throws Exception{
		//读取测试Fund Sample文件，按行遍历测试
		String startTime = df.format(new Date());
		HashMap<String,String> perfId2CUSIP = new HashMap<String,String>();
		HashMap<String,String> perfId2SEDOL = new HashMap<String,String>();
		HashMap<String,String> perfId2ISIN = new HashMap<String,String>();
		HashMap<String,String> perfId2ExchangeId = new HashMap<String,String>();
		HashMap<String,String> perfId2Symbol = new HashMap<String,String>();
		HashMap<String,String> perfId2YieldAsAtDate = new HashMap<String,String>();
		HashMap<String,String> perfId2SecurityYieldRate = new HashMap<String,String>();
		HashMap<String,String> perfId2BetaAsAtDate = new HashMap<String,String>();
		HashMap<String,String> perfId2BetaRate = new HashMap<String,String>();
		HashMap<String,String> perfId2ProxyId = new HashMap<String,String>();
		HashMap<String,String> investmentId2investmentName = new HashMap<String,String>();
		
		System.out.println("[TestForContent]Begin to test every data's contents in each lines,please wait.......");																																												
		System.out.println("Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestForContent]Begin to test every data's contents in each lines,start at:" + startTime);
		int lineRange = Helper.getTotalLinesOfFile(FundfilePath);
//获取Fund Sample File中的所有PerformanceId的组合
		String performanceIdBuild = buildString(7);
//获取Fund Sample File中的所有ProxyId的组合
		String investmentIdBuild = buildString(14);

//DB端数据准备		
		String sqlToGetCUSIP = "SELECT PerformanceId,CUSIP FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";				
		String sqlToGetSEDOL = "SELECT InvestmentId,Identifier FROM dbi.InvestmentPrimaryIdentifiers WHERE IdentifierType = 3 AND Identifier is not null AND InvestmentId IN (" + performanceIdBuild + ")";
		String sqlToGetISIN  = "SELECT PerformanceId,ISIN FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetExchangeId = "SELECT PerformanceId,ExchangeId FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetTickerSymbol = "SELECT PerformanceId,Symbol FROM dbo.PerformanceIdDimension WHERE PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetYieldAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetYieldRate = "SELECT PerformanceId,Average3MonthYield FROM dbo.MLAverage3MonthYieldForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetBetaAsAtDate = "SELECT PerformanceId,EndDate FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetBetaRate = "SELECT PerformanceId,Beta FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetProxyId = "SELECT PerformanceId,ProxyId FROM dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '1' AND PerformanceId IN (" + performanceIdBuild + ")";
		String sqlToGetProxyName = "SELECT InvestmentId,InvestmentId FROM dbo.InvestmentIdDimension WHERE InvestmentId IN (" + investmentIdBuild + ")";				
//File端数据准备
		String fileCUSIP = "";
		String fileSEDOL = "";
		String fileISIN = "";
		String fileExchangeId = "";
		String fileTickerSymbol = "";
		String fileMorningstarSecurityId = "";
		String fileYieldAsAtDate = "";
		String fileSecurityYieldRate = "";
		String fileBetaAsAtDate = "";
		String fileSecurityBetaRate = "";
		String fileBetaBackfillIndexCode = "";
		String fileMorningstarBetaProxyName = "";
						
//SQL执行部分
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
		investmentId2investmentName = DBCommons.getDataHashMap(sqlToGetProxyName, Database.Vertica2);
		
		String fileContent = null;
		String[] element = null;
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
				fileContent = Helper.readFileInLines(FundfilePath, lineNum);
				element = fileContent.split("~",16);
//File端数据准备
				fileCUSIP = element[0].trim();
				fileSEDOL = element[1].trim();
				fileISIN = element[2].trim();
				fileExchangeId = element[3].trim();
				fileTickerSymbol = element[4].trim();
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
					if(realCUSIPInDb.equals(fileCUSIP) == false){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "CUSIP not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual CUSIP in DB is:" + realCUSIPInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual CUSIP in FundFile is:" + fileCUSIP);
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
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual SEDOL in FundFile is:" + fileSEDOL);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
					}
				}
										
//测试ISIN
				String actISINInDb = perfId2ISIN.get(fileMorningstarSecurityId);
				if(actISINInDb != null){
					String realISINInDb = actISINInDb.trim();
					if(!realISINInDb.equals(fileISIN)){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "ISIN not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ISIN in DB is:" + realISINInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ISIN in FundFile is:" + fileISIN);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
					}
				}
																
//测试ExchangeId
				String actExchangeIdInDb = perfId2ExchangeId.get(fileMorningstarSecurityId);
				if(actExchangeIdInDb != null){
					String realExchangeIdInDb = actExchangeIdInDb.trim();
					if(!realExchangeIdInDb.equals(fileExchangeId)){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "ExchangeId not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ExchangeId in DB is:" + realExchangeIdInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual ExchangeId in FundFile is:" + fileExchangeId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");		
					}
				}
																
//测试Symbol
				String actTicherSymbolInDb = perfId2Symbol.get(fileMorningstarSecurityId);
				if(actTicherSymbolInDb != null){
					String realTicherSymbolInDb = actTicherSymbolInDb.trim();
					if(!realTicherSymbolInDb.equals(fileTickerSymbol)){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Symbol not mapping!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Symbol in DB is:" + actTicherSymbolInDb);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Symbol in FundFile is:" + fileTickerSymbol);
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
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in FundFile is:" + fileDomicile);
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
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Domicile in FundFile is:" + fileDomicile);
	//				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");										
	//			}
//测试Yield As At Date
					String actyieldAsAtDateInDb = perfId2YieldAsAtDate.get(fileMorningstarSecurityId);
					if(actyieldAsAtDateInDb!=null){
						if(!actyieldAsAtDateInDb.isEmpty() && !fileYieldAsAtDate.isEmpty()){
							Date realYieldAsAtDateInDb = Helper.dateParse(actyieldAsAtDateInDb, "yyyy-MM-dd");
							Date realYieldAsAtDateInFile = Helper.dateParse(actyieldAsAtDateInDb, "yyyyMMdd");
							if(!realYieldAsAtDateInDb.equals(realYieldAsAtDateInFile)){
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Yield As At Date not mapping!");
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Yield As At Date in DB is:" + actyieldAsAtDateInDb);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Yield As At Date in FundFile is:" + fileYieldAsAtDate);
								CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
							}
						}														
					}
					
//测试Security Yield Rate
				String actYieldRateInDb = perfId2SecurityYieldRate.get(fileMorningstarSecurityId);
				if(actYieldRateInDb != null){
					Double DactYieldRateInDb = Double.parseDouble(actYieldRateInDb);
					Double DrealYieldRateInDb = Helper.setDoublePrecision(DactYieldRateInDb, 5, BigDecimal.ROUND_DOWN);
					Double DrealYieldRateInFile = Double.parseDouble(fileSecurityYieldRate);
					if(!DrealYieldRateInDb.equals(DrealYieldRateInFile)){
						if(DrealYieldRateInFile - DrealYieldRateInDb != 9.999999999621423E-6){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Security Yield Rate not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Security Yield Rate in DB is:" + actYieldRateInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Security Yield Rate in FundFile is:" + fileSecurityYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
						}						
					}
				}
				
//测试Beta As At Date
				String betaAsAtDateInDb = perfId2BetaAsAtDate.get(fileMorningstarSecurityId);
				if(betaAsAtDateInDb!=null){
					Date actBetaAsAtDateInDb = Helper.dateParse(betaAsAtDateInDb, "yyyyMMdd");
					if(fileBetaAsAtDate!=null){
						Date actFileBetaAsAtDate = Helper.dateParse(fileBetaAsAtDate, "yyyyMMdd");
						if(actBetaAsAtDateInDb.compareTo(actFileBetaAsAtDate) !=0){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Beta As At Date not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Beta As At Date in DB is:" + betaAsAtDateInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Actual Beta As At Date in FundFile is:" + fileBetaAsAtDate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");											
						}
					}					
				}				
//测试Beta Rate
				String actBetaRateInDb = perfId2BetaRate.get(fileMorningstarSecurityId);
				if(actBetaRateInDb != null){
					Double DactBetaRateInDb = Double.parseDouble(actBetaRateInDb);
					Double DrealBetaRateInDb = Helper.setDoublePrecision(DactBetaRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
					Double DrealBetaRateInFile = Double.parseDouble(fileSecurityBetaRate);
					if(DrealBetaRateInDb.equals(DrealBetaRateInFile) == false){
						if(DrealBetaRateInFile - DrealBetaRateInDb != 0.001000000000000334){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Beta Rate not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Rate in DB is:" + actBetaRateInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Beta Rate in FundFile is:" + fileSecurityBetaRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");
						}																	
					}
				}				
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
				if(actProxyIdInDb != null){
					if(!fileBetaBackfillIndexCode.isEmpty()){
						String actInvestmentNameInDb = investmentId2investmentName.get(fileBetaBackfillIndexCode);
						if(actInvestmentNameInDb.equalsIgnoreCase(fileMorningstarBetaProxyName) == false){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Testing failed:[Case3]" + "Line number is:" + lineNum + "   " + "Morningstar Beta Proxy Name not mapping!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Original Fund file is:" + fileContent);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Morningstar Beta Proxy Name in DB is:" + actInvestmentNameInDb);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Morningstar Beta Proxy Name in FundFile is:" + fileMorningstarBetaProxyName);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "==============================================");											
						}																
					}												
		}
}
		String endTime = Base.currentSysTime();
		System.out.println("[TestCase3]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestCase3]Test has finished,please check log file for results,end at:" + endTime);
//关闭之前所有的数据库连接
		DBFreshpool.closeConnection();	}
	
/*	
	private static boolean recordsEquals(int totalSQLRecords,int totalFileRecords) {
		Assert.assertEquals(totalSQLRecords, totalFileRecords);
		return false;
	}
*/	

//duplicate test
	@Test(description = "Testing:FUND sample file for Merrill Lynch data duplicate testing!")
	public static void testFundFileDuplicateData() throws Exception{
		String startTime1 = df.format(new Date());
		System.out.println("[TestForDuplicateData]Begin to test no duplicate lines in Fund sample file,please wait.......");
		System.out.println("[TestForDuplicateData]Test at:" + startTime1);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "[TestForDuplicateData]Begin to test no duplicate lines in Fund sample file,start at:" + startTime1);
		
		List<String> listFullDuplicateDataContainer = new ArrayList<String>();
		List<String> listDuplicateDatawithNoSedolAndSecIdContainer = new ArrayList<String>();
		List<String> listSecIdContainer = new ArrayList<String>();
		List<String> listSedolContainer = new ArrayList<String>();
		
		listFullDuplicateDataContainer = loadLineDataToList();
		listDuplicateDatawithNoSedolAndSecIdContainer = loadDataToListNoSedolAndSecId();
		listSecIdContainer = loadAssignDataToList(7);
		listSedolContainer = loadAssignDataToList(1);
		
		int lineRange = Helper.getTotalLinesOfFile(FundfilePath);
		
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileContent = Helper.readFileInLines(FundfilePath, lineNum);
			int lineDuplicateCount = Collections.frequency(listFullDuplicateDataContainer, fileContent);
			if(lineDuplicateCount > 1){
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Testing failed:[Case1]Duplicate lines number is:" + lineDuplicateCount);
				CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Failed line number is:" + lineNum + "\t" + "\nFailed original content is:" + fileContent);				
			}
		}
//释放listFullDuplicateDataContainer的内存空间
		if(listFullDuplicateDataContainer.isEmpty() == false){
			listFullDuplicateDataContainer.removeAll(listFullDuplicateDataContainer);
			System.out.println("[Notice]The listFullDuplicateDataContainer has been removed!");
		}
	
		String startTime2 = df.format(new Date());
		System.out.println("[TestForDuplicateData]Begin to test No duplicate SEDOL in Fund sample file,please wait.......");
		System.out.println("[TestForDuplicateData]Test at:" + startTime2);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "[TestForDuplicateData]Begin to test No duplicate SEDOL in Fund sample file,start at:" + startTime2);
		
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileLineContent = Helper.readFileInLines(FundfilePath, lineNum);
			String[] element = fileLineContent.split("~",16);
			String fileSedol = element[1];			
			int lineDuplicateSedolCount = Collections.frequency(listSedolContainer,fileSedol);
//若全文中出现重复的SEDOL，则测试失败
			if(fileSedol.isEmpty() == false && lineDuplicateSedolCount > 1){								
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Testing failed:[Case2]Having duplicate SEDOL,the line number is:" + lineNum);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Invalid SEDOL is:" + fileSedol + "\tDuplicate SEDOL count is:" + lineDuplicateSedolCount + "\nFailed original content is:" + fileLineContent);							
			}
		}
		
		String startTime3 = df.format(new Date());
		System.out.println("[TestForDuplicateData]Begin to test beside SEDOL other datas need to be totally same when SecId has duplicate records,please wait.......");
		System.out.println("[TestForDuplicateData]Test at:" + startTime3);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "[TestForDuplicateData]Begin to test beside SEDOL other datas need to be totally same when SecId has duplicate records,start at:" + startTime3);
		for(int lineNum = 2;lineNum < lineRange;lineNum++){
			String fileLineContent = Helper.readFileInLines(FundfilePath, lineNum);
			String[] ele = fileLineContent.split("~", 16);
			String fileSecId = ele[7];
			String fileNoSedolAndSecId = ele[0]+ele[2]+ele[3]+ele[4]+ele[5]+ele[6]+ele[8]+ele[9]+ele[10]+ele[11]+ele[12]+ele[13]+ele[14]+ele[15];
			int duplicateSecId = Collections.frequency(listSecIdContainer,fileSecId);
			if(duplicateSecId > 1){
				int duplicateDatawithNoSedolAndSecIdCount = Collections.frequency(listDuplicateDatawithNoSedolAndSecIdContainer, fileNoSedolAndSecId);
//若除SecId和Sedol外，其他值不是唯一的，则测试失败				
				if(duplicateDatawithNoSedolAndSecIdCount == 1){
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Testing failed:[Case3]Other datas isn't same beside SEDOL while SecId has copied records in this line,the line number is:" + lineNum);
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "Copied SecId is:" + fileSecId + "\nFailed original content is:" + fileLineContent);							
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

			if(listDuplicateDatawithNoSedolAndSecIdContainer.isEmpty() == false){
				listDuplicateDatawithNoSedolAndSecIdContainer.removeAll(listDuplicateDatawithNoSedolAndSecIdContainer);
				System.out.println("[Notice]The listDuplicateDatawithNoSedolAndSecIdContainer has been removed!");
			}
		
		String endTime = df.format(new Date());
		System.out.println("[TestCase4]Test has finished,please check log file for results");
		System.out.println("End at:" + endTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase5, "[TestCase4]Test has finished,please check log file for results,end at:" + endTime);	
	}
	
//将每行数据LOAD到List中	
	public static List<String> loadLineDataToList() throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(FundfilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(FundfilePath, i);
			list.add(fileLineContent);
		}
		return list;
	}
	
//将除了SEDOL和SecId的数据LOAD到List中
	private static List<String> loadDataToListNoSedolAndSecId() throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(FundfilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(FundfilePath, i);
//去除每行的SEDOL(保证索引值不包含1的就可以了)
			String[] element = fileLineContent.split("~",16);
			String fileRealLineContent = element[0]+element[2]+element[3]+element[4]+element[5]+element[6]+element[8]+element[9]+element[10]+element[11]+element[12]+element[13]+element[14]+element[15];
			list.add(fileRealLineContent);
		}
		return list;
	}
	
//将指定列数据Load到一个List中
	private static List<String> loadAssignDataToList(int columnNum) throws Exception{
		List<String> list = new ArrayList<String>();
		int lineNum = Helper.getTotalLinesOfFile(FundfilePath);
		for(int i = 2;i < lineNum; i++){
			String fileLineContent =Helper.readFileInLines(FundfilePath, i);
			String[] element = fileLineContent.split("~",16);
			String str = element[columnNum];
			list.add(str);
		}
		return list;
	}
	
	
	public static void test() throws Exception{	
	}

//将指定列数据Load到一个List中(增加规则)
		private static String buildString(int columnNum) throws Exception{
			StringBuilder sb = new StringBuilder();
			int lineNum = Helper.getTotalLinesOfFile(FundfilePath);
			for(int i = 2;i < lineNum; i++){
				String fileLineContent =Helper.readFileInLines(FundfilePath, i);
				String[] element = fileLineContent.split("~",16);
				String str = element[columnNum];
				sb.append("'"+str+"',");
				if(i == lineNum-1){
					sb.append("'"+str+"'");
				}				
			}
			return sb.toString();
		}

	
	public static void main(String[] args) throws Exception{
		testPrepare();
		long startTime1 = System.currentTimeMillis();
		testFundFileCompletement();
		long endTime1 = System.currentTimeMillis()-startTime1;
		System.out.println("FundFileCompletement has finished! Total cost: " + endTime1 +" ms");
		long startTime2 = System.currentTimeMillis();
		testFundFileFormat();
		long endTime2 = System.currentTimeMillis()-startTime2;
		System.out.println("FundFileFormat has finished! Total cost: " + endTime2 +" ms");
		long startTime3 = System.currentTimeMillis();
		testFundFileContent();
		long endTime3 = System.currentTimeMillis()-startTime3;
		System.out.println("FundFileContent has finished! Total cost: " + endTime3 +" ms");
		long startTime4 = System.currentTimeMillis();
		testFundFileDuplicateData();
		long endTime4 = System.currentTimeMillis()-startTime4;
		System.out.println("FundFileDuplicateData has finished! Total cost: " + endTime4 +" ms");
	}
}
