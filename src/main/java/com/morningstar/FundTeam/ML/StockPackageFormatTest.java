package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class StockPackageFormatTest {
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/StockPackage/";
	private static String testLogNameCase = "formatTestResult-" + currentTime + "." + "log";
	
	private static String stockFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/StockDemoFile/StagingENV/20140830/PLP320XC.MSSTOCKS";
	
	private static void testStockFileFormat(){
		int lineNum = 0;
		String[] element = null;
		String fileCUSIP = "";
		String fileISIN = "";
		String fileSEDOL = "";
		String fileTickerSymbol = "";
		String fileExchangeId = "";
		String fileDomicile = "";
		String filePrimaryExchangeId = "";
		String fileSecId = "";
		String fileYieldAsAtDate = "";
		String fileSecurityYieldRate = "";
		String fileBetaRate = "";
		String fileBetaAsAtDate = "";
		String fileBetaBackfillIndexCode = "";
		String fileMorningstarBetaProxyName = "";
		boolean isDecimal = true;
		int decimalScale;
		List<String> fileLineList = new ArrayList<String>();
		
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataFormat]Begin to test the Stock File's Data format in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Stock File's Data format in each lines,start at:" + startTime);
//Get file data stream from Stock package to a list
				try {
					Long readfileStartTime = System.currentTimeMillis(); 
					fileLineList = Helper.readFileList(stockFilePath);
					long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
					System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
				} catch (IOException e) {
					e.printStackTrace();
				}
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				if(!lineContentStr.isEmpty() || lineContentStr !=null){
					element = lineContentStr.split("~", 16);
					fileCUSIP = element[0];
					fileSEDOL = element[1];
					fileISIN = element[2];
					fileExchangeId = element[3];
					fileTickerSymbol = element[4];
					fileDomicile = element[5];
					filePrimaryExchangeId = element[6];
					fileSecId = element[7];
					fileYieldAsAtDate = element[8];
					fileSecurityYieldRate = element[9];
					fileBetaAsAtDate = element[10];
					fileBetaRate = element[11];
					fileBetaBackfillIndexCode = element[14];
					fileMorningstarBetaProxyName = element[15];
		//测试CUSIP
					if(fileCUSIP != null && !fileCUSIP.isEmpty()){
						if(fileCUSIP.length() != 0 && fileCUSIP.length() != 9 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of CUSIP in Stock file isn't 9 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of CUSIP in Stock file is:" + fileCUSIP.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");				
						}
					}
					
        //测试SEDOL
					if(fileSEDOL != null && !fileSEDOL.isEmpty()){
						if(fileSEDOL.length() != 0 && fileSEDOL.length() != 7 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of SEDOL in stock file isn't 7 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of SEDOL in Stock file is:" + fileSEDOL.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		//测试ISIN
					if(fileISIN != null && !fileISIN.isEmpty()){
						if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of ISIN in Stock file isn't 12 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Stock file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
        //测试ExchangeId
					if(fileExchangeId != null && !fileExchangeId.isEmpty()){
						if(fileExchangeId.length() != 0 && fileExchangeId.length() !=10 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of ExchangeId in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ExchangeId in Stock file is:" + fileExchangeId.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		
        //测试Ticker Symbol
					if(fileTickerSymbol != null && !fileTickerSymbol.isEmpty()){
						if(fileTickerSymbol.length() != 0 && fileTickerSymbol.length() > 20 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Primary Ticker Symbol in stock file isn't 20 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Symbol in Stock file is:" + fileTickerSymbol.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
        //测试Domicile
					if(fileDomicile != null && !fileDomicile.isEmpty()){
						if(fileDomicile.length() != 0 && fileDomicile.length() != 10 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Domicile in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Domicile in Stock file is:" + fileDomicile.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
        //测试PrimaryExchangeId
					if(filePrimaryExchangeId != null && !filePrimaryExchangeId.isEmpty()){
						if(filePrimaryExchangeId.length() != 0 && filePrimaryExchangeId.length() != 10 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Primary Exchange Id in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Primary ExchangeId in Stock file is:" + filePrimaryExchangeId.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试SecId
					if(fileSecId != null){
						if(fileSecId.length() != 0 && fileSecId.length() != 10){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of SecId in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Stock file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}				
					}
					
		//测试YieldRate，精度为(9,5)
					if(fileSecurityYieldRate != null && !fileSecurityYieldRate.isEmpty()){
						isDecimal = Helper.isDecimal(fileSecurityYieldRate);
						decimalScale = Helper.getDecimalScale(fileSecurityYieldRate);
						if(fileSecurityYieldRate.length() != 0 && isDecimal && decimalScale != 5){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The Decimal size of YieldRate in stock file isn't 5 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileSecurityYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of YieldRate in Stock file is:" + Helper.getDecimalScale(fileSecurityYieldRate));
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试YieldAsAtDate
					if(fileYieldAsAtDate != null && !fileYieldAsAtDate.isEmpty()){
						if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of YieldAsAtDate in stock file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of YieldAsAtDate in Stock file is:" + fileYieldAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试BetaAsAtDate
					if(fileBetaAsAtDate != null && !fileBetaAsAtDate.isEmpty()){
						if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of BetaAsAtDate in stock file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of BetaAsAtDate in Stock file is:" + fileBetaAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试BetaRate，精度为(9,3)
					if(fileBetaRate != null && !fileBetaRate.isEmpty()){
						isDecimal = Helper.isDecimal(fileBetaRate);
						decimalScale = Helper.getDecimalScale(fileBetaRate);
						if(fileBetaRate.length() != 0 && isDecimal && decimalScale != 3){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of BetaRate in Stock file isn't 3 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileBetaRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of BetaRate in Stock file is:" + decimalScale);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
       //测试BetaBackfillIndexCode
					if(fileBetaBackfillIndexCode != null && !fileBetaBackfillIndexCode.isEmpty()){
						if(fileBetaBackfillIndexCode.length() != 0 && fileBetaBackfillIndexCode.length() != 10 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Beta Backfill Index Code in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Beta Backfill Index Code in Stock file is:" + fileBetaBackfillIndexCode.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
       //测试MorningstarBetaProxyName
					if(fileMorningstarBetaProxyName != null && !fileMorningstarBetaProxyName.isEmpty()){
						if(fileMorningstarBetaProxyName.length() != 0 && fileMorningstarBetaProxyName.length() > 40 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Morningstar Beta Proxy Name in stock file isn't 40 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Morningstar Beta Proxy Name in Stock file is:" + fileMorningstarBetaProxyName.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
				}
			}
		}
				
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testStockFileFormat();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Format test for Stock has finished, total cost: " + endTestTime + " ms");
	}
}
