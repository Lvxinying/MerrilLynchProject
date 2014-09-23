package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class HybridFundPackageFormatTest {
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/HybridFundPackage/";
	private static String testLogNameCase = "formatTestResult-" + currentTime + "." + "log";
	
	private static String hybridFundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/HybridDemoFile/StagingENV/20140830/PLP320XB.MSHYBFND";
	
	private static void testFundFileFormat(){
		int lineNum = 0;
		String[] element = null;
		String fileISIN = "";
		String filePerformanceId = "";
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
		System.out.println("[TestForDataFormat]Begin to test the Hybrid Fund File's Data format in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Hybrid Fund File's Data format in each lines,start at:" + startTime);
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
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				if(!lineContentStr.isEmpty() || lineContentStr !=null){
					element = lineContentStr.split("~", 16);
					fileISIN = element[2];
					filePerformanceId = element[7];
					fileYieldAsAtDate = element[8];
					fileSecurityYieldRate = element[9];
					fileBetaAsAtDate = element[10];
					fileBetaRate = element[11];
					fileBetaBackfillIndexCode = element[14];
					
		//测试ISIN
					if(fileISIN != null && !fileISIN.isEmpty()){
						if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of ISIN in Hybrid Fund file isn't 12 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Fund file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试PerformanceId
					if(filePerformanceId != null){
						if(filePerformanceId.length() != 0 && filePerformanceId.length() != 10){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of PerformanceId in Hybrid Fund file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Stock file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}				
					}
					
		//测试YieldRate，精度为(9,5)
					if(fileSecurityYieldRate != null && !fileSecurityYieldRate.isEmpty()){
						isDecimal = Helper.isDecimal(fileSecurityYieldRate);
						decimalScale = Helper.getDecimalScale(fileSecurityYieldRate);
						if(fileSecurityYieldRate.length() != 0 && isDecimal && decimalScale != 5){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The Decimal size of YieldRate in fund file isn't 5 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileSecurityYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of YieldRate in Fund file is:" + Helper.getDecimalScale(fileSecurityYieldRate));
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试YieldAsAtDate
					if(fileYieldAsAtDate != null && !fileYieldAsAtDate.isEmpty()){
						if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of YieldAsAtDate in fund file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of YieldAsAtDate in Fund file is:" + fileYieldAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试BetaAsAtDate
					if(fileBetaAsAtDate != null && !fileBetaAsAtDate.isEmpty()){
						if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of BetaAsAtDate in fund file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of BetaAsAtDate in Fund file is:" + fileBetaAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
		//测试BetaRate，精度为(9,3)
					if(fileBetaRate != null && !fileBetaRate.isEmpty()){
						isDecimal = Helper.isDecimal(fileBetaRate);
						decimalScale = Helper.getDecimalScale(fileBetaRate);
						if(fileBetaRate.length() != 0 && isDecimal && decimalScale != 3){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of BetaRate in Fund file isn't 3 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Fund file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileBetaRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of BetaRate in Hybrid Fund file is:" + decimalScale);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
					
//测试ProxyId
					if(fileBetaBackfillIndexCode.length() != 0 && fileBetaBackfillIndexCode.length() != 10 ){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Beta Backfill Index Code in fund file isn't 10 bytes!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Beta Backfill Index Code in Hybrid fund file is:" + fileBetaBackfillIndexCode.length());
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
					}
//测试ProxyName
					if(fileMorningstarBetaProxyName.length() != 0 && fileMorningstarBetaProxyName.length() > 40 ){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]Line number is:" + lineNum + "   " + "The size of Morningstar Beta Proxy Name in fund file is over 40 bytes!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Hybrid Fund file is:" + lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of Morningstar Beta Proxy Name in Hybrid Fund file is:" + fileMorningstarBetaProxyName.length());
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
					}
				}
			}
		}
				
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testFundFileFormat();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Format test for Fund has finished, total cost: " + endTestTime + " ms");
	}
}
