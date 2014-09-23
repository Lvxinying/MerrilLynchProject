package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageFormatTest {
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	private static String testLogNameCase = "formatTestResult-" + currentTime + "." + "log";
	
	private static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/StagingENV/20140830/PLP320XD.MSTBONDS";
	
	private static void testBondFileFormat(){
		int lineNum = 0;
		String[] element = null;
		String fileCUSIP = "";
		String fileISIN = "";
		String fileSecId = "";
		String fileYieldAsAtDate = "";
		String fileSecurityYieldRate = "";
		String fileBetaRate = "";
		String fileBetaAsAtDate = "";
		List<String> fileLineList = new ArrayList<String>();
		
		String startTime = Base.currentSysTime();
		System.out.println("[TestForDataFormat]Begin to test the Bond File's Data format in each lines,please wait.......");
		System.out.println("[TestForDataFormat]Test at:" + startTime);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForDataFormat]Begin to test the Bond File's Data format in each lines,start at:" + startTime);
//Get file data stream from Bond package to a list
				try {
					Long readfileStartTime = System.currentTimeMillis(); 
					fileLineList = Helper.readFileList(bondFilePath);
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
					fileISIN = element[2];
					fileSecId = element[7];
					fileYieldAsAtDate = element[8];
					fileSecurityYieldRate = element[9];
					fileBetaAsAtDate = element[10];
					fileBetaRate = element[11];
					//测试CUSIP
					if(fileCUSIP != null){
						if(fileCUSIP.length() != 0 && fileCUSIP.length() != 9 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of CUSIP in stock file isn't 9 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of CUSIP in Stock file is:" + fileCUSIP.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");				
						}
					}
		//测试ISIN
					if(fileISIN != null){
						if(fileISIN.length() != 0 && fileISIN.length() != 12 ){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of ISIN in stock file isn't 12 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Stock file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		//测试SecId
					if(fileSecId != null){
						if(fileSecId.length() != 0 && fileSecId.length() != 10){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of SecId in stock file isn't 10 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of ISIN in Stock file is:" + fileISIN.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}				
					}
		//测试YieldRate，精度为(9,5)
					if(fileSecurityYieldRate != null){
						if(fileSecurityYieldRate.length() != 0 && Helper.isDecimal(fileSecurityYieldRate) == true && Helper.getDecimalScale(fileSecurityYieldRate) != 5){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of YieldRate in stock file isn't 5 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileSecurityYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of YieldRate in Stock file is:" + Helper.getDecimalScale(fileSecurityYieldRate));
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		//测试YieldAsAtDate
					if(fileYieldAsAtDate != null){
						if(fileYieldAsAtDate.length() != 0 && fileYieldAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of YieldAsAtDate in stock file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of YieldAsAtDate in Stock file is:" + fileYieldAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		//测试BetaAsAtDate
					if(fileBetaAsAtDate != null){
						if(fileBetaAsAtDate.length() != 0 && fileBetaAsAtDate.length() != 8){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The size of BetaAsAtDate in stock file isn't 8 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual size of BetaAsAtDate in Stock file is:" + fileYieldAsAtDate.length());
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
		//测试BetaRate，精度为(9,3)
					if(fileBetaRate != null){
						if(fileBetaRate.length() != 0 && Helper.isDecimal(fileBetaRate) == true && Helper.getDecimalScale(fileBetaRate) != 3){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[Testing failed]" + "Line number is:" + lineNum + "   " + "The Decimal size of BetaRate in stock file isn't 3 bytes!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original Content In Stock file is:" + lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural Yield Rate is: " + fileBetaRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"Actual Decimal size of BetaRate in Stock file is:" + Helper.getDecimalScale(fileBetaRate));
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase,"===========================================================");
						}
					}
				}
			}
		}
				
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testBondFileFormat();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Format test for Bond has finished, total cost: " + endTestTime + " ms");
	}
}
