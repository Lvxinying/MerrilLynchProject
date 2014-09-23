package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class FundPackageCompleteTest {
	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/FundPackage/";
	private static String testLogNameCase = "completenessTestResult-" + currentTime + "." + "log";
	
	private static String fundFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/FundDemoFile/StagingENV/20140830/PLP320XA.MSGBLFND";
	
	private static void testFundFileComplete(){
		int lineNum = 0;
		int separatorCount = 0;
		String[] element = null;
		String fileInvestmentId = "";
		String fileYieldRate = "";
		String fileBetaRate = "";
		List<String> fileLineList = new ArrayList<String>();
		
		String startTime1 = Base.currentSysTime();
		System.out.println("[TestForCompletment]Fund package complete testing begins,please wait.......");
		System.out.println("[TestForCompletment]Test at:" + startTime1);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[TestForCompletment]Begin to test no CUSIP and ISIN in each lines,start at:" + startTime1);
//Get file data stream from Fund package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(fundFilePath);
			long readfileEndTime = System.currentTimeMillis()-readfileStartTime;
			System.out.println("[INFO]File read finished,total cost: "+readfileEndTime+" ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
//QA test
		for(String lineContentStr:fileLineList){
			lineNum++;
			if(lineContentStr.contains("UHDR ")){
				continue;
			}else if(lineContentStr.contains("UTRL ")){
				continue;
			}else{
				if(!lineContentStr.isEmpty() || lineContentStr !=null){
					element = lineContentStr.split("~", 16);
					fileInvestmentId = element[7];
					fileYieldRate = element[9];
					fileBetaRate = element[11];					
//No InvestmentId
					if(fileInvestmentId.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[FAILED]Line number is:" + lineNum + "   " + "No InvestmentId in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "InvestmentId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "========================================================");
					}
					
//Test for NO YieldRate && NO BetaRate
					if(fileYieldRate.isEmpty() && fileBetaRate.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[FAILED]Line number is:" + lineNum + "   " + "No YieldRate and BetaRate in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "InvestmentId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Beta Rate is: "+fileBetaRate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Yield Rate is: "+fileYieldRate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "========================================================");
					}

//Test no more or less 15 "~" symbol
					separatorCount = Helper.getMatchCount("~",lineContentStr);
					if(separatorCount != 15){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "[FAILED]" + "Line number is:" + lineNum + "   " + "There has no 16 '~' in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "InvestmentId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "Actural count of separator is: "+separatorCount);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase, "========================================================");
					}					
				}				
			}
		}
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testFundFileComplete();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Complete test for Fund has finished, total cost: " + endTestTime + " ms");
	}
}
