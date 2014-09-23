package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class BondPackageCompleteTest {

	/**
	 * @param args
	 */
	
	private static String currentTime = Base.currentSysTime();
	private static String testLogPath = "./log/TestLog/MerrillLynch/BondPackage/";
	private static String testLogNameCase1 = "completenessTestResult-" + currentTime + "." + "log";
	
	private static String bondFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/BondDemoFile/StagingENV/20140830/PLP320XD.MSTBONDS";
	
	private static void testBondFileComplete(){
//统计Bond文件中有多少个即没有CUSIP也没有ISIN的行
		int lineNum = 0;
		int separatorCount = 0;
		String[] element = null;
		String fileInvestmentId = "";
		String fileCUSIP = "";
		String fileISIN = "";
		String fileYieldRate = "";
		String fileYieldAsAtDate = "";
		List<String> fileLineList = new ArrayList<String>();
		
		String startTime1 = Base.currentSysTime();
		System.out.println("[TestForCompletment]Bond package complete testing begins,please wait.......");
		System.out.println("[TestForCompletment]Test at:" + startTime1);
		CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestForCompletment]Begin to test no CUSIP and ISIN in each lines,start at:" + startTime1);
//Get file data stream from Bond package to a list
		try {
			Long readfileStartTime = System.currentTimeMillis(); 
			fileLineList = Helper.readFileList(bondFilePath);
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
					fileCUSIP = element[0];
					fileISIN = element[2];
					fileInvestmentId = element[7];
					fileYieldAsAtDate = element[8];
					fileYieldRate = element[9];
//Test for NO CUSIP && NO ISIN case				
					if(fileCUSIP.isEmpty()==true && fileISIN.isEmpty()==true){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No CUSIP and ISIN in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "InvestmentId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
					}
//Test for NO YieldRate && NO YieldAsAtDate
					if(!fileYieldAsAtDate.isEmpty()){
						if(fileYieldRate.isEmpty()){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldRate while YieldAsAtDate isn't empty in this line!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "PerformanceId is:" + fileInvestmentId);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
						}
					}
					if(fileYieldRate.isEmpty()){
						if(!fileYieldAsAtDate.isEmpty()){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldAsAtDate while YieldRate isn't empty in this line!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "PerformanceId is:" + fileInvestmentId);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+lineContentStr);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
						}
					}
					if(fileYieldRate.isEmpty() && fileYieldRate.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "No YieldAsAtDate and YieldRate in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "PerformanceId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield As At Date is: "+fileYieldAsAtDate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Yield Rate is: "+fileYieldRate);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
					}
//Test no more or less 15 "~" symbol
					separatorCount = Helper.getMatchCount("~",lineContentStr);
					if(separatorCount != 15){
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[FAILED]" + "Line number is:" + lineNum + "   " + "There has no 16 '~' in this line!");
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "PerformanceId is:" + fileInvestmentId);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Original content in file is: "+lineContentStr);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Actural count of separator is: "+separatorCount);
						CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "========================================================");
					}					
				}				
			}
		}
	}
	
	public static void main(String[] args) {
		Long startTestTime = System.currentTimeMillis();
		testBondFileComplete();
		Long endTestTime = System.currentTimeMillis()-startTestTime;
		System.out.println("[INFO]Complete test for Bond has finished, total cost: " + endTestTime + " ms");
	}

}