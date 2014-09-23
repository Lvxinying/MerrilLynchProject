package FundOperationETL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.morningstar.FundAutoTest.XmlHelperNew;
import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class OE91 extends Base{

	/**
	 * @param args
	 */
	static String currentTime = Base.currentSysTime();
	
	public static String testLogPath = "./log/TestLog/FundOperationETL/OE-91/";
	public static String testLogNameCase1 = "OneTimeDumpCountTestResult-" + currentTime + ".log";
	public static String testLogNameCase2 = "DbContentBasicCheckingResult-" + currentTime + ".log";
	
	public static String testLogTopic1 = "OE-91 Data counts checking!";
	public static String testLogTopic2 = "OE-91 Data content Size checking!";
	
	static{
		try {
			testPrepare();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void testPrepare() throws IOException{
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase1, testLogTopic1);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase2, testLogTopic2);		
	}
	
	private static void testCountChecking() throws Exception{
		String startTime =Base.currentSysTime();
		System.out.println("Begin to test records count in each table for OE-91!Start at: "+ startTime);
		List<String> MsTableNameList = new ArrayList<String>();
		List<String> VerticaTableNameList = new ArrayList<String>();
		MsTableNameList = getMsDBTablename();
		VerticaTableNameList = getVerticaDBTablename();
		String sqlToGetMSRecordsCount ="SELECT COUNT(*) FROM ";
		String realSqlToGetMSRecordsCount = "";
		String sqlToGetVerticaRecordsCount = "SELECT COUNT(*) FROM ";
		String realSqlToGetVerticaRecordsCount = "";
		String MSRecordsCount = "";
		String VerticaRecordsCount = "";
		
		if(MsTableNameList.size() == VerticaTableNameList.size()){
			for(int tableNum =0;tableNum<VerticaTableNameList.size();tableNum++){
				realSqlToGetMSRecordsCount = sqlToGetMSRecordsCount + MsTableNameList.get(tableNum);
				realSqlToGetVerticaRecordsCount = sqlToGetVerticaRecordsCount + VerticaTableNameList.get(tableNum);
				MSRecordsCount = DBCommons.getData(realSqlToGetMSRecordsCount, Database.MsSQL4);
				VerticaRecordsCount = DBCommons.getData(realSqlToGetVerticaRecordsCount, Database.Vertica3);
				if(!MSRecordsCount.equals(VerticaRecordsCount)){
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase1, "[TestFailed]Records count not same!");
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase1, "MS-Table: "+MsTableNameList.get(tableNum)+"      Count = " + MSRecordsCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase1, "Vertica-Table: "+VerticaTableNameList.get(tableNum)+"      Count = " + VerticaRecordsCount);
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase1, "===================================================================");
				}				
			}
		}
		else{
			System.out.println("Table count isn't same,please check XML config file!");
		}
		
		String endTime = Base.currentSysTime();
		System.out.println("Test finished,end at: "+endTime);
	}
	
	private static void testDataContentBasic() throws Exception{
		String startTime =Base.currentSysTime();
		System.out.println("Begin to test data counts in each table for OE-91!Start at: "+ startTime);
		List<String> MsTableNameList = new ArrayList<String>();
		List<String> VerticaTableNameList = new ArrayList<String>();
		int MsDataListSize = 0;
		int VerticaDataListSize = 0;
		
		MsTableNameList = getMsDBTablename();
		VerticaTableNameList = getVerticaDBTablename();
		
		if(MsTableNameList.size() == VerticaTableNameList.size()){
			for(int tableNum =0;tableNum<VerticaTableNameList.size();tableNum++){
				MsDataListSize = getMSContentListSize(MsTableNameList.get(tableNum));
				VerticaDataListSize = getVerticaContentListSize(VerticaTableNameList.get(tableNum));
				if(MsDataListSize != VerticaDataListSize){
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase2, "[TestFailed]Data counts not same!");
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase2, "MS-Table: "+MsTableNameList.get(tableNum)+"      Data counts: " + MsDataListSize);
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase2, "Vertica-Table: "+VerticaTableNameList.get(tableNum)+"      Data counts: " + VerticaDataListSize);
					CustomizedLog.writeCustomizedLogFile(testLogPath+testLogNameCase2, "===================================================================");
				}
			}
		}
		else{
			System.out.println("Table count isn't same,please check XML config file!");
		}
		
		String endTime = Base.currentSysTime();
		System.out.println("Test finished,end at: "+endTime);
	}
	
	private static List<String> getMsDBTablename() throws Exception
	{   
		List<String> list = new ArrayList<String>();
		list = XmlHelperNew.parserXml("./config/OE-91/MsTableList.xml");
		return list;			
	}
	
	private static List<String> getVerticaDBTablename() throws Exception
	{   
		List<String> list = new ArrayList<String>(); 
		list = XmlHelperNew.parserXml("./config/OE-91/VerticaTableList.xml");
		return list;			
	}
	
	private static int getMSContentListSize(String sqlSplit) throws Exception{
		int listSize = 0;
		List<String> contentList = new ArrayList<String>();
		String sqlToGetMSContent = "SELECT * FROM " + sqlSplit;
		contentList = DBCommons.getDataList(sqlToGetMSContent, Database.MsSQL4);
		listSize = contentList.size();
		contentList.clear();
		return listSize;
	}
	
	private static int getVerticaContentListSize(String sqlSplit) throws Exception{
		int listSize = 0;
		List<String> contentList = new ArrayList<String>();
		String sqlToGetVerticaContent = "SELECT * FROM " + sqlSplit;
		contentList = DBCommons.getDataList(sqlToGetVerticaContent, Database.Vertica3);
		listSize = contentList.size();
		contentList.clear();
		return listSize;
	}
	
	public static void main(String[] args) throws Exception {
		testCountChecking();
		testDataContentBasic();
	}

}
