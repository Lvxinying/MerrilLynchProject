package FundOperationETL;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.ibm.icu.text.SimpleDateFormat;
import com.morningstar.FundAutoTest.XmlHelperNew;
import com.morningstar.FundAutoTest.commons.*;
import com.morningstar.FundAutoTest.commons.testbase.Base;

public class FundOperationETLTest extends Base{

	/**@author Stefan.Hou
	 */
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	public static String currenTime = df.format(new Date());
	
	public static String testLogPath = "./log/TestLog/FundOperationETL/";
	public static String testLogNameCase1 = "DBdataTypeTestResult-" + currenTime + ".log";
	public static String testLogNameCase2 = "DbdataSizeTestResult-" + currenTime + ".log";
	public static String testLogNameCase3 = "DbdataContentTestResult-" + currenTime + ".log";
	public static String testLogTopic1 = "Fund Operation ETL testing for DB Data Type mapping test between Vertica and MS-SQL";
	public static String testLogTopic2 = "Fund Operation ETL testing for DB Data size test between Vertica and MS-SQL";
	public static String testLogTopic3 = "Fund Operation ETL testing for DB Data Content test between Vertica and MS-SQL";
	
@BeforeClass(description = "Testing preparing!")
	public static void testPrepare() throws IOException{
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase1, testLogTopic1);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase2, testLogTopic2);
		CustomizedLog.creatCusomizedLogFile(testLogPath, testLogNameCase3, testLogTopic3);		
	}
	
@BeforeClass(description = "Read MS-SQL Database Table name!")
	private static List<String> getMsDBTablename() throws Exception
	{   
		List<String> list = new ArrayList<String>();
		list = XmlHelperNew.parserXml("./config/FundFixIncome/MsTableList.xml");
		return list;			
	}

@BeforeClass(description = "Read Vertica Database Table name!")
	private static List<String> getVerticaDBTablename() throws Exception
	{   
		List<String> list = new ArrayList<String>(); 
		list = XmlHelperNew.parserXml("./config/FundFixIncome/VerticaTableList.xml");
		return list;			
	}

@Test(description = "Testing for DataBase Compared DataType!")  	
	public static void testDbDataType() throws Exception{
		String startTime = currentSysTime();
		boolean testResult = true;
		List<String> VerticaDbTableList = getVerticaDBTablename();
		List<String> MsDbTableList = getMsDBTablename();
		
		if(VerticaDbTableList.size() != MsDbTableList.size()){
			System.out.println("Vertica and MS-SQL table counts isn't same,please check!");
		}
		
		if(VerticaDbTableList.size() == MsDbTableList.size()){
			for(int i = 0;i < VerticaDbTableList.size();i++){
				System.out.println("=====================================================================");
	    		System.out.println("Start at: " + startTime);
				System.out.println("Begin to test DB dataType,the table name in Vertica is:" + " " + VerticaDbTableList.get(i));
	    		System.out.println("Begin to test DB dataType,the table name in MsSQL is:" + " " + MsDbTableList.get(i));
	    		
	    		String sql_vertica = "SELECT * FROM" + " " + VerticaDbTableList.get(i);
				String sql_mssql = "SELECT * FROM" + " " + "CurrentData." + MsDbTableList.get(i);
			
				int columncount_vertica = DBCommons.getColumnCount(Database.Vertica3, sql_vertica);
				System.out.println("Vertica Table name is: " + VerticaDbTableList.get(i) + " Vertica total colunm number is: " + columncount_vertica);
				int columncount_sqlserver = DBCommons.getColumnCount(Database.MsSQL1,sql_mssql);
				System.out.println("SQL-SERVER Table name is: " + MsDbTableList.get(i) + " SQL-SERVER total colunm number is: " + columncount_sqlserver);
				if(columncount_vertica != columncount_sqlserver){
					System.out.println("[WARNING]Data column isn't same between Vertica and MS-SQL!");					
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Mismatch counts of columns between Vertica And MS-SQL!");
				}
				else{
					System.out.println("Testing begins... ...");
					for(int j = 1;j <= columncount_vertica;j++){						
						String vType = DBCommons.getColumnDataTypeName(Database.Vertica3,sql_vertica, j);
						String msType = DBCommons.getColumnDataTypeName(Database.MsSQL1,sql_mssql, j);
						String vDataColumnName = DBCommons.getColumnName(Database.Vertica3, sql_vertica, j);
						String msSqlDataColumnName = DBCommons.getColumnName(Database.MsSQL1, sql_mssql, j);
						Map<String, String> map = new HashMap<String, String>();    	   
				    	map.put(vType, msType);
				    	String actualMsType = map.get(vType);
				    	if(vType == "CHAR"){
				    		if(actualMsType.equalsIgnoreCase("bit") == false
				    		||actualMsType.equalsIgnoreCase("char") == false
				    		||actualMsType.equalsIgnoreCase("nchar") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is CHAR while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'CHAR' mapping MS-SQL 'bit''char''nchar' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "BIGINT"){
				    		if(actualMsType.equalsIgnoreCase("datetime") == false
				    		||actualMsType.equalsIgnoreCase("smalldatetime") == false
				    		||actualMsType.equalsIgnoreCase("bigint") == false
				    		||actualMsType.equalsIgnoreCase("int") == false
				    		||actualMsType.equalsIgnoreCase("tinyint") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is BIGINT while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'BIGINT' mapping MS-SQL 'datetime''smalldatetime''bigint''int''tinyint' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "BIGINT" && vDataColumnName == "LastUpdateDate"){
				    		if(actualMsType.equalsIgnoreCase("timestamp") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is BIGINT and Column Name is LastUpdateDate while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'BIGINT' && vDataColumnName='LastUpdateDate' mapping MS-SQL 'timestamp' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "TIMESTAMP"){
				    		if(actualMsType.equalsIgnoreCase("smalldatetime") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is TIMESTAMP while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'TIMESTAMP' mapping MS-SQL 'smalldatetime' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "VARCHAR"){
				    		if(actualMsType.equalsIgnoreCase("xml") == false
				    		  ||actualMsType.equalsIgnoreCase("ntext")
				    		  ||actualMsType.equalsIgnoreCase("varchar")
				    		  ||actualMsType.equalsIgnoreCase("nvarchar")){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is VARCHAR while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'VARCHAR' mapping MS-SQL 'xml' 'ntext' 'varchar' 'nvarchar' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "VARBINARY(65000)"){
				    		if(actualMsType.equalsIgnoreCase("image") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is VARBINARY(65000) while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'VARBINARY(65000)' mapping MS-SQL 'image' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "INTEGER"){
				    		if(actualMsType.equalsIgnoreCase("int") == false
				    		  ||actualMsType.equalsIgnoreCase("smallint")
						      ||actualMsType.equalsIgnoreCase("bigint")
						      ||actualMsType.equalsIgnoreCase("tinyint")){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is INTEGER while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'INTEGER' mapping MS-SQL 'int' 'smallint' 'bigint' 'tinyint' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "REAL"){
				    		if(actualMsType.equalsIgnoreCase("bit") == false
				    		  ||actualMsType.equalsIgnoreCase("smallint")
						      ||actualMsType.equalsIgnoreCase("bigint")
						      ||actualMsType.equalsIgnoreCase("tinyint")){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is INTEGER while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'REAL' mapping MS-SQL 'bit' 'smallint' 'bigint' 'tinyint' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "NUMERIC"){
				    		if(actualMsType.equalsIgnoreCase("numeric") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is NUMERIC while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'NUMERIC' mapping MS-SQL 'numeric' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "DECIMAL"){
				    		if(actualMsType.equalsIgnoreCase("decimal") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is DECIMAL while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'DECIMAL' mapping MS-SQL 'decimal' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "BINARY"){
				    		if(actualMsType.equalsIgnoreCase("binary") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is BINARY while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'BINARY' mapping MS-SQL 'binary' is failed!");
				    		}
				    	}
				    	
				    	if(vType == "VARBINARY"){
				    		if(actualMsType.equalsIgnoreCase("varbinary") == false){
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "[TestFailed]Vertica Table name: "+ VerticaDbTableList.get(i) + "\tVertica Column Name is: " + vDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "MS-SQL Table name: "+ MsDbTableList.get(i) + "\tMS-SQL Column Name is: " + msSqlDataColumnName);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "Vertica Data Type is VARBINARY while actural MS-SQL Data Type is: " + actualMsType);
				    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase1, "=============================================================================");				    			
				    			testResult = false;
				    			Assert.assertTrue(testResult, "vType = 'VARBINARY' mapping MS-SQL 'varbinary' is failed!");
				    		}
				    	}				    	
					}
				}
				String endTime = currentSysTime();
				System.out.println("End at: " + endTime);
			}
		}
	}

	@Test(description = "Testing for DataBase compared data size!")
	public static void testDbDataSize() throws Exception{
		String startTime = currentSysTime();
		boolean testResult = true;
		List<String> VerticaDbTableList = getVerticaDBTablename();
		List<String> MsDbTableList = getMsDBTablename();
		if(VerticaDbTableList.size() != MsDbTableList.size()){
			System.out.println("Vertica and MS-SQL table counts isn't same,please check!");
		}
		
		if(VerticaDbTableList.size() == MsDbTableList.size()){
			System.out.println("Testing begins... ...");
			for(int i = 0;i< VerticaDbTableList.size();i++){
				System.out.println("=====================================================================");
				System.out.println("Start at: " + startTime);
	    		System.out.println("Begin to test DB datasize,the table name in Vertica is:" + " " + VerticaDbTableList.get(i));
	    		System.out.println("Begin to test DB datasize,the table name in MsSQL is:" + " " + MsDbTableList.get(i));
	    		
	    		String sql_vertica = "SELECT * FROM" + " " + VerticaDbTableList.get(i);
				String sql_mssql = "SELECT * FROM" + " " + "CurrentData." + MsDbTableList.get(i);
				
				int columncount_vertica = DBCommons.getColumnCount(Database.Vertica1, sql_vertica);
				System.out.println("Vertica Table name is: " + VerticaDbTableList.get(i) + " Vertica total colunm number is: " + columncount_vertica);
				int columncount_sqlserver = DBCommons.getColumnCount(Database.MsSQL1,sql_mssql);
				System.out.println("SQL-SERVER Table name is: " + MsDbTableList.get(i) + " SQL-SERVER total colunm number is: " + columncount_sqlserver);
				
				if(columncount_vertica != columncount_sqlserver){
					System.out.println("Mismatch counts of columns between Vertica And MS-SQL!");
					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestFailed]Mismatch counts of columns between Vertica And MS-SQL!");
				}
				if(columncount_vertica == columncount_sqlserver){
					for(int j=1;j<=columncount_vertica;j++){
					String columnName_vertica = DBCommons.getColumnName(Database.Vertica1, sql_vertica, j);
					String columnName_mssql = DBCommons.getColumnName(Database.MsSQL1, sql_mssql, j);
					int verticaDataSize = DBCommons.getColumnDataSize(Database.Vertica1, sql_vertica, j);
					int sqlserverDataSize = DBCommons.getColumnDataSize(Database.MsSQL1,sql_mssql , j);
						if(verticaDataSize != sqlserverDataSize){
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "[TestFailed]The column data size isn't same between Vertica and MS-SQL!");
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "Vertica Column Name is: " + columnName_vertica + "\tVertica Data Size is: " + verticaDataSize +"\tMS-SQL Data Size is: " + sqlserverDataSize +"\tMS-SQL Column Name is: " + columnName_mssql);
							CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase2, "=============================================================================");
							testResult = false;
							Assert.assertTrue(testResult, "Data Size testing is failed!");
						}
					}
				}
				
			}
		}
		String endTime =currentSysTime();
		System.out.println("End at: " + endTime);
	}


	@Test(description = "Testing for DataBase compared data contengt!")
	public static void testDbDataContent() throws Exception{
		String startTime = currentSysTime();
		boolean testResuilt = true;
		List<String> VerticaDbList = getVerticaDBTablename();
		List<String> MsDbList = getMsDBTablename();
		for(int i = 0;i< VerticaDbList.size();i++){
			System.out.println("=====================================================================");
			System.out.println("Start at: " + startTime);
			System.out.println("Begin to test DB dataSize,the table name in Vertica is:" + " " + VerticaDbList.get(i));
    		String sql_vertica = "SELECT * FROM" + " " + VerticaDbList.get(i);
			String sql_mssql = "SELECT * FROM" + " " + "CurrentData." + MsDbList.get(i);
    		List<String> verticaContentList = DBCommons.getDataList(sql_vertica, Database.Vertica1);
    		List<String> mssqlContentList = DBCommons.getDataList(sql_mssql, Database.Vertica1);
    		if(verticaContentList.size() != mssqlContentList.size()){
    			CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestFailed]The records count isn't same between Vertica and MS-SQL!");
    			testResuilt = false;
    			Assert.assertTrue(testResuilt, "DB records count isn't same!");
    		}
    		if(verticaContentList.size() == mssqlContentList.size()){
    			for(int j = 0;j <= verticaContentList.size();j++){
    				String verticaContent = verticaContentList.get(j);
    				String mssqlContent = mssqlContentList.get(j);
    				if(verticaContent.equals(mssqlContent) == false){
    					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "[TestFailed]The content isn't same between Vertica and MS-SQL!");
    					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "Vertica content is: " + verticaContent + "\t line No." + j);
    					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "MS-SQL content is: " + mssqlContent + "\t line No." + j);
    					CustomizedLog.writeCustomizedLogFile(testLogPath + testLogNameCase3, "=============================================================================");
    					testResuilt = false;
    					Assert.assertTrue(testResuilt, "DB content test is failed!");
    				}
    			}
    		}
		}
		String endTime = currentSysTime();
		System.out.println("End at: " + endTime);
	}

	public static void main(String[] args) {

	}

}
