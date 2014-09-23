package MarketPriceBackFeedToTSDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBFreshpool;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.BaseObject;
import com.morningstar.data.tsapi.TSException;
import com.morningstar.data.tsapi.tscontext;
import com.morningstar.data.tsapi.blobData.BlobDF;

public class MarketHistoricalReturnBaseObjectUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_MarketHistoricalReturn(BaseObject).log";
	private static tscontext context_r = null;
	private static  BaseObject ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDF> seriesBlobDF = null;
		
	private static Date dFirstDate = null;
	private static String sFirstDate = null;
	private static float floatValue;
	private static String floatValueStr = null;
	
	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueInMonthlyMarketReturn = "";
	static String sqlToGetValueInQuarterlyMarketReturn = "";
	static String sqlToGetValueInAnnualMarketReturn = "";
	
	static{
		iniTsReader();
	}
	
	private static void connectToDb(Database database) throws SQLException{
				con = DBFreshpool.getConnection(database);
	}
	
	private static void recoverDb() throws SQLException{			
		if( !con.isClosed() || con != null){
			con.close();
		}
	}
	
	private static void iniTsReader(){
		try {
			context_r = new tscontext(tsConfigURL);
		} catch (TSException e) {
			System.out.println("TSDB Reader initialization error: " + e.getErrorInfo());
		}
		ts_r = new BaseObject(context_r);					
	}
	
	private static void getDataFromTSDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,String perfId) throws TSException, Exception{
		switch (dataTypeName){
		case tsMonthlyMarketReturn:
			try {
				seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
				}
				if(seriesBlobDF == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsQuarterlyMarketReturn:
			try {
				seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
				}
				if(seriesBlobDF == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsAnnualMarketReturn:
			try {
				seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDF = ts_r.LoadSeries(BlobDF.class, tsType, perfId);
				}
				if(seriesBlobDF == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;		
		default:
			break;
		}
	}
			
//		环境恢复(必要步骤)
		private static void tsContextRecover(){
			if(context_r != null){
				try {
					context_r.dispose();
				} catch (TSException e) {
					System.out.println("Current TSDB context doesn't been closed! error :" +e.getErrorInfo());
				}
			}
		}

//返回Map<String,Map<String,List<String>>>
//外侧Map的Key为PerformanceId Value为一个Map
//内侧Map的Key为日期 Value为其他数据的List集合		
	public static Map<String,Map<String,List<String>>> getFullValueFromNetezza(TsBlobDataTypeBaseObject tsType,Database database,List<String> perfIdList,String tableName) throws SQLException{
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
		
//Connect to DB	
		if(con ==  null || con.isClosed()){			
			connectToDb(database);
		}

		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(tsType){
			case tsMonthlyMarketReturn:
				sqlToGetValueInMonthlyMarketReturn = "SELECT EndDate,MonthlyReturns FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EndDate IS NOT NULL AND MonthlyReturns IS NOT NULL ORDER BY EndDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInMonthlyMarketReturn);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						valueList.add(rs.getString(2));
						dateValueMap.put("NullValue", valueList);
					}else{
						valueList.add(rs.getString(2));
						dateValueMap.put(rs.getString(1), valueList);
					}				
				}				
				resultMap.put(perfIdStr, dateValueMap);
				break;
				
			case tsQuarterlyMarketReturn:
				sqlToGetValueInMonthlyMarketReturn = "SELECT EndDate,QuarterlyReturns FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EndDate IS NOT NULL AND QuarterlyReturns IS NOT NULL ORDER BY EndDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInMonthlyMarketReturn);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						valueList.add(rs.getString(2));
						dateValueMap.put("NullValue", valueList);
					}else{
						valueList.add(rs.getString(2));
						dateValueMap.put(rs.getString(1), valueList);
					}				
				}				
				resultMap.put(perfIdStr, dateValueMap);
			break;
			
			case tsAnnualMarketReturn:
				sqlToGetValueInAnnualMarketReturn = "SELECT EndDate,AnnualReturns FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EndDate IS NOT NULL AND AnnualReturns IS NOT NULL ORDER BY EndDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInAnnualMarketReturn);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						valueList.add(rs.getString(2));
						dateValueMap.put("NullValue", valueList);
					}else{
						valueList.add(rs.getString(2));
						dateValueMap.put(rs.getString(1), valueList);
					}				
				}				
				resultMap.put(perfIdStr, dateValueMap);
			break;
			
				default:
					break;
			}//End Switch
		}
//recover DB
		recoverDb();
		return resultMap;
	}
	
//返回Map<String,Map<String,List<String>>>
//外侧Map的Key为PerformanceId Value为一个Map
//内侧Map的Key为日期 Value为其他数据的List集合	
	public static Map<String,Map<String,List<String>>> getFullValueFromTsDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,List<String> perfIdList) {
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
				
		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(dataTypeName) {
			case tsMonthlyMarketReturn:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDF data :seriesBlobDF){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						floatValue = data.getFloatValue();
						floatValueStr = Float.toString(floatValue);
						if(floatValueStr != null){
							valueList.add(String.valueOf(floatValueStr));
						}else{
							valueList.add("NullValue");
						}	
						dateValueMap.put(sFirstDate, valueList);
					}
				} catch (TSException e) {
					System.out.println("[ERROR]Getting data from TSDB error,message: " + e.getErrorInfo());
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
				} catch (Exception e) {
					e.printStackTrace();
				}
				resultMap.put(perfIdStr, dateValueMap);				
				break;
			
			case tsQuarterlyMarketReturn:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDF data :seriesBlobDF){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						floatValue = data.getFloatValue();
						floatValueStr = Float.toString(floatValue);
						if(floatValueStr != null){
							valueList.add(String.valueOf(floatValueStr));
						}else{
							valueList.add("NullValue");
						}	
						dateValueMap.put(sFirstDate, valueList);
					}
				} catch (TSException e) {
					System.out.println("[ERROR]Getting data from TSDB error,message: " + e.getErrorInfo());
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
				} catch (Exception e) {
					e.printStackTrace();
				}
				resultMap.put(perfIdStr, dateValueMap);
				break;
				
			case tsAnnualMarketReturn:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDF data :seriesBlobDF){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						floatValue = data.getFloatValue();
						floatValueStr = Float.toString(floatValue);
						if(floatValueStr != null){
							valueList.add(String.valueOf(floatValueStr));
						}else{
							valueList.add("NullValue");
						}	
						dateValueMap.put(sFirstDate, valueList);
					}
				} catch (TSException e) {
					System.out.println("[ERROR]Getting data from TSDB error,message: " + e.getErrorInfo());
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
					CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
				} catch (Exception e) {
					e.printStackTrace();
				}
				resultMap.put(perfIdStr, dateValueMap);
				break;				
				default:
					break;
			}
		}
//dispose connection to TSDB		
		tsContextRecover();
		return resultMap;
	}
}