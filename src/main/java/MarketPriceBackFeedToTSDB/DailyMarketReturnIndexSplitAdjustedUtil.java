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
import com.morningstar.data.tsapi.CorporateActionAdjustment;
import com.morningstar.data.tsapi.TSException;
import com.morningstar.data.tsapi.tscontext;
import com.morningstar.data.tsapi.blobData.BlobDCj;
import com.morningstar.data.tsapi.blobData.BlobDL;
import com.morningstar.data.tsapi.blobData.BlobDLB;
import com.morningstar.data.tsapi.blobData.BlobDLLL;
import com.morningstar.data.tsapi.blobData.BlobDLLLL;
import com.morningstar.data.tsapi.blobData.BlobDLLLLL;

public class DailyMarketReturnIndexSplitAdjustedUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_DailyMarketReturnIndex(SplitAdjusted).log";
	private static tscontext context_r = null;
	private static CorporateActionAdjustment ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDLLLLL> seriesBlobDLLLLL = null;
	static List<BlobDLLLL> seriesBlobDLLLL = null;
	static List<BlobDCj> seriesBlobDCj = null;
	static List<BlobDL> seriesBlobDL = null;
	static List<BlobDLB> seriesBlobDLB = null;
	static List<BlobDLLL> seriesBlobDLLL = null;
	
	private static Date dFirstDate = null;
	private static String sFirstDate = null;
	private static Double doubleValue1 = null;
	private static Double doubleValue2 = null;
	private static Double doubleValue3 = null;
	private static Double doubleValue4 = null;
	private static Byte byteValue;
	private static String byteValueStr = null;
	
	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueMarketPrice = "";
	static String sqlToGetValueInSpecialCashDividend = "";
	static String sqlToGetValueInDailyMarketReturnIndex = "";
	static String sqlToGetValueInTradingVolume = "";
	static String sqlToGetValueInCashDividend = "";
	static String sqlToGetValueInMarketBidOfferMidPrice = "";
	
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
		ts_r = new CorporateActionAdjustment(context_r);					
	}
	
	private static void getDataFromTSDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,String perfId) throws TSException, Exception{
		switch (dataTypeName){
		case tsMarketPrice:
			try {
				seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);
				}
				if(seriesBlobDLLLL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		
		case tsSpecialCashDividend:
			try {
				seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
				}
				if(seriesBlobDL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsDailyMarketReturnIndex:
			try {
				seriesBlobDLB = ts_r.LoadSeries(BlobDLB.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLB = ts_r.LoadSeries(BlobDLB.class, tsType, perfId);
				}
				if(seriesBlobDLB == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsTradingVolume:
			try {
				seriesBlobDL =ts_r.LoadSeries(BlobDL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
				}
				if(seriesBlobDL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsCashDividend:
			try {
				seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
				}
				if(seriesBlobDL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;

		case tsMarketBidOfferMidPrice:
			try {
				seriesBlobDLLL = ts_r.LoadSeries(BlobDLLL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLLL = ts_r.LoadSeries(BlobDLLL.class, tsType, perfId);
				}
				if(seriesBlobDLLL == null){
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
			case tsMarketPrice:
				sqlToGetValueMarketPrice = "SELECT EffectiveDate,AdjustedOpenPrice,AdjustedHighPrice,AdjustedLowPrice,AdjustedClosePrice FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND AdjustedOpenPrice IS NOT NULL AND AdjustedHighPrice IS NOT NULL " +
						"AND AdjustedLowPrice IS NOT NULL AND AdjustedClosePrice IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueMarketPrice);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						valueList.add(rs.getString(2));
						valueList.add(rs.getString(3));
						valueList.add(rs.getString(4));
						valueList.add(rs.getString(5));
						dateValueMap.put("NullValue", valueList);
					}else{
						valueList.add(rs.getString(2));
						valueList.add(rs.getString(3));
						valueList.add(rs.getString(4));
						valueList.add(rs.getString(5));
						dateValueMap.put(rs.getString(1), valueList);
					}				
				}				
				resultMap.put(perfIdStr, dateValueMap);
				break;
			
			case tsSpecialCashDividend:
				sqlToGetValueInSpecialCashDividend = "SELECT EffectiveDate,AdjustedSpecialCashDividend FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND AdjustedSpecialCashDividend IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInSpecialCashDividend);
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
				
			case tsDailyMarketReturnIndex:
				sqlToGetValueInDailyMarketReturnIndex = "SELECT EffectiveDate,DailyMarketReturnIndex,DailyMarketReturnIndexFlag FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND DailyMarketReturnIndex IS NOT NULL AND TradeCount IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
			if(con == null || con.isClosed()){
				connectToDb(database);
			}
			pstmt = con.prepareStatement(sqlToGetValueInDailyMarketReturnIndex);
			rs = pstmt.executeQuery();
			while(rs.next()){
				List<String> valueList = new ArrayList<String>();
				if(rs.getString(1).isEmpty()){
					valueList.add(rs.getString(2));
					valueList.add(rs.getString(3));
					dateValueMap.put("NullValue", valueList);
				}else{
					valueList.add(rs.getString(2));
					valueList.add(rs.getString(3));
					dateValueMap.put(rs.getString(1), valueList);
				}				
			}
			resultMap.put(perfIdStr, dateValueMap);
			break;
			
			case tsTradingVolume:
				sqlToGetValueInTradingVolume = "SELECT EffectiveDate,AdjustedTradingVolume FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND AdjustedTradingVolume IS NOT NULL " +
						"ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
			if(con == null || con.isClosed()){
				connectToDb(database);
			}
			pstmt = con.prepareStatement(sqlToGetValueInTradingVolume);
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
			
			case tsCashDividend:
				sqlToGetValueInCashDividend = "SELECT EffectiveDate,AdjustedCashDividend FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND AdjustedCashDividend IS NOT NULL " +
								"ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInCashDividend);
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
				
			case tsMarketBidOfferMidPrice:
				sqlToGetValueInMarketBidOfferMidPrice = "SELECT EffectiveDate,AdjustedBidPrice,AdjustedAskPrice,AdjustedMiddlePrice FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND AdjustedBidPrice IS NOT NULL " +
								"AND AdjustedAskPrice IS NOT NULL AND AdjustedMiddlePrice IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInMarketBidOfferMidPrice);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						valueList.add(rs.getString(2));
						valueList.add(rs.getString(3));
						valueList.add(rs.getString(4));
						dateValueMap.put("NullValue", valueList);
					}else{
						valueList.add(rs.getString(2));
						valueList.add(rs.getString(3));
						valueList.add(rs.getString(4));
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
	public static Map<String,Map<String,List<String>>> getFullValueFromTsDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,List<String> perfIdList){
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
				
		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(dataTypeName) {
			case tsMarketPrice:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDLLLL data :seriesBlobDLLLL){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue1();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
						}else{
							valueList.add("NullValue");
						}
						doubleValue2 = data.getDoubleValue2();
						if(doubleValue2 != null){
							valueList.add(String.valueOf(doubleValue2));
						}else{
							valueList.add("NullValue");
						}
						doubleValue3 = data.getDoubleValue3();
						if(doubleValue3 != null){
							valueList.add(String.valueOf(doubleValue3));
						}else{
							valueList.add("NullValue");
						}
						doubleValue4 = data.getDoubleValue4();
						if(doubleValue4 != null){
							valueList.add(String.valueOf(doubleValue4));
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
			
			case tsSpecialCashDividend:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDL data :seriesBlobDL){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue1();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
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
				
			case tsDailyMarketReturnIndex:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDLB data :seriesBlobDLB){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
						}else{
							valueList.add("NullValue");
						}
						byteValue = data.getByteValue();
						byteValueStr = Byte.toString(byteValue);
						if(byteValueStr != null){
							valueList.add(String.valueOf(byteValueStr));
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
				
			case tsTradingVolume:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDL data :seriesBlobDL){					
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue1();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
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
				
			case tsCashDividend:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDL data : seriesBlobDL){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue1();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
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
				
			case tsMarketBidOfferMidPrice:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDLLL data : seriesBlobDLLL){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						doubleValue1 = data.getDoubleValue1();
						if(doubleValue1 != null){
							valueList.add(String.valueOf(doubleValue1));
						}else{
							valueList.add("NullValue");
						}
						doubleValue2 = data.getDoubleValue2();
						if(doubleValue2 != null){
							valueList.add(String.valueOf(doubleValue2));
						}else{
							valueList.add("NullValue");
						}
						doubleValue3 = data.getDoubleValue3();
						if(doubleValue3 != null){
							valueList.add(String.valueOf(doubleValue3));
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