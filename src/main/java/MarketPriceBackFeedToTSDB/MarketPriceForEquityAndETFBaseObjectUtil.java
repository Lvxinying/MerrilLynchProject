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
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.DBFreshpool;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.BaseObject;
import com.morningstar.data.tsapi.TSException;
import com.morningstar.data.tsapi.tscontext;
import com.morningstar.data.tsapi.blobData.BaseBlob;
import com.morningstar.data.tsapi.blobData.BlobDCj;
import com.morningstar.data.tsapi.blobData.BlobDDB;
import com.morningstar.data.tsapi.blobData.BlobDL;
import com.morningstar.data.tsapi.blobData.BlobDLLL;
import com.morningstar.data.tsapi.blobData.BlobDLLLL;
import com.morningstar.data.tsapi.blobData.BlobDLLLLL;

public class MarketPriceForEquityAndETFBaseObjectUtil {
	
	private static String tsConfigURL = "./config/TsdbBackFill/Ts_Config.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_MarketPrice(BaseObject).log";
	private static tscontext context_r = null;
	private static BaseObject ts_r = null;
	
	private static String tableName = "DBI_MARKETPRICE";
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDLLLL> seriesBlobDLLLL = null;
	static List<BlobDLLLLL> seriesBlobDLLLLL = null;
	static List<BlobDCj> seriesBlobDCj = null;
	static List<BlobDDB> seriesBlobDDB = null;
	static List<BlobDLLL> seriesBlobDLLL = null;
	static List<BlobDL> seriesBlobDL = null;
	
	private static Date dFirstDate = null;
	private static Date dSecondDate = null;
	private static String sFirstDate = null;
	private static String sSecondDate = null;
	private static Double doubleValue1 = null;
	private static Double doubleValue2 = null;
	private static Double doubleValue3 = null;
	private static Double doubleValue4 = null;
	private static String tenChars = null;
	private static Double byteValue = null;
			
	static String sqlToGetValueInMarketPrice = "";
	static String sqlToGetValueInMarketPriceCurrencyHistory = "";
	static String sqlToGetValueOfEffectiveDate = "";
	static String sqlToGetValueInMarketPriceCopyOver = "";
	static String sqlToGetValueInMarketBidOfferMidPrice = "";
	static String sqlToGetValueInTradingVolume = "";
	
	static{
		iniTsReader();
	}

	private static Connection connectToDb(Database database) throws SQLException{		
		Connection con = null;
		con = DBFreshpool.getConnection(database);
		return con;
	}
	
	private static void iniTsReader(){
		try {
			context_r = new tscontext(tsConfigURL);
		} catch (TSException e) {
			System.out.println("TSDB Reader initialization error: " + e.getErrorInfo());
		}
		if(context_r != null){
			ts_r = new BaseObject(context_r);								
		}
	}
	
	private static void getDataFromTSDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,String perfId) throws TSException, Exception{
		switch (dataTypeName){
//		MarketPrice
		case tsMarketPrice:			
			try {
				if(seriesBlobDLLLL == null){
					seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);					
				}				
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);
					if(seriesBlobDLLLL != null){
						break;
					}
				}
				if(seriesBlobDLLLL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
			break;
			
		case tsMarketPriceCurrencyHistory:
			try {
				if(seriesBlobDCj == null){
					seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
				}
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
					if(seriesBlobDCj != null){
						break;
					}
					
				}
				if(seriesBlobDCj == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsMarketPriceCopyOver:
			try {
				if(seriesBlobDDB == null){
					seriesBlobDDB = ts_r.LoadSeries(BlobDDB.class, tsType, perfId);
				}				
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDB =ts_r.LoadSeries(BlobDDB.class, tsType, perfId);
					if(seriesBlobDDB != null){
						break;
					}
				}
				if(seriesBlobDDB == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsMarketBidOfferMidPrice:
			try {
				if(seriesBlobDLLL == null){
					seriesBlobDLLL = ts_r.LoadSeries(BlobDLLL.class, tsType, perfId);
				}
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLLL =ts_r.LoadSeries(BlobDLLL.class, tsType, perfId);
					if(seriesBlobDLLL != null){
						break;
					}
				}
				if(seriesBlobDLLL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsTradingVolume:
			try {
				if(seriesBlobDL == null){
					seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);					
				}
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL =ts_r.LoadSeries(BlobDL.class, tsType, perfId);
					if(seriesBlobDL != null){
						break;
					}
				}
				if(seriesBlobDL == null){
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
					System.out.println("[ERROR]Current TSDB context doesn't been closed! error :" +e.getErrorInfo());
				}
			}
		}

//返回Map<String,Map<String,List<String>>>
//外侧Map的Key为PerformanceId Value为一个Map
//内侧Map的Key为日期 Value为其他数据的List集合		
	public static Map<String,Map<String,List<String>>> getFullValueFromNetezza(TsBlobDataTypeBaseObject tsType,Database database,List<String> perfIdList,String tableName) throws SQLException{
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
//Connect to DB		
		if(conn ==  null){			
			conn = connectToDb(database);
		}
		
		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(tsType){
			case tsMarketPrice:
				sqlToGetValueInMarketPrice = "SELECT EffectiveDate,OpenPrice,HighPrice,LowPrice,ClosePrice FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND OpenPrice IS NOT NULL AND HighPrice IS NOT NULL AND LowPrice IS NOT NULL AND ClosePrice IS NOT NULL" +
						" ORDER BY EffectiveDate DESC";			
				
				pstmt = conn.prepareStatement(sqlToGetValueInMarketPrice);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(2)==null){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(2));
					}
					if(rs.getString(3)==null){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(3));
					}
					if(rs.getString(4)==null){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(4));
					}
					if(rs.getString(5)==null){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(5));
					}
					if(rs.getString(1)==null){
						dateValueMap.put("NullValue", valueList);
					}else{
						 dateValueMap.put(rs.getString(1), valueList);
					}
				}
				
				resultMap.put(perfIdStr, dateValueMap);
				break;
				
			case tsMarketPriceCurrencyHistory:
//特殊逻辑，针对于tsMarketPriceCurrencyHistory->1584，对于TSDB，仅保存Currency改变的那天记录，因此要升序排列且只存放第一个记录
				String EffectiveDate = "";
				sqlToGetValueOfEffectiveDate = "SELECT DISTINCT FIRST_VALUE(EffectiveDate) OVER(ORDER BY CurrencyId) FROM " +
						 tableName+" WHERE PERFORMANCEID = '"+ perfIdStr + "' AND CurrencyId IS NOT NULL";
				
				pstmt = conn.prepareStatement(sqlToGetValueOfEffectiveDate);
				rs = pstmt.executeQuery();				
				while(rs.next()){
					EffectiveDate = rs.getString(1);
				}				
				sqlToGetValueInMarketPriceCurrencyHistory = "SELECT EffectiveDate,CurrencyId FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND EffectiveDate = '" + EffectiveDate + "'";
			pstmt = conn.prepareStatement(sqlToGetValueInMarketPriceCurrencyHistory);
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

//特殊逻辑，对于TsType =  "tsMarketPriceCopyOver",只有当EFFECTIVEDATE <> LASTTRADEDATE时数据才会被更新			
			case tsMarketPriceCopyOver:
				sqlToGetValueInMarketPriceCopyOver = "SELECT EffectiveDate,LastTradeDate,CopyOverReason FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND LastTradeDate IS NOT NULL AND CopyOverReason IS NOT NULL " +
						"AND CAST(CAST(EFFECTIVEDATE AS CHAR(10)) AS DATE) != CAST(LASTTRADEDATE AS DATE) " +
						"ORDER BY EffectiveDate DESC";

			pstmt = conn.prepareStatement(sqlToGetValueInMarketPriceCopyOver);
			rs = pstmt.executeQuery();
			while(rs.next()){
				List<String> valueList = new ArrayList<String>();
				if(rs.getString(2).isEmpty()){
					valueList.add("NullValue");
				}else{
					valueList.add(rs.getString(2));
				}
				if(rs.getString(3).isEmpty()){
					valueList.add("NullValue");
				}else{
					valueList.add(rs.getString(3));
				}
				if(rs.getString(1).isEmpty()){
					dateValueMap.put("NullValue", valueList);
				}else{
					dateValueMap.put(rs.getString(1), valueList);
				}
			}
			resultMap.put(perfIdStr, dateValueMap);
			break;
			
			case tsMarketBidOfferMidPrice:
				sqlToGetValueInMarketBidOfferMidPrice = "SELECT EffectiveDate,BidPrice,AskPrice,MiddlePrice FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND MiddlePrice IS NOT NULL AND BidPrice IS NOT NULL AND AskPrice IS NOT NULL " +
								"ORDER BY EffectiveDate DESC";

				pstmt = conn.prepareStatement(sqlToGetValueInMarketBidOfferMidPrice);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(2).isEmpty()){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(2));
					}
					if(rs.getString(3).isEmpty()){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(3));
					}
					if(rs.getString(4).isEmpty()){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(4));
					}
					if(rs.getString(1).isEmpty()){
						dateValueMap.put("NullValue", valueList);
					}else{
						dateValueMap.put(rs.getString(1), valueList);
					}
				}
				resultMap.put(perfIdStr, dateValueMap);
				break;
				
			case tsTradingVolume:
				sqlToGetValueInTradingVolume = "SELECT EffectiveDate,TradingVolume FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND TradingVolume IS NOT NULL ORDER BY EffectiveDate DESC";

				pstmt = conn.prepareStatement(sqlToGetValueInTradingVolume);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(2).isEmpty()){
						valueList.add("NullValue");
					}else{
						valueList.add(rs.getString(2));
					}
					if(rs.getString(1).isEmpty()){
						dateValueMap.put("NullValue", valueList);
					}else{
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
		if(rs != null){
			rs.close();
		}
		if(pstmt != null){
			pstmt.close();
		}
		if(conn != null){
			conn.close();
		}
		return resultMap;
	}
	
//返回Map<String,Map<String,List<String>>>
//外侧Map的Key为PerformanceId Value为一个Map
//内侧Map的Key为日期 Value为其他数据的List集合	
	public synchronized static Map<String,Map<String,List<String>>> getFullValueFromTsDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,List<String> perfIdList) {
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
				
		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(dataTypeName) {
			case tsMarketPrice:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					if(seriesBlobDLLLL != null && !seriesBlobDLLLL.isEmpty()){
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
						resultMap.put(perfIdStr, dateValueMap);
					}										
				} catch (TSException e) {
//Check in Netezza
					String countSql4tsMarketPrice = "SELECT COUNT(1) FROM "+tableName+" WHERE PERFORMANCEID = '" +perfIdStr+
							"' AND EffectiveDate IS NOT NULL AND OpenPrice IS NOT NULL AND HighPrice IS NOT NULL " +
							"AND LowPrice IS NOT NULL AND ClosePrice IS NOT NULL";
					try {
						String count1 = DBCommons.getData(countSql4tsMarketPrice, Database.Netezza2);
						System.out.println("[INFO]>>>>>>>>>>Facing with exception while getting data from TSDB,TsType="+dataTypeName+" PerformanceId="+perfIdStr);
						System.out.println("[INFO]>>>>>>>>>>Checking data records in Netezza...");
						if(Integer.valueOf(count1)>=1){
							System.out.println("[ERROR]Getting data from TSDB error,PerformanceId="+perfIdStr +" TsType="+dataTypeName +" error code: " + e.getErrorCode());							
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " has found data records in netezza,count is: "+count1);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
						}						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}										
				} catch (Exception e) {
					e.printStackTrace();
				}
				seriesBlobDLLLL = null;				
				break;
			
			case tsMarketPriceCurrencyHistory:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					if(seriesBlobDCj != null && !seriesBlobDCj.isEmpty()){
						for(BlobDCj data : seriesBlobDCj){
							List<String> valueList = new ArrayList<String>();
							dFirstDate = data.getFirstDateValue().ToDate();
							if(dFirstDate != null){				
								sFirstDate = dataFormat.format(dFirstDate);								
							}else{
								sFirstDate = "NullValue";
							}
							tenChars = data.getTenChars();
							if(!tenChars.isEmpty()){
								valueList.add(tenChars);
							}else{
								valueList.add("NullValue");
							}
							dateValueMap.put(sFirstDate, valueList);
						}
						resultMap.put(perfIdStr, dateValueMap);
					}					
				} catch (TSException e) {
//Check in Netezza
					String countSql4tsMarketPriceCurrencyHistory = "SELECT COUNT(1) FROM "+tableName+" WHERE PERFORMANCEID = '" +perfIdStr+
							"' AND CurrencyId IS NOT NULL";
					try {
						String count2 = DBCommons.getData(countSql4tsMarketPriceCurrencyHistory, Database.Netezza2);
						System.out.println("[INFO]>>>>>>>>>>Facing with exception while getting data from TSDB,TsType="+dataTypeName+" PerformanceId="+perfIdStr);
						System.out.println("[INFO]>>>>>>>>>>Checking data records in Netezza...");
						if(Integer.valueOf(count2)>=1){
							System.out.println("[ERROR]Getting data from TSDB error,PerformanceId="+perfIdStr +" TsType="+dataTypeName +" error code: " + e.getErrorCode());							
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " has found data records in netezza,count is: "+count2);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
						}						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}					
				} catch (Exception e) {
					e.printStackTrace();
				}
				seriesBlobDCj = null;
				break;
				
			case tsMarketPriceCopyOver:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					if(seriesBlobDDB != null && !seriesBlobDDB.isEmpty()){
						for(BlobDDB data:seriesBlobDDB){
							List<String> valueList = new ArrayList<String>();
							dFirstDate = data.getFirstDateValue().ToDate();
							if(dFirstDate != null){
								sFirstDate = dataFormat.format(dFirstDate);
							}else{
								sFirstDate = "NullValue";
							}
							dSecondDate = data.getSecondDateValue().ToDate();
							if(dSecondDate != null){
								sSecondDate = dataFormat.format(dSecondDate);
								valueList.add(sSecondDate);
							}else{
								sSecondDate = "NullValue";
								valueList.add(sSecondDate);
							}
							byteValue = data.getByteValue();
							if(byteValue != null){
								valueList.add(String.valueOf(byteValue));
							}else{
								valueList.add("NullValue");
							}
							dateValueMap.put(sFirstDate, valueList);
						}
						resultMap.put(perfIdStr, dateValueMap);
					}										
				} catch (TSException e) {									
//Check in Netezza
					String countSql4tsMarketPriceCopyOver = "SELECT COUNT(1) FROM "+tableName+" WHERE PERFORMANCEID = '" +perfIdStr+
							"' AND LastTradeDate IS NOT NULL AND CopyOverReason IS NOT NULL " +
							"AND CAST(CAST(EFFECTIVEDATE AS CHAR(10)) AS DATE) != CAST(LASTTRADEDATE AS DATE)";
					try {
						String count3 = DBCommons.getData(countSql4tsMarketPriceCopyOver, Database.Netezza2);
						System.out.println("[INFO]>>>>>>>>>>Facing with exception while getting data from TSDB,TsType="+dataTypeName+" PerformanceId="+perfIdStr);
						System.out.println("[INFO]>>>>>>>>>>Checking data records in Netezza...");
						if(Integer.valueOf(count3)>=1){
							System.out.println("[ERROR]Getting data from TSDB error,PerformanceId="+perfIdStr +" TsType="+dataTypeName +" error code: " + e.getErrorCode());							
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " has found data records in netezza,count is: "+count3);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
						}						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				seriesBlobDDB = null;
				break;
				
			case tsMarketBidOfferMidPrice:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					if(seriesBlobDLLL != null && !seriesBlobDLLL.isEmpty()){
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
						resultMap.put(perfIdStr, dateValueMap);
					}
				} catch (TSException e) {
//Check in Netezza
					String countSql4tsMarketBidOfferMidPrice = "SELECT COUNT(1) FROM "+tableName+" WHERE PERFORMANCEID = '" +perfIdStr+
							"' AND MiddlePrice IS NOT NULL AND BidPrice IS NOT NULL AND AskPrice IS NOT NULL";							
					try {
						String count4 = DBCommons.getData(countSql4tsMarketBidOfferMidPrice, Database.Netezza2);
						System.out.println("[INFO]>>>>>>>>>>Facing with exception while getting data from TSDB,TsType="+dataTypeName+" PerformanceId="+perfIdStr);
						System.out.println("[INFO]>>>>>>>>>>Checking data records in Netezza...");
						if(Integer.valueOf(count4)>=1){
							System.out.println("[ERROR]Getting data from TSDB error,PerformanceId="+perfIdStr +" TsType="+dataTypeName +" error code: " + e.getErrorCode());							
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " has found data records in netezza,count is: "+count4);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
						}						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}														
				} catch (Exception e) {
					e.printStackTrace();
				}
				seriesBlobDLLL = null;
				break;
			
			case tsTradingVolume:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					if(seriesBlobDL != null && !seriesBlobDL.isEmpty()){
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
						resultMap.put(perfIdStr, dateValueMap);
					}
				} catch (TSException e) {
//Check in Netezza
					String countSql4tsTradingVolume = "SELECT COUNT(1) FROM "+tableName+" WHERE PERFORMANCEID = '" +perfIdStr+
							"' AND TradingVolume IS NOT NULL";							
					try {
						String count5 = DBCommons.getData(countSql4tsTradingVolume, Database.Netezza2);
						System.out.println("[INFO]>>>>>>>>>>Facing with exception while getting data from TSDB,TsType="+dataTypeName+" PerformanceId="+perfIdStr);
						System.out.println("[INFO]>>>>>>>>>>Checking data records in Netezza...");
						if(Integer.valueOf(count5)>=1){
							System.out.println("[ERROR]Getting data from TSDB error,PerformanceId="+perfIdStr +" TsType="+dataTypeName +" error code: " + e.getErrorCode());							
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " not found in TSDB");
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "PerformanceId is: " + perfIdStr + " has found data records in netezza,count is: "+count5);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "TsType NAME is: " + dataTypeName + " TsType no. = " + tsType);
							CustomizedLog.writeCustomizedLogFile(logPath+logName, "<------------------------------------------------------------->");
						}						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}									
				} catch (Exception e) {
					e.printStackTrace();
				}
				seriesBlobDL = null;			
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