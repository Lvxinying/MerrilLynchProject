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
import com.morningstar.data.tsapi.blobData.BlobDCj;
import com.morningstar.data.tsapi.blobData.BlobDL;
import com.morningstar.data.tsapi.blobData.BlobDLLLL;

public class MarketPriceForWarrantsBaseObjectUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_MarketPriceForWarrants(BaseObject).log";
	private static tscontext context_r = null;
	private static  BaseObject ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDLLLL> seriesBlobDLLLL = null;
	static List<BlobDCj> seriesBlobDCj = null;
	static List<BlobDL> seriesBlobDL = null;
	
	private static Date dFirstDate = null;
	private static String sFirstDate = null;
	private static Double doubleValue1 = null;
	private static Double doubleValue2 = null;
	private static Double doubleValue3 = null;
	private static Double doubleValue4 = null;
	private static String tenChars = null;
	
	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueInWarrantMarketPrice = "";
	static String sqlToGetValueInWarrantMarketPriceCurrencyHistory = "";
	static String sqlToGetValueInTradingVolume = "";
	
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
//		MarketPrice
		case tsWarrantMarketPrice:
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
		case tsWarrantMarketPriceCurrencyHistory:
			try {
				seriesBlobDCj =ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
				}
				if(seriesBlobDCj == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsWarrantTradingVolume:
			try {
				seriesBlobDL =ts_r.LoadSeries(BlobDL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL =ts_r.LoadSeries(BlobDL.class, tsType, perfId);
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
			
			case tsWarrantMarketPrice:
				sqlToGetValueInWarrantMarketPrice = "SELECT EFFECTIVEDATE,OPENPRICE,HIGHPRICE,LOWPRICE,CLOSEPRICE FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND OPENPRICE IS NOT NULL AND HIGHPRICE IS NOT NULL AND LOWPRICE IS NOT NULL AND CLOSEPRICE IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
			if(con == null || con.isClosed()){
				connectToDb(database);
			}
			pstmt = con.prepareStatement(sqlToGetValueInWarrantMarketPrice);
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
			
			case tsWarrantMarketPriceCurrencyHistory:
				sqlToGetValueInWarrantMarketPriceCurrencyHistory = "SELECT EffectiveDate,CurrencyId FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND CurrencyId IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
			if(con == null || con.isClosed()){
				connectToDb(database);
			}
			pstmt = con.prepareStatement(sqlToGetValueInWarrantMarketPriceCurrencyHistory);
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
				
			case tsWarrantTradingVolume:
				sqlToGetValueInTradingVolume = "SELECT EffectiveDate,TradingVolume FROM " + tableName +" WHERE PERFORMANCEID = '"
						+ perfIdStr + "' AND TradingVolume IS NOT NULL ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInTradingVolume);
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
			case tsWarrantMarketPrice:
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
			
			case tsWarrantMarketPriceCurrencyHistory:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
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
				
			case tsWarrantTradingVolume:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDL data:seriesBlobDL){
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
				default:
					break;
			}
		}
//dispose connection to TSDB		
		tsContextRecover();
		return resultMap;
	}
}