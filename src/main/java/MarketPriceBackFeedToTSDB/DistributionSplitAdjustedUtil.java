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
import com.morningstar.data.tsapi.blobData.BlobDDF;
import com.morningstar.data.tsapi.blobData.BlobDL;

public class DistributionSplitAdjustedUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_Distribution(SplitAdjusted).log";
	private static tscontext context_r = null;
	private static  CorporateActionAdjustment ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDL> seriesBlobDL = null;
	static List<BlobDDF> seriesBlobDDF = null;
	
	private static Date dFirstDate = null;
	private static String sFirstDate = null;
	private static Date dSecondDate = null;
	private static String sSecondDate = null;
	private static Double douvbleValue1;
	private static Float floatValue;
	
	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueInCashDividend = "";
	static String sqlToGetValueInCapitalGain = "";

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
		case tsCashDividend:
			try {
				seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDL = ts_r.LoadSeries(BlobDL.class, tsType, perfId);
				}
				if(seriesBlobDL.isEmpty()){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
			
		case tsCapitalGain:
			try {
				seriesBlobDDF = ts_r.LoadSeries(BlobDDF.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDF = ts_r.LoadSeries(BlobDDF.class, tsType, perfId);
				}
				if(seriesBlobDDF.isEmpty()){
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
//特殊逻辑，只选择 distributiontype = 1 的记录			
			case tsCashDividend:
				sqlToGetValueInCashDividend = "SELECT ExcludingDate,TotalDistribution FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND DistributionType = 1 AND ExcludingDate IS NOT NULL AND TotalDistribution IS NOT NULL ORDER BY ExcludingDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
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
				
			case tsCapitalGain:
//特殊逻辑，DistributionType = 2				
				sqlToGetValueInCapitalGain = "SELECT ExcludingDate,PayDate,TotalDistribution FROM " + tableName +" WHERE PERFORMANCEID = '"
				+ perfIdStr + "' AND DistributionType = 2 AND ExcludingDate IS NOT NULL AND PayDate IS NOT NULL AND TotalDistribution IS NOT NULL ORDER BY ExcludingDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInCapitalGain);
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
			case tsCashDividend:
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
						douvbleValue1 = data.getDoubleValue1();
						if(douvbleValue1 != null){
							valueList.add(Double.toString(douvbleValue1));
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
			
			case tsCapitalGain:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDF data :seriesBlobDDF){					
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
						}
						if(!sSecondDate.isEmpty()){
							valueList.add(sSecondDate);
						}else{
							valueList.add("NullValue");
						}
						floatValue = data.getFloatValue();
						if(floatValue != null){
							valueList.add(Float.toString(floatValue));
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