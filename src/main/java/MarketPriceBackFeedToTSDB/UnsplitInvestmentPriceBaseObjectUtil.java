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
import com.morningstar.data.tsapi.blobData.BlobDLLLL;

public class UnsplitInvestmentPriceBaseObjectUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_UnSplitInvestmentPrice.log";
	private static tscontext context_r = null;
	private static BaseObject ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	static List<BlobDLLLL> seriesBlobDLLLL = null;
	
	private static Date dFirstDate = null;
	private static String sFirstDate = null;
	private static Double doubleValue1;
	private static Double doubleValue2;
	private static Double doubleValue3;
	private static Double doubleValue4;

	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueInTsMarketPrice = "";

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
		case tsMarketPrice:
			try {
				seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLLLL = ts_r.LoadSeries(BlobDLLLL.class, tsType, perfId);
				}
				if(seriesBlobDLLLL.isEmpty()){
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
//特殊逻辑，InvestmentType=130，PriceType = 12				
				sqlToGetValueInTsMarketPrice = "SELECT EffectiveDate,Price FROM " + tableName +" WHERE InvestmentId = '"
				+ perfIdStr + "' AND EffectiveDate IS NOT NULL AND Price IS NOT NULL " +
						"AND InvestmentType=130 AND PriceType = 12 ORDER BY EffectiveDate DESC";
//Getting trouble, re-connect to DB				
				if(con.isClosed() || con == null){
					connectToDb(database);
				}				
				pstmt = con.prepareStatement(sqlToGetValueInTsMarketPrice);
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
							valueList.add(Double.toString(doubleValue1));
						}else{
							valueList.add("NullValue");
						}
						doubleValue2 = data.getDoubleValue2();
						if(doubleValue2 != null){
							valueList.add(Double.toString(doubleValue2));
						}else{
							valueList.add("NullValue");
						}
						doubleValue3 = data.getDoubleValue3();
						if(doubleValue3 != null){
							valueList.add(Double.toString(doubleValue3));
						}else{
							valueList.add("NullValue");
						}
						doubleValue4 = data.getDoubleValue4();
						if(doubleValue4 != null){
							valueList.add(Double.toString(doubleValue4));
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