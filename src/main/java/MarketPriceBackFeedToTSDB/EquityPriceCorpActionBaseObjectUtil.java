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
import com.morningstar.data.tsapi.blobData.BlobCjLL;
import com.morningstar.data.tsapi.blobData.BlobDB;
import com.morningstar.data.tsapi.blobData.BlobDBSet50;
import com.morningstar.data.tsapi.blobData.BlobDCj;
import com.morningstar.data.tsapi.blobData.BlobDDDD;
import com.morningstar.data.tsapi.blobData.BlobDFF;
import com.morningstar.data.tsapi.blobData.BlobDL;
import com.morningstar.data.tsapi.blobData.BlobDLL;
import com.morningstar.data.tsapi.blobData.BlobDLLL;
import com.morningstar.data.tsapi.blobData.BlobDLLLL;

public class EquityPriceCorpActionBaseObjectUtil {
	
	private static String tsConfigURL = "http://tsdevwriter81/Inte-config/ts_proxy.xml";
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName = "404NotFoundDataInTSDB_DBI_EquityPriceCorpAction(BaseObject).log";
	private static tscontext context_r = null;
	private static  BaseObject ts_r = null;
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");

	static List<BlobDLLLL> seriesBlobDLLLL = null;
	
	static List<BlobDLL> seriesBlobDLL = null;
	static List<BlobDBSet50> seriesBlobDBSet50 = null;
	static List<BlobDFF> seriesBlobDFF = null;
	static List<BlobDL> seriesBlobDL = null;
	static List<BlobDB> seriesBlobDB = null;
	static List<BlobDCj> seriesBlobDCj = null;
	static List<BlobDDDD> seriesBlobDDDD = null;
	static List<BlobDLLL> seriesBlobDLLL = null;
	
	
	private static Date dFirstDate = null;
	private static Date dDateValue2 = null;
	private static Date dDateValue3 = null;
	private static Date dDateValue4 = null;
	private static String sFirstDate = null;
	private static String sDateValue2 = null;
	private static String sDateValue3 = null;
	private static String sDateValue4 = null;
	private static byte byteValue;
	private static String byteValueStr = null;
	private static Double doubleValue1 = null;
	private static Double doubleValue2 = null;
	private static Double doubleValue3 = null;
	private static String tenCharStr = null;
	private static float floatValue1;
	private static float floatValue2;

	
	static List<String> dateList = new ArrayList<String>();
	static Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>(); 
	
	private static Connection con = null;
	private static ResultSet rs = null;
	private static PreparedStatement pstmt = null;
	
	static String sqlToGetValueInTenforeCurrencyTradingPrice = "";
	static String sqlToGetValueInForexSpotExchangeRate = "";
	
	static String sqlToGetValueInShareSplitRatio = "";
	static String sqlToGetValueInCashDividendDates = "";
	static String sqlToGetValueInSpinoff = "";
	static String sqlToGetValueInStockDistribution = "";
	static String sqlToGetValueInSpecialCashDividend = "";
	static String sqlToGetValueInDividendFrequency = "";
	static String sqlToGetValueInCashDividend = "";
	static String sqlToGetValueInCashDividendCurrencyHistory = "";
	static String sqlToGetValueInSpecialCashDividendDates = "";
	static String sqlToGetValueInSpecialCashDividendCurrencyHistory = "";
	static String sqlToGetValueInSplitDates = "";
	static String sqlToGetValueInStockDistributionDates = "";
	static String sqlToGetValueInRightsOfferingDates = "";
	static String sqlToGetValueInRightsOffering = "";
	static String sqlToGetValueInRightsOfferingAdjustmentFactor = "";
	static String sqlToGetValueInRightsOfferingCurrencyHistory = "";
	
	
	
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
		case tsShareSplitRatio:
			try {
				seriesBlobDLL = ts_r.LoadSeries(BlobDLL.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDLL = ts_r.LoadSeries(BlobDLL.class, tsType, perfId);
				}
				if(seriesBlobDLL == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsSpinoff:
			try {
				seriesBlobDBSet50 = ts_r.LoadSeries(BlobDBSet50.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDBSet50 = ts_r.LoadSeries(BlobDBSet50.class, tsType, perfId);
				}
				if(seriesBlobDBSet50 == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsStockDistribution:
			try {
				seriesBlobDFF = ts_r.LoadSeries(BlobDFF.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDFF = ts_r.LoadSeries(BlobDFF.class, tsType, perfId);
				}
				if(seriesBlobDFF == null){
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
		case tsDividendFrequency:
			try {
				seriesBlobDB = ts_r.LoadSeries(BlobDB.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDB = ts_r.LoadSeries(BlobDB.class, tsType, perfId);
				}
				if(seriesBlobDB == null){
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
		case tsCashDividendCurrencyHistory:
			try {
				seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
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
		case tsSpecialCashDividendDates:
			try {
				seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
				}
				if(seriesBlobDDDD == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsSpecialCashDividendCurrencyHistory:
			try {
				seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
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
		case tsSplitDates:
			try {
				seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
				}
				if(seriesBlobDDDD == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsStockDistributionDates:
			try {
				seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
				}
				if(seriesBlobDDDD == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsRightsOfferingDates:
			try {
				seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
				}
				if(seriesBlobDDDD == null){
					System.out.println("[ERROR]The performance Id: "+perfId+" may not in TsDB!");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case tsRightsOffering:
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
		case tsRightsOfferingAdjustmentFactor:
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
		case tsRightsOfferingCurrencyHistory:
			try {
				seriesBlobDCj = ts_r.LoadSeries(BlobDCj.class, tsType, perfId);
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
			
		case tsCashDividendDates:
			try {
				seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
			} catch (TSException e) {
				for(int i=0;i<3;i++){
					seriesBlobDDDD = ts_r.LoadSeries(BlobDDDD.class, tsType, perfId);
				}
				if(seriesBlobDDDD == null){
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
//特殊逻辑，只选择type = 'SS'的数据			
			case tsShareSplitRatio:
				sqlToGetValueInShareSplitRatio = "SELECT ExDate,SplitFrom,SplitTo FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND SplitFrom IS NOT NULL AND SplitTo IS NOT NULL " +
								"AND type = 'SS' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInShareSplitRatio);
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
				
			case tsSpinoff:
//特殊逻辑，只选择type = 'SP'的值				
				sqlToGetValueInSpinoff = "SELECT ExDate,NumberOfShares,Amount,ChildPerformanceId FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Amount IS NOT NULL AND ChildPerformanceId IS NOT NULL " +
					    "AND NumberOfShares IS NOT NULL AND type = 'SP' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInSpinoff);
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
				
			case tsStockDistribution:
//特殊逻辑，只选择type = 'SD'的值				
				sqlToGetValueInStockDistribution = "SELECT ExDate,SplitFrom,SplitTo FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND SplitFrom IS NOT NULL AND SplitTo IS NOT NULL " +
					    "AND type = 'SD' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInStockDistribution);
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
				
			case tsSpecialCashDividend:
//特殊逻辑，只选择type="SC"的数据				
				sqlToGetValueInSpecialCashDividend = "SELECT ExDate,Amount FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Amount IS NOT NULL AND type = 'SC' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
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
				
			case tsDividendFrequency:
//特殊逻辑，只选择Type='CD'	的数据			
				sqlToGetValueInDividendFrequency = "SELECT ExDate,Frequency FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Frequency IS NOT NULL AND type = 'CD' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInDividendFrequency);
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
//特殊逻辑，只选择type = 'CD'的数据				
				sqlToGetValueInCashDividend = "SELECT ExDate,Amount FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Amount IS NOT NULL AND type = 'CD' ORDER BY ExDate DESC";
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
				
			case tsCashDividendCurrencyHistory:
//特殊逻辑，只选择type='CD'的数据				
				sqlToGetValueInCashDividendCurrencyHistory = "SELECT ExDate,Currency FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Currency IS NOT NULL AND type = 'CD' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInCashDividendCurrencyHistory);
				rs = pstmt.executeQuery();
				while(rs.next()){
					List<String> valueList = new ArrayList<String>();
					if(rs.getString(1).isEmpty()){
						if(!rs.getString(2).isEmpty()){
							valueList.add(rs.getString(2));
							dateValueMap.put("NullValue", valueList);
						}				
					}else{
						if(!rs.getString(2).isEmpty()){
							valueList.add(rs.getString(2));
							dateValueMap.put(rs.getString(1), valueList);
						}
					}
				}
				resultMap.put(perfIdStr, dateValueMap);
				break;
				
			case tsSpecialCashDividendDates:
//特殊逻辑，只选择type = 'SC'的数据
				sqlToGetValueInSpecialCashDividendDates = "SELECT ExDate,DeclareDate,RecordDate,PayDate FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND DeclareDate IS NOT NULL AND RecordDate IS NOT NULL" +
								" AND PayDate IS NOT NULL AND type = 'SC' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInSpecialCashDividendDates);
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
				
			case tsSpecialCashDividendCurrencyHistory:
//特殊逻辑，只选择type='SC'的数据				
				sqlToGetValueInSpecialCashDividendCurrencyHistory = "SELECT ExDate,Currency FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Currency IS NOT NULL AND type = 'SC' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInSpecialCashDividendCurrencyHistory);
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
				
			case tsSplitDates:
//特殊逻辑，只选择type='SS'的数据				
				sqlToGetValueInSplitDates = "SELECT ExDate,DeclareDate,RecordDate,PayDate FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND DeclareDate IS NOT NULL AND RecordDate IS NOT NULL" +
								" AND PayDate IS NOT NULL AND type = 'SS' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInSplitDates);
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
				
			case tsStockDistributionDates:
//特殊逻辑，只选择type='SD'的数据
				sqlToGetValueInStockDistributionDates = "SELECT ExDate,DeclareDate,RecordDate,PayDate FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND DeclareDate IS NOT NULL AND RecordDate IS NOT NULL" +
								" AND PayDate IS NOT NULL AND type = 'SD' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInStockDistributionDates);
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
				
			case tsRightsOfferingDates:
//特殊数据，只选择type='RI'的数据				
				sqlToGetValueInRightsOfferingDates = "SELECT ExDate,DeclareDate,RecordDate,PayDate FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND DeclareDate IS NOT NULL AND RecordDate IS NOT NULL" +
								" AND PayDate IS NOT NULL AND type = 'RI' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInRightsOfferingDates);
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
				
			case tsRightsOffering:
//特殊逻辑，只选择type='RI'的数据				
				sqlToGetValueInRightsOffering = "SELECT ExDate,ExistingShare,NewShare,SubscriptionPrice FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND NewShare IS NOT NULL AND ExistingShare IS NOT NULL" +
								" AND SubscriptionPrice IS NOT NULL AND type = 'RI' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInRightsOffering);
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
				
			case tsRightsOfferingAdjustmentFactor:
//特殊逻辑，只选择type='RI'的数据				
				sqlToGetValueInRightsOfferingAdjustmentFactor = "SELECT ExDate,Amount FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Amount IS NOT NULL AND type = 'RI' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInRightsOfferingAdjustmentFactor);
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
				
			case tsRightsOfferingCurrencyHistory:
//特殊逻辑，只选择type='RI'的数据				
				sqlToGetValueInRightsOfferingCurrencyHistory = "SELECT ExDate,Currency FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND Currency IS NOT NULL AND type = 'RI' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInRightsOfferingCurrencyHistory);
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
				
			case tsCashDividendDates:
//特殊逻辑，只选择type = 'CD'的数据
				sqlToGetValueInCashDividendDates = "SELECT ExDate,DeclareDate,RecordDate,PayDate FROM " + tableName +" WHERE PerformanceId = '"
						+ perfIdStr + "' AND ExDate IS NOT NULL AND DeclareDate IS NOT NULL AND RecordDate IS NOT NULL" +
								" AND PayDate IS NOT NULL AND type = 'CD' ORDER BY ExDate DESC";
//Getting trouble, re-connect to DB				
				if(con == null || con.isClosed()){
					connectToDb(database);
				}
				pstmt = con.prepareStatement(sqlToGetValueInCashDividendDates);
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
	public static Map<String,Map<String,List<String>>> getFullValueFromTsDB(TsBlobDataTypeBaseObject dataTypeName,int tsType,List<String> perfIdList) {
		Map<String,Map<String,List<String>>> resultMap = new HashMap<String,Map<String,List<String>>>();
				
		for(String perfIdStr : perfIdList){
			Map<String,List<String>> dateValueMap = new HashMap<String,List<String>>();
			switch(dataTypeName) {							
			case tsShareSplitRatio:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDLL data : seriesBlobDLL){
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
				
			case tsSpinoff:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDBSet50 data : seriesBlobDBSet50){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
	//暂时不要byteValue					
//						byteValue = data.getByteValue();
//						byteValueStr = Byte.toString(byteValue);
//						if(byteValueStr != null){
//							valueList.add(byteValueStr);
//						}else{
//							valueList.add("NullValue");
//						}
						List<BlobCjLL> sublist = data.getSetsValue();
						for(BlobCjLL setValue : sublist){
							doubleValue1 = setValue.getDoubleValue1();
							if(doubleValue1 != null){
								valueList.add(doubleValue1.toString());
							}else{
								valueList.add("NullValue");
							}
							doubleValue2 = setValue.getDoubleValue2();
							if(doubleValue2 != null){
								valueList.add(doubleValue2.toString());
							}else{
								valueList.add("NullValue");
							}
							tenCharStr = setValue.getTenChars();
							if(tenCharStr != null){
								valueList.add(tenCharStr);
							}else{
								valueList.add("NullValue");
							}
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
				
			case tsStockDistribution:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDFF data : seriesBlobDFF){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						floatValue1 = data.getFloatValue1();					
						if(floatValue1 == 0){
							valueList.add("NullValue");
						}else{
							valueList.add(String.valueOf(floatValue1));
						}
						floatValue2 = data.getFloatValue2();
						if(floatValue2 == 0){
							valueList.add("NullValue");
						}else{
							valueList.add(String.valueOf(floatValue2));
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
				
			case tsDividendFrequency:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDB data : seriesBlobDB){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						byteValue = data.getByteValue();
						byteValueStr = Byte.toString(byteValue);				
						if(byteValueStr != null){
							valueList.add(byteValueStr);
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
				
			case tsCashDividendCurrencyHistory:
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
						tenCharStr = data.getTenChars();								
						if(tenCharStr != null){
							valueList.add(String.valueOf(tenCharStr));
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
				
			case tsSpecialCashDividendDates:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDDD data : seriesBlobDDDD){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						dDateValue2 = data.getDateValue2().ToDate();
						if(dDateValue2 != null){
							sDateValue2 = dataFormat.format(dDateValue2);
							valueList.add(sDateValue2);
						}else{
							valueList.add("NullValue");
						}
						dDateValue3 = data.getDateValue3().ToDate();
						if(dDateValue3 != null){
							sDateValue3 = dataFormat.format(dDateValue3);
							valueList.add(sDateValue3);
						}else{
							valueList.add("NullValue");
						}
						dDateValue4 = data.getDateValue4().ToDate();
						if(dDateValue4 != null){
							sDateValue4 = dataFormat.format(dDateValue4);
							valueList.add(sDateValue4);
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
				
			case tsSpecialCashDividendCurrencyHistory:
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
						tenCharStr = data.getTenChars();								
						if(tenCharStr != null){
							valueList.add(String.valueOf(tenCharStr));
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
				
			case tsSplitDates:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDDD data : seriesBlobDDDD){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						dDateValue2 = data.getDateValue2().ToDate();
						if(dDateValue2 != null){
							sDateValue2 = dataFormat.format(dDateValue2);
							valueList.add(sDateValue2);
						}else{
							valueList.add("NullValue");
						}
						dDateValue3 = data.getDateValue3().ToDate();
						if(dDateValue3 != null){
							sDateValue3 = dataFormat.format(dDateValue3);
							valueList.add(sDateValue3);
						}else{
							valueList.add("NullValue");
						}
						dDateValue4 = data.getDateValue4().ToDate();
						if(dDateValue4 != null){
							sDateValue4 = dataFormat.format(dDateValue4);
							valueList.add(sDateValue4);
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
				
			case tsStockDistributionDates:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDDD data : seriesBlobDDDD){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						dDateValue2 = data.getDateValue2().ToDate();
						if(dDateValue2 != null){
							sDateValue2 = dataFormat.format(dDateValue2);
							valueList.add(sDateValue2);
						}else{
							valueList.add("NullValue");
						}
						dDateValue3 = data.getDateValue3().ToDate();
						if(dDateValue3 != null){
							sDateValue3 = dataFormat.format(dDateValue3);
							valueList.add(sDateValue3);
						}else{
							valueList.add("NullValue");
						}
						dDateValue4 = data.getDateValue4().ToDate();
						if(dDateValue4 != null){
							sDateValue4 = dataFormat.format(dDateValue4);
							valueList.add(sDateValue4);
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
				
			case tsRightsOfferingDates:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDDD data : seriesBlobDDDD){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						dDateValue2 = data.getDateValue2().ToDate();
						if(dDateValue2 != null){
							sDateValue2 = dataFormat.format(dDateValue2);
							valueList.add(sDateValue2);
						}else{
							valueList.add("NullValue");
						}
						dDateValue3 = data.getDateValue3().ToDate();
						if(dDateValue3 != null){
							sDateValue3 = dataFormat.format(dDateValue3);
							valueList.add(sDateValue3);
						}else{
							valueList.add("NullValue");
						}
						dDateValue4 = data.getDateValue4().ToDate();
						if(dDateValue4 != null){
							sDateValue4 = dataFormat.format(dDateValue4);
							valueList.add(sDateValue4);
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
				
			case tsRightsOffering:
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
				
			case tsRightsOfferingAdjustmentFactor:
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
				
			case tsRightsOfferingCurrencyHistory:
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
						tenCharStr = data.getTenChars();
						if(tenCharStr != null){
							valueList.add(tenCharStr);
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
				
			case tsCashDividendDates:
				try {
					getDataFromTSDB(dataTypeName,tsType,perfIdStr);
					for(BlobDDDD data : seriesBlobDDDD){
						List<String> valueList = new ArrayList<String>();
						dFirstDate = data.getFirstDateValue().ToDate();
						if(dFirstDate != null){
							sFirstDate = dataFormat.format(dFirstDate);
						}else{
							sFirstDate = "NullValue";
						}
						dDateValue2 = data.getDateValue2().ToDate();
						if(dDateValue2 != null){
							sDateValue2 = dataFormat.format(dDateValue2);
							valueList.add(sDateValue2);
						}else{
							valueList.add("NullValue");
						}
						dDateValue3 = data.getDateValue3().ToDate();
						if(dDateValue3 != null){
							sDateValue3 = dataFormat.format(dDateValue3);
							valueList.add(sDateValue3);
						}else{
							valueList.add("NullValue");
						}
						dDateValue4 = data.getDateValue4().ToDate();
						if(dDateValue4 != null){
							sDateValue4 = dataFormat.format(dDateValue4);
							valueList.add(sDateValue4);
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