package MarketPriceBackFeedToTSDB;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.TSException;

public class DailyMarketReturnIndexSplitAdjusted {
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName1 = "MissingRecord_DBI_DailyMarketReturnIndex(SplitAdjusted).log";
	private static String logName2 = "ContentChecking_DBI_DailyMarketReturnIndex(SplitAdjusted).log";
	static SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
//Get PerformanceIdList(EOD Mode)--DBI_DAILYMARKETRETURNINDEX_SPECIALCASE
	private static List<String> getUpdatedPerformanceIdListEODMode(String tableName,String createdOnTime,String InvestmentType) throws SQLException{
		List<String> list = new ArrayList<String>();
		String sqlToGetUpdatedPerformanceId = null;
		long startTime = System.currentTimeMillis();
//保留CREATEDON的使用
		if(createdOnTime.isEmpty()){
			createdOnTime = "2000-01-01";
		}
		switch(InvestmentType){
		case "ETF":
//Case = DMRI-ETF, INVESTMENTTYPE = 'ET'			
			sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
					 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
					 " WHERE ND.IDTYPE = 2 AND EU.INVESTMENTTYPE = 'ET' AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
			break;
			
		case "CE":
//Case =  EMRI-CE, INVESTMENTTYPE = 'CE'			
			sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
					 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
					 " WHERE ND.IDTYPE = 2 AND EU.INVESTMENTTYPE = 'CE' AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
			break;
			
		case "IN":
//Case =  DMRI-Index, INVESTMENTTYPE = 'IN'			
			sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
					 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
					 " WHERE ND.IDTYPE = 2 AND EU.INVESTMENTTYPE = 'IN' AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
			break;
			
		case "EQUITY":
//Case =  DMRI-Equity, INVESTMENTTYPE != 'ET','IN','CE'	
			sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
					 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
					 " WHERE ND.IDTYPE = 2 AND EU.INVESTMENTTYPE NOT IN ('ET','IN','CE') AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
			break;
		default:
			break;
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(EOD MODE),investmenttype<==>"+InvestmentType+",time cost: "+endTime+" ms");
		return list;
	}

//Get PerformanceIdList(Individual Mode)
	private static List<String> getUpdatedPerformanceIdListIndividualMode(String testedTableName,int TsType, int testedIdCounts){
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//选取IdType != 2的所有情况，且PROCESSNAME为DMRI for Equity或者DMRI for ETF才能保证是BASE OBJECT的		
		String sqlToGetUpdatedPerformanceIdList = "SELECT ID FROM ADT_NOTIFICATIONDRIVER WHERE " +
				"IDTYPE != 2 TSTYPE = " + TsType + " AND TABLENAME = '" + testedTableName + "' LIMIT "+testedIdCounts;
		try {
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceIdList, Database.Netezza1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got " + list.size() +" performanceids(Individual MODE), TSTYPE = "+ TsType + ",time cost: "+endTime+" ms");
		return list;
	}
	
//	检测Netezza到TsDB更新后的数据量是否保持一致
	public static void compareUpdatedData(TsBlobDataTypeBaseObject tsType, List<String> perfIdList,int bufferSize) throws ParseException{
 		Map<String,Map<String,List<String>>> netezzaValueMap = new HashMap<String,Map<String,List<String>>>();
 		Map<String,Map<String,List<String>>> tsdbValueMap = new HashMap<String,Map<String,List<String>>>();		
		int i = 0;
		int size = perfIdList.size();
		String strTsDB = "";
		Double doubleTsDB = null;
		Double doubleNetezza = null;
		List<ArrayList<String>> perfIdContainer = new ArrayList<ArrayList<String>>();

//Table DBI_MARKETPRICE				
		switch(tsType){
		case tsMarketPrice:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketPrice =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsMarketPrice++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsMarketPrice*bufferSize,size - countTsMarketPrice*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsSpecialCashDividend:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSpecialCashDividend =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsSpecialCashDividend++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSpecialCashDividend*bufferSize,size - countTsSpecialCashDividend*bufferSize);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);			
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
		
		case tsDailyMarketReturnIndex:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsDailyMarketReturnIndex =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsDailyMarketReturnIndex++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsDailyMarketReturnIndex*bufferSize,size - countTsDailyMarketReturnIndex*bufferSize);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsTradingVolume:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsTradingVolume =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsTradingVolume++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsTradingVolume*bufferSize,size - countTsTradingVolume*bufferSize);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsCashDividend:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsCashDividend =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsCashDividend++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsCashDividend*bufferSize,size - countTsCashDividend*bufferSize);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);			
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsMarketBidOfferMidPrice:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_DailyMarketReturnIndex(SplitAdjusted)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_DailyMarketReturnIndex(SplitAdjusted)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of DailyMarketReturnIndex between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:SplitAdjusted--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketBidOfferMidPrice =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsMarketBidOfferMidPrice++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsMarketBidOfferMidPrice*bufferSize,size - countTsMarketBidOfferMidPrice*bufferSize);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = DailyMarketReturnIndexSplitAdjustedUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_DAILYMARKETRETURNINDEX");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;			
			default:
			break;
		}		
	}
	
	@SuppressWarnings("unchecked")
	private static List<ArrayList<String>> getSubList(List<String> perfIdListFull,int bufferSize){
		List<ArrayList<String>> listContainer = new ArrayList<ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		int flag = 0;
		int buffer = bufferSize;
		int count = 0;
		int size = perfIdListFull.size();
		for(String perfId:perfIdListFull){
			flag++;
			if(flag%buffer !=0){
				list.add(perfId);
			}else{
				list.add(perfId);
				ArrayList<String> sublist = new ArrayList<String>();
				sublist = (ArrayList<String>) list.clone();
				listContainer.add(sublist);
				list.clear();
				flag=0;
				count++;
			}
			if(size-count*buffer>0 && size-count*buffer<buffer){
				for(int j=count*buffer;j<size;j++){
					list.add(perfIdListFull.get(j));
				}
				listContainer.add(list);
				break;
			}
		}
		return listContainer;
	}
	
	private static List<String> getMissingDate(List<String> tsdbDateList,List<String> netezzaDateList) throws ParseException{
		List<String> resultList = new ArrayList<String>();
		List<String> tempList = new ArrayList<String>();
		Date newDate;
		boolean bl = false;
		for(String dateStr:tsdbDateList){
			newDate = df2.parse(dateStr);
			dateStr = df1.format(newDate);
			tempList.add(dateStr);
		}
		bl = netezzaDateList.removeAll(tempList);
		if(bl){
			resultList = netezzaDateList;
		}
		if(!tempList.isEmpty()){
			tempList.clear();
		}
		return resultList;
	}
	
	public static void main(String[] args) throws TSException, Exception{
		List<String> individualPerfIdtsMarketPriceList = new ArrayList<String>();
		List<String> individualPerfIdtsTradingVolumeList = new ArrayList<String>();
		List<String> individualPerfIdtsSpecialCashDividendList = new ArrayList<String>();
		List<String> individualPerfIdtsCashDividendList = new ArrayList<String>();
		List<String> individualPerfIdtsMarketBidOfferMidPriceList = new ArrayList<String>();
		
		String tableName = "DBI_DAILYMARKETRETURNINDEX";
		String createdOnTime = "2014-06-16";
		int limitCount = 200;
		List<String> performanceIdListEODModeET = new ArrayList<String>();
		List<String> performanceIdListEODModeCE = new ArrayList<String>();
		List<String> performanceIdListEODModeIN = new ArrayList<String>();
		List<String> performanceIdListEODModeEQUITY = new ArrayList<String>();

//EOD MODE
//Case1:ET		
		performanceIdListEODModeET = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime,"ET");
		if(!performanceIdListEODModeET.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketPrice,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradingVolume,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice,performanceIdListEODModeET,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,InvestmentType = 'DMRI-ETF',ignore this QA check!");
		}
//Case2:CE		
		performanceIdListEODModeCE = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime,"CE");
		if(!performanceIdListEODModeCE.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketPrice,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradingVolume,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice,performanceIdListEODModeET,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,InvestmentType = 'EMRI-CE',ignore this QA check!");
		}
//Case3:IN		
		performanceIdListEODModeIN = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime,"IN");
		if(!performanceIdListEODModeIN.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketPrice,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradingVolume,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice,performanceIdListEODModeET,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,InvestmentType = 'DMRI-Index',ignore this QA check!");
		}
//Case4:Equity
		performanceIdListEODModeEQUITY = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime,"EQUITY");
		if(!performanceIdListEODModeEQUITY.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketPrice,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradingVolume,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,performanceIdListEODModeET,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice,performanceIdListEODModeET,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,InvestmentType = 'DMRI-Equity',ignore this QA check!");
		}
		
//Individual PerformanceId mode
		individualPerfIdtsMarketPriceList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketPrice.TsType,limitCount);
		if(!individualPerfIdtsMarketPriceList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketPrice,individualPerfIdtsMarketPriceList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketPrice list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsTradingVolumeList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsTradingVolume.TsType,limitCount);
		if(!individualPerfIdtsTradingVolumeList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradingVolume,individualPerfIdtsTradingVolumeList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsTradingVolume list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpecialCashDividendList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpecialCashDividend.TsType,limitCount);
		if(!individualPerfIdtsSpecialCashDividendList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,individualPerfIdtsSpecialCashDividendList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpecialCashDividend list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsCashDividendList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsCashDividend.TsType,limitCount);
		if(!individualPerfIdtsCashDividendList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,individualPerfIdtsCashDividendList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsCashDividend list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsMarketBidOfferMidPriceList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice.TsType,limitCount);
		if(!individualPerfIdtsMarketBidOfferMidPriceList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice,individualPerfIdtsMarketBidOfferMidPriceList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketBidOfferMidPrice list is empty,ignore this QA check!");
		}
	}
}
