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
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.data.tsapi.TSException;

public class MarketPriceForEquityAndETFBaseObject {
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName1 = "MissingRecord_DBI_MarketPrice(BaseObject).log";
	private static String logName2 = "ContentChecking_DBI_MarketPrice(BaseObject).log";
	static SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");

	//利用AutoId来获取PerformanceId List
	private static List<String> getUpdatedPerformanceIdListByAutoId(String tableName,int AutoId) throws SQLException{
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
		String sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EX"+
								 " JOIN ADT_NOTIFICATIONDRIVER ND ON EX.EXCHANGEID = ND.ID AND EX.EFFECTIVEDATE = ND.STARTDATE "+
								 " WHERE EX.TABLENAME = ND.TABLENAME AND ND.TABLENAME = '" + tableName + "' AND ND.AUTOID = " + AutoId;
		list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza2);
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(Using AutoId to get),time cost: "+endTime+" ms");
		return list;
	}
	
//Get PerformanceIdList(EOD Mode)
	private static List<String> getUpdatedPerformanceIdListEODMode(String tableName,String createdOnTime) throws SQLException{
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//保留CREATEDON的使用
		if(createdOnTime.isEmpty()){
			createdOnTime = "2000-01-01";
		}
		String sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
								 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
								 " WHERE ND.IDTYPE = 2 AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
		list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza2);
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(EOD MODE),time cost: "+endTime+" ms");
		return list;
	}

//Get PerformanceIdList(Individual Mode)
	private static List<String> getUpdatedPerformanceIdListIndividualMode(String testedTableName,int TsType, int testedIdCounts){
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//选取IdType != 2的所有情况，且PROCESSNAME为DMRI for Equity或者DMRI for ETF才能保证是BASE OBJECT的		
		String sqlToGetUpdatedPerformanceIdList = "SELECT ID FROM ADT_NOTIFICATIONDRIVER WHERE PROCESSNAME IN('DMRI for Equity','DMRI for ETF') " +
				"AND IDTYPE != 2 AND TSTYPE = " + TsType + " AND TABLENAME = '" + testedTableName + "' LIMIT "+testedIdCounts;
		try {
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceIdList, Database.Netezza2);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got " + list.size() +" performanceids(Individual MODE), TSTYPE = "+ TsType + ",time cost: "+endTime+" ms");
		return list;
	}
	
//	检测Netezza到TsDB更新后的数据量是否保持一致
	public void compareUpdatedData(TsBlobDataTypeBaseObject tsType, List<String> perfIdList,int bufferSize) throws ParseException{		
		int i = 0;
		int size = perfIdList.size();
//	特殊逻辑，CurrencyId在被写回至TSDB时必须加此前缀
		String currencyPrefix = "CU$$$$$";
		String strTsDB = "";
		Double doubleTsDB = null;
		Double doubleNetezza = null;
		List<ArrayList<String>> perfIdContainer = new ArrayList<ArrayList<String>>();

//Table DBI_MARKETPRICE				
		switch(tsType){
		case tsMarketPrice:
			Map<String,Map<String,List<String>>> netezzaValueMap1 = new HashMap<String,Map<String,List<String>>>();
	 		Map<String,Map<String,List<String>>> tsdbValueMap1 = new HashMap<String,Map<String,List<String>>>();
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_MarketPrice(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_MarketPrice(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of tsMarketPrice between Netezza and TsDB");
			System.out.println("[INFO]Total Tested PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketPrice =0;

			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb1 = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza1 = new HashMap<String,List<String>>();
				List<String> valueListTsdb1 = new ArrayList<String>();
				List<String> valueListNetezza1 = new ArrayList<String>();
				
				countTsMarketPrice++;
//batch IDs testing once
				System.out.printf("[INFO]===============================  No.%d batch id group for tsMarketPrice  ===============================\n",countTsMarketPrice);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds TOTAL IN %d PerformanceIds \n", countTsMarketPrice*bufferSize,size);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap1 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);		
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap1 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
					System.out.println("[INFO]Retry to get " + tsType+" values from Netezza... ...");
					int reTryCount = 0;
					while(netezzaValueMap1.isEmpty()){
						try {
							netezzaValueMap1 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
						} catch (SQLException e1) {
							reTryCount++;
							if(reTryCount == 66){
								System.out.println("[INFO]Stop retrying to get " + tsType+" values from Netezza,66 times costed!");
								break;
							}
						}
					}
					if(!netezzaValueMap1.isEmpty()){
						System.out.println("[INFO]Success to get " + tsType+" values from Netezza,totally trying "+reTryCount+" times!");
					}
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap1.keySet()){					
					List<String> tsdbDateList1 = new ArrayList<String>();
					List<String> netezzaDateList1 = new ArrayList<String>();
					List<String> missDateList1 = new ArrayList<String>();
					dateValueListMapTsdb1 = tsdbValueMap1.get(keyPerfId);
					dateValueListMapNetezza1 = netezzaValueMap1.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb1 != null && !dateValueListMapTsdb1.isEmpty()){
						for(String dateStr:dateValueListMapTsdb1.keySet()){
							tsdbDateList1.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza1 != null && !dateValueListMapNetezza1.isEmpty()){
						for(String dateStr:dateValueListMapNetezza1.keySet()){
							netezzaDateList1.add(dateStr);
						}
					}
					if(tsdbDateList1!=null && netezzaDateList1!= null)
					missDateList1 = getMissingDate(tsdbDateList1,netezzaDateList1);
					if(!missDateList1.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList1){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
//Clear List					
					if(tsdbDateList1 != null && !tsdbDateList1.isEmpty()){
						tsdbDateList1.clear();
					}
					if(netezzaDateList1 != null && !netezzaDateList1.isEmpty()){
						netezzaDateList1.clear();
					}
					if(netezzaDateList1 != null && !missDateList1.isEmpty()){
						missDateList1.clear();
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap1.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb1 = tsdbValueMap1.get(keyPerfId);
						dateValueListMapNetezza1 = netezzaValueMap1.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza1.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(dateValueListMapTsdb1 != null){
								if(keyDate!=null && dateValueListMapTsdb1.containsKey(keyDate)){
									valueListTsdb1 =  dateValueListMapTsdb1.get(keyDate);
									if(valueListTsdb1.isEmpty() || valueListTsdb1 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}
									valueListNetezza1 = dateValueListMapNetezza1.get(keyDateOre);
									if(valueListNetezza1.isEmpty() || valueListNetezza1 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}else{
										for(String strNetezza:valueListNetezza1){
											strTsDB = valueListTsdb1.get(i);
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
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
				if(dateValueListMapTsdb1 != null  && !dateValueListMapTsdb1.isEmpty()){
					dateValueListMapTsdb1.clear();
					if(dateValueListMapTsdb1.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapTsdb1 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza1 != null && !dateValueListMapNetezza1.isEmpty()){
					dateValueListMapNetezza1.clear();
					if(dateValueListMapNetezza1.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapNetezza2 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListTsdb1 != null && !valueListTsdb1.isEmpty()){
					valueListTsdb1.clear();
					if(valueListTsdb1.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListTsdb1 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListNetezza1 != null && !valueListNetezza1.isEmpty()){
					valueListNetezza1.clear();
					if(valueListNetezza1.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListNetezza has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
			}			
			break;
			
		case tsMarketPriceCurrencyHistory:
			Map<String,Map<String,List<String>>> netezzaValueMap2 = new HashMap<String,Map<String,List<String>>>();
	 		Map<String,Map<String,List<String>>> tsdbValueMap2 = new HashMap<String,Map<String,List<String>>>();
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of tsMarketPriceCurrencyHistory between Netezza and TsDB");
			System.out.println("[INFO]Total Tested PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketPriceCurrencyHistory =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb2 = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza2 = new HashMap<String,List<String>>();
				List<String> valueListTsdb2 = new ArrayList<String>();
				List<String> valueListNetezza2 = new ArrayList<String>();
				
				countTsMarketPriceCurrencyHistory++;
//batch IDs testing once
				System.out.printf("[INFO]===============================  No.%d batch id group for tsMarketPriceCurrencyHistory  ===============================\n",countTsMarketPriceCurrencyHistory);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds TOTAL IN %d PerformanceIds \n", countTsMarketPriceCurrencyHistory*bufferSize,size);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap2 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap2 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
					System.out.println("[INFO]Retry to get " + tsType+" values from Netezza... ...");
					int reTryCount = 0;
					while(netezzaValueMap2.isEmpty()){
						try {
							netezzaValueMap2 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
						} catch (SQLException e1) {
							reTryCount++;
							if(reTryCount == 66){
								System.out.println("[INFO]Stop retrying to get " + tsType+" values from Netezza,66 times costed!");
								break;
							}
						}
					}
					if(!netezzaValueMap2.isEmpty()){
						System.out.println("[INFO]Success to get " + tsType+" values from Netezza,totally trying "+reTryCount+" times!");
					}
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap2.keySet()){					
					List<String> tsdbDateList2 = new ArrayList<String>();
					List<String> netezzaDateList2 = new ArrayList<String>();
					List<String> missDateList2 = new ArrayList<String>();
					dateValueListMapTsdb2 = tsdbValueMap2.get(keyPerfId);
					dateValueListMapNetezza2 = netezzaValueMap2.get(keyPerfId);
//Get tsdb date list
					if(tsdbDateList2!=null && netezzaDateList2!= null && !dateValueListMapTsdb2.isEmpty() && !netezzaDateList2.isEmpty()){
						for(String dateStr:dateValueListMapTsdb2.keySet()){
							tsdbDateList2.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza2 != null && !dateValueListMapNetezza2.isEmpty()){
						for(String dateStr:dateValueListMapNetezza2.keySet()){
							netezzaDateList2.add(dateStr);
						}
					}
					if(tsdbDateList2 != null && netezzaDateList2 != null)
					missDateList2 = getMissingDate(tsdbDateList2,netezzaDateList2);
					if(!missDateList2.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: " + keyPerfId);
						for(String strDate:missDateList2){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
//Clear List					
					if(tsdbDateList2 != null && !tsdbDateList2.isEmpty()){
						tsdbDateList2.clear();
					}
					if(netezzaDateList2 != null && !netezzaDateList2.isEmpty()){
						netezzaDateList2.clear();
					}
					if(missDateList2 != null && !missDateList2.isEmpty()){
						missDateList2.clear();
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap2.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb2 = tsdbValueMap2.get(keyPerfId);
						dateValueListMapNetezza2 = netezzaValueMap2.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza2.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下
							if(!keyDateOre.isEmpty());
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(dateValueListMapTsdb2 != null){
								if(keyDate!=null && dateValueListMapTsdb2.containsKey(keyDate)){
									valueListTsdb2 =  dateValueListMapTsdb2.get(keyDate);
									if(valueListTsdb2.isEmpty() || valueListTsdb2 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}
									valueListNetezza2 = dateValueListMapNetezza2.get(keyDateOre);
									if(valueListNetezza2.isEmpty() || valueListNetezza2 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}else{
										for(String strNetezza:valueListNetezza2){
											strTsDB = valueListTsdb2.get(0);
											strNetezza = currencyPrefix+strNetezza;
											if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
												if(!strNetezza.equals(strTsDB)){
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
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
				if(dateValueListMapTsdb2 != null && !dateValueListMapTsdb2.isEmpty()){
					dateValueListMapTsdb2.clear();
					if(dateValueListMapTsdb2.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapTsdb2 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza2 != null && !dateValueListMapNetezza2.isEmpty()){
					dateValueListMapNetezza2.clear();
					if(dateValueListMapNetezza2.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapNetezza2 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza2 != null && !valueListTsdb2.isEmpty()){
					valueListTsdb2.clear();
					if(valueListTsdb2.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListTsdb2 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListNetezza2 != null && !valueListNetezza2.isEmpty()){
					valueListNetezza2.clear();
					if(valueListNetezza2.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListNetezza2 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
			}			
			break;
			
		case tsMarketPriceCopyOver:
			Map<String,Map<String,List<String>>> netezzaValueMap3 = new HashMap<String,Map<String,List<String>>>();
	 		Map<String,Map<String,List<String>>> tsdbValueMap3 = new HashMap<String,Map<String,List<String>>>();
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of tsMarketPriceCopyOver between Netezza and TsDB");
			System.out.println("[INFO]Total Tested PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketPriceCopyOver =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb3 = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza3 = new HashMap<String,List<String>>();
				List<String> valueListTsdb3 = new ArrayList<String>();
				List<String> valueListNetezza3 = new ArrayList<String>();
				
				countTsMarketPriceCopyOver++;
//batch IDs testing once
				System.out.printf("[INFO]===============================  No.%d batch id group for tsMarketPriceCopyOver  ===============================\n",countTsMarketPriceCopyOver);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds TOTAL IN %d PerformanceIds \n", countTsMarketPriceCopyOver*bufferSize,size);
				long startTime1 = System.currentTimeMillis();		
				tsdbValueMap3 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap3 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
					System.out.println("[INFO]Retry to get " + tsType+" values from Netezza... ...");
					int reTryCount = 0;
					while(netezzaValueMap3.isEmpty()){
						try {
							netezzaValueMap3 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
						} catch (SQLException e1) {
							reTryCount++;
							if(reTryCount == 66){
								System.out.println("[INFO]Stop retrying to get " + tsType+" values from Netezza,66 times costed!");
								break;
							}
						}
					}
					if(!netezzaValueMap3.isEmpty()){
						System.out.println("[INFO]Success to get " + tsType+" values from Netezza,totally trying "+reTryCount+" times!");
					}
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap3.keySet()){
					List<String> tsdbDateList3 = new ArrayList<String>();
					List<String> netezzaDateList3 = new ArrayList<String>();
					List<String> missDateList3 = new ArrayList<String>();
					dateValueListMapTsdb3 = tsdbValueMap3.get(keyPerfId);
					dateValueListMapNetezza3 = netezzaValueMap3.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb3 != null && !dateValueListMapTsdb3.isEmpty()){
						for(String dateStr:dateValueListMapTsdb3.keySet()){
							tsdbDateList3.add(dateStr);
						}
					}					
//Get netezza date list
					if(dateValueListMapNetezza3 != null && !dateValueListMapNetezza3.isEmpty()){
						for(String dateStr:dateValueListMapNetezza3.keySet()){
							netezzaDateList3.add(dateStr);
						}
					}
					if(tsdbDateList3!=null && netezzaDateList3!= null)
					missDateList3 = getMissingDate(tsdbDateList3,netezzaDateList3);
					if(!missDateList3.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList3){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
//Clear List					
					if(tsdbDateList3 != null && !tsdbDateList3.isEmpty()){
						tsdbDateList3.clear();
					}
					if(netezzaDateList3 != null && !netezzaDateList3.isEmpty()){
						netezzaDateList3.clear();
					}
					if(missDateList3 != null && !missDateList3.isEmpty()){
						missDateList3.clear();
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap3.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb3 = tsdbValueMap3.get(keyPerfId);
						dateValueListMapNetezza3 = netezzaValueMap3.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza3.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(dateValueListMapTsdb3 != null){
								if(keyDate!=null && dateValueListMapTsdb3.containsKey(keyDate)){
									valueListTsdb3 =  dateValueListMapTsdb3.get(keyDate);
									if(valueListTsdb3.isEmpty() || valueListTsdb3 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}
									valueListNetezza3 = dateValueListMapNetezza3.get(keyDateOre);
									if(valueListNetezza3.isEmpty() || valueListNetezza3 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}else{
										for(String strNetezza:valueListNetezza3){
											strTsDB = valueListTsdb3.get(i);
//处理比较特殊，需要先判断是否为带有日期信息的字符串
											if(strTsDB.contains("-")){
												Date dateNetezza = df2.parse(strNetezza);
												Date dateTsDB = df2.parse(strTsDB);
												if(!dateNetezza.equals(dateTsDB)){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}else if(strNetezza.equals("NullValue")){
													if(!strTsDB.equals("NaN")){
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
														CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												   }
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
											}else{
												if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
//全部转换成小数点后加一位的样式						
													doubleTsDB =  Double.valueOf(strTsDB);
													strTsDB = Helper.addZeroForDouble(doubleTsDB, "0.0");
													doubleNetezza = Double.valueOf(strNetezza);
													strNetezza = Helper.addZeroForDouble(doubleNetezza, "0.0");
													if(!strNetezza.equals(strTsDB)){
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
											i++;
										}
										i=0;
									}				
								}
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
				if(dateValueListMapTsdb3 != null && !dateValueListMapTsdb3.isEmpty()){
					dateValueListMapTsdb3.clear();
					if(dateValueListMapTsdb3.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapTsdb3 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza3 != null && !dateValueListMapNetezza3.isEmpty()){
					dateValueListMapNetezza3.clear();
					if(dateValueListMapNetezza3.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapNetezza3 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListTsdb3 != null && !valueListTsdb3.isEmpty()){
					valueListTsdb3.clear();
					if(valueListTsdb3.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListTsdb3 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListNetezza3 != null && !valueListNetezza3.isEmpty()){
					valueListNetezza3.clear();
					if(valueListNetezza3.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListNetezza3 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
			}			
			break;
			
		case tsMarketBidOfferMidPrice:
			Map<String,Map<String,List<String>>> netezzaValueMap4 = new HashMap<String,Map<String,List<String>>>();
	 		Map<String,Map<String,List<String>>> tsdbValueMap4 = new HashMap<String,Map<String,List<String>>>();
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of tsMarketBidOfferMidPrice between Netezza and TsDB");
			System.out.println("[INFO]Total Tested PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsMarketBidOfferMidPrice =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb4 = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza4 = new HashMap<String,List<String>>();
				List<String> valueListTsdb4 = new ArrayList<String>();
				List<String> valueListNetezza4 = new ArrayList<String>();
				
				countTsMarketBidOfferMidPrice++;
//batch IDs testing once
				System.out.printf("[INFO]===============================  No.%d batch id group for tsMarketBidOfferMidPrice  ===============================\n",countTsMarketBidOfferMidPrice);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds TOTAL IN %d PerformanceIds \n", countTsMarketBidOfferMidPrice*bufferSize,size);
				long startTime1 = System.currentTimeMillis();
				tsdbValueMap4 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap4 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
					System.out.println("[INFO]Retry to get " + tsType+" values from Netezza... ...");
					int reTryCount = 0;
					while(netezzaValueMap4.isEmpty()){
						try {
							netezzaValueMap4 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
						} catch (SQLException e1) {
							reTryCount++;
							if(reTryCount == 66){
								System.out.println("[INFO]Stop retrying to get " + tsType+" values from Netezza,66 times costed!");
								break;
							}
						}
					}
					if(!netezzaValueMap4.isEmpty()){
						System.out.println("[INFO]Success to get " + tsType+" values from Netezza,totally trying "+reTryCount+" times!");
					}
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap4.keySet()){
					List<String> tsdbDateList4 = new ArrayList<String>();
					List<String> netezzaDateList4 = new ArrayList<String>();
					List<String> missDateList4 = new ArrayList<String>();
					dateValueListMapTsdb4 = tsdbValueMap4.get(keyPerfId);
					dateValueListMapNetezza4 = netezzaValueMap4.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb4 != null && !dateValueListMapTsdb4.isEmpty()){
						for(String dateStr:dateValueListMapTsdb4.keySet()){
							tsdbDateList4.add(dateStr);
						}
					}					
//Get netezza date list
					if(dateValueListMapNetezza4 != null && !dateValueListMapNetezza4.isEmpty()){
						for(String dateStr:dateValueListMapNetezza4.keySet()){
							netezzaDateList4.add(dateStr);
						}
					}
					if(tsdbDateList4!=null && netezzaDateList4!= null )
					missDateList4 = getMissingDate(tsdbDateList4,netezzaDateList4);
					if(!missDateList4.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList4){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
//Clear List					
					if(tsdbDateList4 != null && !tsdbDateList4.isEmpty()){
						tsdbDateList4.clear();
					}
					if(netezzaDateList4 != null && !netezzaDateList4.isEmpty()){
						netezzaDateList4.clear();
					}
					if(missDateList4 != null && !missDateList4.isEmpty()){
						missDateList4.clear();
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap4.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb4 = tsdbValueMap4.get(keyPerfId);
						dateValueListMapNetezza4 = netezzaValueMap4.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza4.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(dateValueListMapTsdb4 != null){
								if(keyDate!=null && dateValueListMapTsdb4.containsKey(keyDate)){
									valueListTsdb4 =  dateValueListMapTsdb4.get(keyDate);
									if(valueListTsdb4.isEmpty() || valueListTsdb4 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}
									valueListNetezza4 = dateValueListMapNetezza4.get(keyDateOre);
									if(valueListNetezza4.isEmpty() || valueListNetezza4 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}else{
										for(String strNetezza:valueListNetezza4){
											strTsDB = valueListTsdb4.get(i);
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
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
				if(dateValueListMapTsdb4 != null && !dateValueListMapTsdb4.isEmpty()){
					dateValueListMapTsdb4.clear();
					if(dateValueListMapTsdb4.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapTsdb4 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza4 != null && !dateValueListMapNetezza4.isEmpty()){
					dateValueListMapNetezza4.clear();
					if(dateValueListMapNetezza4.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapNetezza4 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListTsdb4 != null && !valueListTsdb4.isEmpty()){
					valueListTsdb4.clear();
					if(valueListTsdb4.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListTsdb4 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListNetezza4 != null && !valueListNetezza4.isEmpty()){
					valueListNetezza4.clear();
					if(valueListNetezza4.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListNetezza4 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
			}			
			break;
			
		case tsTradingVolume:
			Map<String,Map<String,List<String>>> netezzaValueMap5 = new HashMap<String,Map<String,List<String>>>();
	 		Map<String,Map<String,List<String>>> tsdbValueMap5 = new HashMap<String,Map<String,List<String>>>();
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of tsTradingVolume between Netezza and TsDB");
			System.out.println("[INFO]Total Tested PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsTradingVolume =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb5 = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza5 = new HashMap<String,List<String>>();
				List<String> valueListTsdb5 = new ArrayList<String>();
				List<String> valueListNetezza5 = new ArrayList<String>();
				
				countTsTradingVolume++;
//batch IDs testing once
				System.out.printf("[INFO]===============================  No.%d batch id group for tsTradingVolume  ===============================\n",countTsTradingVolume);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds TOTAL IN %d PerformanceIds \n", countTsTradingVolume*bufferSize,size);
				long startTime1 = System.currentTimeMillis();
			    tsdbValueMap5 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap5 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
					System.out.println("[INFO]Retry to get " + tsType+" values from Netezza... ...");
					int reTryCount = 0;
					while(netezzaValueMap5.isEmpty()){
						try {
							netezzaValueMap5 = MarketPriceForEquityAndETFBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza2, sublist, "DBI_MARKETPRICE");
						} catch (SQLException e1) {
							reTryCount++;
							if(reTryCount == 66){
								System.out.println("[INFO]Stop retrying to get " + tsType+" values from Netezza,66 times costed!");
								break;
							}
						}
					}
					if(!netezzaValueMap5.isEmpty()){
						System.out.println("[INFO]Success to get " + tsType+" values from Netezza,totally trying "+reTryCount+" times!");
					}
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap5.keySet()){
					List<String> tsdbDateList5 = new ArrayList<String>();
					List<String> netezzaDateList5 = new ArrayList<String>();
					List<String> missDateList5 = new ArrayList<String>();
					dateValueListMapTsdb5 = tsdbValueMap5.get(keyPerfId);
					dateValueListMapNetezza5 = netezzaValueMap5.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb5 != null && !dateValueListMapTsdb5.isEmpty()){
						for(String dateStr:dateValueListMapTsdb5.keySet()){
							tsdbDateList5.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza5 != null && !dateValueListMapNetezza5.isEmpty()){
						for(String dateStr:dateValueListMapNetezza5.keySet()){
							netezzaDateList5.add(dateStr);
						}
					}
					if(tsdbDateList5!=null && netezzaDateList5!= null )
						missDateList5 = getMissingDate(tsdbDateList5,netezzaDateList5);
					if(!missDateList5.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: " + keyPerfId);
						for(String strDate:missDateList5){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
//Clear List					
					if(tsdbDateList5 != null && !tsdbDateList5.isEmpty()){
						tsdbDateList5.clear();
					}
					if(netezzaDateList5 != null && !netezzaDateList5.isEmpty()){
						netezzaDateList5.clear();
					}
					if(missDateList5 != null && !missDateList5.isEmpty()){
						missDateList5.clear();
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap5.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb5 = tsdbValueMap5.get(keyPerfId);
						dateValueListMapNetezza5 = netezzaValueMap5.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza5.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(dateValueListMapTsdb5 != null){
								if(keyDate!=null && dateValueListMapTsdb5.containsKey(keyDate)){
									valueListTsdb5 =  dateValueListMapTsdb5.get(keyDate);
									if(valueListTsdb5.isEmpty() || valueListTsdb5 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}
									valueListNetezza5 = dateValueListMapNetezza5.get(keyDateOre);
									if(valueListNetezza5.isEmpty() || valueListNetezza5 == null){
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
										CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
									}else{
										for(String strNetezza:valueListNetezza5){
											strTsDB = valueListTsdb5.get(0);
											if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
												if(!strNetezza.equals(strTsDB)){
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
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
				if(dateValueListMapTsdb5 != null && !dateValueListMapTsdb5.isEmpty()){
					dateValueListMapTsdb5.clear();
					if(dateValueListMapTsdb5.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapTsdb5 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(dateValueListMapNetezza5 != null && !dateValueListMapNetezza5.isEmpty()){
					dateValueListMapNetezza5.clear();
					if(dateValueListMapNetezza5.isEmpty()){
						System.out.println("[INFO]MAP<=>dateValueListMapNetezza5 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListTsdb5 != null && !valueListTsdb5.isEmpty()){
					valueListTsdb5.clear();
					if(valueListTsdb5.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListTsdb5 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
				if(valueListNetezza5 != null && !valueListNetezza5.isEmpty()){
					valueListNetezza5.clear();
					if(valueListNetezza5.isEmpty()){
						System.out.println("[INFO]LIST<=>valueListNetezza5 has cleared!TsType="+tsType+" >>>"+Thread.currentThread().getName());
					}
				}
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
	
	private static void testEODmode(String utilClassName,String tableName,String createdOnTime,int bufferSize){
		List<String> performanceIdListEODMode = new ArrayList<String>();
		try {
			performanceIdListEODMode = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(!performanceIdListEODMode.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCurrencyHistory, performanceIdListEODMode, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPrice, performanceIdListEODMode, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCopyOver, performanceIdListEODMode, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice, performanceIdListEODMode, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsTradingVolume, performanceIdListEODMode, bufferSize);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,ignore this QA check!");
		}
//shut down thread pool
		MultyThreadRunner.shutDownThreadPool();
	}
	
	private static void testAutoIdMode(String utilClassName,String tableName,int AutoId,int bufferSize){
		List<String> performanceIdListByAutoId = new ArrayList<String>();
		try {
			performanceIdListByAutoId = getUpdatedPerformanceIdListByAutoId(tableName,AutoId);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(!performanceIdListByAutoId.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCurrencyHistory, performanceIdListByAutoId, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPrice, performanceIdListByAutoId, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCopyOver, performanceIdListByAutoId, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice, performanceIdListByAutoId, bufferSize);
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsTradingVolume, performanceIdListByAutoId, bufferSize);
			MultyThreadRunner.shutDownThreadPool();
		}else{
			System.out.println("[WARN]PerformanceId(Using AutoId to get) list is empty,ignore this QA check!");
		}
//shut down thread pool
			MultyThreadRunner.shutDownThreadPool();
	}
	
	private static void testIndividualMode(String utilClassName,String tableName,int bufferSize){
		List<String> individualPerfIdtsMarketPriceList = new ArrayList<String>();
		List<String> individualPerfIdtsMarketPriceCurrencyHistoryList = new ArrayList<String>();
		List<String> individualPerfIdtsMarketPriceCopyOverList = new ArrayList<String>();
		List<String> individualPerfIdtsMarketBidOfferMidPriceList = new ArrayList<String>();
		List<String> individualPerfIdtsTradingVolumeList = new ArrayList<String>();
		
		individualPerfIdtsMarketPriceList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketPrice.TsType,bufferSize);
		if(!individualPerfIdtsMarketPriceList.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPrice, individualPerfIdtsMarketPriceList, bufferSize);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketPrice list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsMarketPriceCurrencyHistoryList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketPriceCurrencyHistory.TsType,bufferSize);
		if(!individualPerfIdtsMarketPriceCurrencyHistoryList.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCurrencyHistory, individualPerfIdtsMarketPriceList, bufferSize);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketPriceCurrencyHistory list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsMarketPriceCopyOverList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketPriceCopyOver.TsType,bufferSize);
		if(!individualPerfIdtsMarketPriceCopyOverList.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketPriceCopyOver, individualPerfIdtsMarketPriceList, bufferSize);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketPriceCopyOver list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsMarketBidOfferMidPriceList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice.TsType,bufferSize);
		if(!individualPerfIdtsMarketBidOfferMidPriceList.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsMarketBidOfferMidPrice, individualPerfIdtsMarketPriceList, bufferSize);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketBidOfferMidPrice list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsTradingVolumeList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsTradingVolume.TsType,bufferSize);
		if(!individualPerfIdtsTradingVolumeList.isEmpty()){
			MultyThreadRunner.startRunning(utilClassName, TsBlobDataTypeBaseObject.tsTradingVolume, individualPerfIdtsMarketPriceList, bufferSize);
		}else{
			System.out.println("[WARN]Individual PerfId TsTradingVolume list is empty,ignore this QA check!");
		}
//shut down thread pool
		MultyThreadRunner.shutDownThreadPool();
	}
	
	public static void main(String[] args) throws TSException, Exception{		
		String utilClassName = "MarketPriceForEquityAndETFBaseObject";
		String tableName = "DBI_MARKETPRICE";
		String createdOnTime = "2014-07-28";
		int bufferSize = 50;
		
//		int AutoId = 222841792;
//		int AutoId = 222841813;
//		int AutoId = 222943543;
//		int AutoId = 222943659;
//		int AutoId = 222943706;
//		int AutoId = 222844897;
//		int AutoId = 222845194;
//		int AutoId = 222845703;
		
		int AutoId = 222963051;
		
		
//Using AutoId to get updated PerformanceId
		testAutoIdMode(utilClassName, tableName, AutoId, bufferSize);
		
//EOD MODE
//		testEODmode(utilClassName, tableName, createdOnTime, bufferSize);
		
//Individual PerformanceId mode
//		testIndividualMode(utilClassName, createdOnTime, bufferSize);
	}
}
