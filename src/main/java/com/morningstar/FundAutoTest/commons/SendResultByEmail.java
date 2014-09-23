package com.morningstar.FundAutoTest.commons;

import java.util.List;


import com.morningstar.ge.mail.MailInfo;
import com.morningstar.ge.mail.MailSender;

import com.morningstar.FundAutoTest.PathUtil;
import com.morningstar.FundAutoTest.commons.testbase.Base;
import com.morningstar.FundAutoTest.commons.testbase.DotTestListener;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class SendResultByEmail {
	// Parameters for send email
	public static String sendMailServerHost;
	public static String sendMailServerPort;
	public static boolean validate = true;
	public static String sendUserName;
	public static String sendPassword;
	public static String fromAddress;
	public static String toAddresses;
	public static String mailSubject;
	public static String mailBody;
//	public static String url=Base.baseUrl;
	public static String resultPath;
	protected static int failed;
	protected static int skipped;
	protected static int totalCase;
	protected static int passed;
	protected static double failedP;
	protected static double skippedP;
	protected static double passedP;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	String time = "Test time:" + sdf.format(new java.util.Date());


	// method for send results by email
	public void sendMail(DotTestListener listener, DotTestListener fListener, String round, String resultOutput) {
//邮件参数初始化	
		setmailSenderParam(round);
		MailInfo mailInfo = new MailInfo();
		
		setParemeters(listener, fListener);
		setMailSubject(round);
		setMailBody();
		
		mailInfo.setMailServerHost(sendMailServerHost);
		mailInfo.setMailServerPort(sendMailServerPort);
		mailInfo.setValidate(validate);
		mailInfo.setUserName(sendUserName);
		mailInfo.setPassword(sendPassword);
		mailInfo.setFromAddress(fromAddress);
		mailInfo.setToAddresses(toAddresses);
		mailInfo.setSubject(mailSubject);
		mailInfo.setContent(mailBody);
		if (passedP < 80 && round.equals("2")) {
			mailInfo.setImportant(true);
		}

		mailInfo.setAttachFileNames(new String[] {
				PathUtil.getAbsolutePath(resultOutput + "/testng-results.xml"),
				PathUtil.getAbsolutePath("log/run.log") });

		MailSender.sendHtmlMail(mailInfo);

	}
	
	private void setParemeters(DotTestListener listener, DotTestListener fListener){
		failed = fListener.getFailed();
		skipped = fListener.getSkipped();
		totalCase = listener.getPassed() + listener.getFailed() + listener.getSkipped();
		passed=totalCase-skipped-failed;
//后缀带p的为百分比，强制转换成Double类型		
		failedP = ((double) failed / totalCase) * 100;
		skippedP = ((double) skipped / totalCase) * 100;
		passedP = 100 - skippedP - failedP;
	}

//邮件标题	
	private void setMailSubject(String round){
		
		if(round.equals("1"))
			mailSubject = "Fund Automation test result";
		
		if(round.equals("2"))
			mailSubject = "Round 2 test result of Fund Automation test" ;
	}

//设置邮件内容,使用了java中的Text的DecimalFormat的方法来设置小数点位数	
	private void setMailBody(){
		DecimalFormat df = new DecimalFormat("0.00");
		mailBody = time + "<br>" 
//				+ "Tested URL:  " + url + "<br>" 
				+ Base.runFrequency + "<br>" 
				+ "Total tests run:  " + totalCase + "<br>"
				+ "Passed:  " + passed + "<br>" 
				+ "Failed:  " + failed + "<br>"
				+ "Skipped:  " + skipped + "<br>" 
				+ "Passed Percentage:  "+ df.format(passedP) + "%" + "<br>" 
				+ "Failed Percentage:  " + df.format(failedP)+ "%" + "<br>" 
				+ "Skipped Percentage:  " + df.format(skippedP) + "%"+ "<br>";
	}
//设置邮件发送者信息，round代表测试执行轮数	
	private void setmailSenderParam(String round) {
		if(round.equals("1"))
			loadMailParam(1);
		
		if(round.equals("2"))
			loadMailParam(2);
		
		
	}
// 从emailConfiguration.xlsx获取邮件相关配置信息	
	private static void loadMailParam(int i) {
		try {
			List<List<String>> info = ExcelUtil.getCellData("/config/emailConfiguration.xlsx", "Sheet1");
			
				sendMailServerHost = info.get(i).get(0);
				sendMailServerPort = info.get(i).get(1);
//将从Excel中读取到的字符串型True或者False转换为Boolean型数据
				validate = Boolean.valueOf(info.get(i).get(2));
				sendUserName = info.get(i).get(3);
				sendPassword = info.get(i).get(4);
				fromAddress = info.get(i).get(5);
				toAddresses = info.get(i).get(6);

			
		} catch (Exception e) {

		}
	}
}
