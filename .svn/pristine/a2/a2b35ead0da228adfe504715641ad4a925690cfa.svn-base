package com.morningstar.FundAutoTest.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ResourceManager {

	private static Properties props;
	public static String serverSite;
//异常捕获，读取resourceBundle.properties配置文件
	static {
		try {
			props = load(new File("config/resourceBundle.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//IO字节流读取属性时的异常捕获	
	public static Properties load(File propsFile) throws IOException {
		props = new Properties();
		FileInputStream fis = new FileInputStream(propsFile);
		props.load(fis);
		fis.close();
		return props;
	}
//以下为获取resourceBundle.properties配置文件中的对应关键字相关信息
//trim（）方法是一个简单格式化函数	
	public static String gettoken() {
		return props.getProperty("token").trim();
	}

	public static String getDevToken() {
		return props.getProperty("devToken").trim();
	}
	
	public static String getQAToken() {
		return props.getProperty("QAToken").trim();
	}
	
	public static String getEmail() {
		return props.getProperty("email").trim();
	}
	
	public static String getSite() {
		if (serverSite != null)  return serverSite.trim();
		else return props.getProperty("site").trim();
	}
	
	public static String getTokenSite() {
		return props.getProperty("token_site").trim();
	}
	
	public static String getDevSite() {
		return props.getProperty("DevSite").trim();
	}

	public static String getLiveSite() {
		if (serverSite != null)  return serverSite.trim();
		else return props.getProperty("LiveSite").trim();
	}

	public static String getQASite() {
		return props.getProperty("QASite").trim();
	}

	public static String getUATSite() {
		return props.getProperty("UATSite").trim();
	}
	
	public static String getCodeMappingURL() {
		return props.getProperty("codeMappingURL").trim();
	}
	
	public static String getIDServiceURL() {
		return props.getProperty("IDServiceURL").trim();
	}
	
	public static String getQuickTestngXML() {
		return props.getProperty("quickTestXML").trim();
	}
	
	public static String getSlowTestngXML() {
		return props.getProperty("slowTestXML").trim();
	}
	
	public static String getNormalTestXML() {
		return props.getProperty("normalTestXML").trim();
	}
	
	public static String getTimeZone() {
		return props.getProperty("timeZone").trim();
	}

	public static String getFailedXMLPath() {
		return props.getProperty("failedXMLPath").trim();
	}

	public static String getTokeLoginEmail() {
		return props.getProperty("token.login.email").trim();
	}

	public static String getTokeLoginPassword() {
		return props.getProperty("token.login.password").trim();
	}

	public static String getRemoteUser() {
		return props.getProperty("remote.user").trim();
	}

	public static String getRemotePassword() {
		return props.getProperty("remote.password").trim();
	}

	public static String getRemoteUrlGexoifs61() {
		return props.getProperty("remote.url.gexoifs61").trim();
	}
	
	public static String getRemoteUrl4GEAPI() {
		return props.getProperty("remote.url.4GEAPI").trim();
	}

	public static String getMysqlDriverClass() {
		return props.getProperty("mysql.connection.driverclass").trim();
	}

	public static String getMysqlUrl() {
		return props.getProperty("mysql.connection.url").trim();
	}

	public static String getMysqlUsername() {
		return props.getProperty("mysql.connection.username").trim();
	}

	public static String getMysqlPassword() {
		return props.getProperty("mysql.connection.password").trim();
	}

	public static int getMysqlPoolSize() {
		return Integer.valueOf(props.getProperty("mysql.connection.poolSize").trim());
	}

	public static String getMssqlDriverClass() {
		return props.getProperty("mssql.connection.driverclass").trim();
	}

//	新增Stefan.Hou
	public static String getMssqlUrl() {
		return props.getProperty("mssql.connection.url").trim();
	}
	
	public static String getMssqlPassword(){
		return props.getProperty("mssql.connection.password").trim();
	}
	
	public static String getMssqlUsername(){
		return props.getProperty("mssql.connection.username").trim();
	}
	
	public static String getVerticaDriverClass() {
		return props.getProperty("Vertica.connection.driverclass").trim();
	}
	
	public static String getVerticaUrl() {
		return props.getProperty("Vertica.connection.url").trim();
	}
	
	public static String getVerticaUsername() {
		return props.getProperty("Vertica.connection.username").trim();
	}
	
	public static String getVerticaPassword() {
		return props.getProperty("Vertica.connection.password").trim();
	}
	
	public static String getVerticaPoolSize() {
		return props.getProperty("Vertica.connection.poolSize").trim();
	}

	
	public static String getGeproddb61Url() {
		return props.getProperty("geproddb61.connection.url").trim();
	}
	
	public static String getGedevdb91Url() {
		return props.getProperty("gedevdb91.connection.url").trim();
	}
	
	public static String getGeoutputdb61Url() {
		return props.getProperty("geoutputdb61.connection.url").trim();
	}
	
	public static String getMsOutputDb3Url() {
		return props.getProperty("MsOutputDb3.connection.url").trim();
	}
	
	public static String getMsOutputDb3User() {
		return props.getProperty("mssql.MsOutputDb3.username").trim();
	}
	
	public static String getMsOutputDb3Password() {
		return props.getProperty("mssql.MsOutputDb3.password").trim();
	}

	public static int getMssqlPoolSize() {
		return Integer.valueOf(props.getProperty("mssql.connection.poolSize").trim());
	}

	public static int getMinConnectionsPerPartition() {
		return Integer.valueOf(props.getProperty("minConnectionsPerPartition").trim());
	}

	public static int getMaxConnectionsPerPartition() {
		return Integer.valueOf(props.getProperty("maxConnectionsPerPartition").trim());
	}

	public static int getPartitionCount() {
		return Integer.valueOf(props.getProperty("partitionCount").trim());
	}

	public static String getResultWarning() {
		return props.getProperty("resultWarning").trim();
	}

	public static String getResultError() {
		return props.getProperty("resultError").trim();
	}

	public static String getMailTo() {
		return props.getProperty("mail_to").trim();
	}

	public static String getMailFrom() {
		return props.getProperty("mail_from").trim();
	}

	public static String getMailHost() {
		return props.getProperty("mail_host").trim();
	}

	public static String getXoi_user() {
		return props.getProperty("xoi_user").trim();
	}

	public static String getXoi_password() {
		return props.getProperty("xoi_password").trim();
	}

	public static String getGIDLoginUrl() {
		return props.getProperty("GIDLoginUrl").trim() + "email=" + getXoi_user() + "&password=" + getXoi_password();
	}

	public static String getGIDLink() {
		return props.getProperty("GIDLink").trim();
	}
	
	public static String getCompanyOperationBackLink() {
		return props.getProperty("remote.url.CompanyOperation.back").trim();
	}

}
