package com.morningstar.test.httpclient;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

public class Auth {

	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		String host = "http://xoibeta.morningstar.com/XOISuite/IdList.aspx?Package=DataBank&Option=RecvMsgs_Start&ProductGroup=TSProduction";
		String unm = "TSProduction@morningstar.com";
		String pwd = "TSRocks!";
		credsProvider.setCredentials(new AuthScope(host, AuthScope.ANY_PORT),
		new UsernamePasswordCredentials(unm, pwd));
		
//		credsProvider.setCredentials(new AuthScope(host, 8080),
//		new UsernamePasswordCredentials("u2", "p2"));
//		
//		credsProvider.setCredentials(new AuthScope(host, 8080, AuthScope.ANY_REALM, "ntlm"),
//		new UsernamePasswordCredentials("u3", "p3"));
		
		System.out.println(credsProvider.getCredentials(
		new AuthScope(host, 80, "realm", "basic")));
		
		System.out.println(credsProvider.getCredentials(
		new AuthScope(host, 8080, "realm", "basic")));
		
		System.out.println(credsProvider.getCredentials(
		new AuthScope(host, 8080, null, "ntlm")));
	}
}
