package com.morningstar.test.httpclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HttpPostMethod {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		String url ="http://localhost:8080/myDemo/Ajax/serivceJ.action";
//创建默认的httpClient实例.
		HttpClient httpclient = new DefaultHttpClient();
//创建httpPost
		HttpPost httppost = new HttpPost(url);
//创建参数队列,使用NameValuePair
		List<NameValuePair> formparams = new ArrayList<NameValuePair>();
		formparams.add(new BasicNameValuePair("type", "house"));
		UrlEncodedFormEntity uefEntity;
			
			try {
				uefEntity = new UrlEncodedFormEntity(formparams, "UTF-8");
				httppost.setEntity(uefEntity);
				System.out.println("executing request " + httppost.getURI());
				HttpResponse response;
				response = httpclient.execute(httppost);
				HttpEntity entity = response.getEntity();
				if (entity != null) {		
					System.out.println("--------------------------------------");
				}
				System.out.println("Response content: " + EntityUtils.toString(entity,"UTF-8"));
				System.out.println("--------------------------------------");
			} catch (ClientProtocolException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
}
