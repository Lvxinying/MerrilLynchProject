package com.morningstar.FundAutoTest;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class HttpConnection {

	public static InputStream getInputStream(String url) {
		try {
			return getConnection(url).getInputStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static HttpURLConnection getConnection(String url) {
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) initURL(url).openConnection();
			conn.setRequestMethod("GET");
			setTimeout(conn);
			conn.connect();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static InputStream getGIDInputStream(String loginUrl, String link) {
		HttpURLConnection conn = null;
		InputStream in = null;
		String[] cookie = null;
		String cookie_GIDDOMAINAUTH = "";

		conn = getConnection(loginUrl);
		cookie = conn.getHeaderField("Set-Cookie").split(";");
		for (int i = 0; i < cookie.length; i++) {
			if (cookie[i].contains(".GIDDOMAINAUTH"))
				cookie_GIDDOMAINAUTH = cookie[i];
		}

		HttpURLConnection conn1 = null;

		try {
			conn1 = (HttpURLConnection) initURL(link).openConnection();
			setTimeout(conn1);
			conn1.setRequestProperty("Accept-Encoding", "gzip,deflate,sdch");
			conn1.setRequestProperty("Cookie", cookie_GIDDOMAINAUTH);
			conn1.connect();

			in = conn1.getInputStream();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();

		} finally {
			conn.disconnect();
		}
		return in;
	}

	public static InputStream getPriceXOIInputStream(String url, String token) {
		HttpURLConnection conn = null;
		HttpURLConnection conn1 = null;
		String[] cook = null;

		try {
			// 302 redirect
			conn = (HttpURLConnection) initURL(url).openConnection();
			setTimeout(conn);
			conn.setInstanceFollowRedirects(false);
			conn.setRequestProperty("Authorization", "Basic " + token);
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("Accept-Encoding", "gzip,deflate");
			conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
			conn.connect();
			cook = conn.getHeaderField("Set-Cookie").split(";");
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}

		try {
			conn1 = (HttpURLConnection) initURL(url).openConnection();
			setTimeout(conn1);
			conn1.setRequestProperty("Accept-Charset", "utf-8");
			conn1.setRequestProperty("Accept-Encoding", "gzip,deflate");
			String XoiLogin = cook[0];
			conn1.setRequestProperty("Cookie", XoiLogin);
			conn1.connect();

			if (conn1.getResponseCode() != 200)
				return null;
			else {
				if ("gzip".equals(conn1.getContentEncoding()))
					return new GZIPInputStream(conn1.getInputStream());
				return conn1.getInputStream();
			}
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();

		} finally {
			conn.disconnect();
		}

		return null;
	}

	private static URL initURL(String url) {
		URL u = null;
		try {
			u = new URL(url);
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return u;
	}

	private static void setTimeout(HttpURLConnection conn) {
		conn.setConnectTimeout(120 * 1000);
		conn.setReadTimeout(120 * 1000);
	}
}
