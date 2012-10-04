package org.fcrepo.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.Principal;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class TestIngester {
	private final URI fedoraUri;
	private final DefaultHttpClient client = new DefaultHttpClient();

	public TestIngester(String fedoraUri, String user, String pass) {
		super();
		if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
			fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
		}
		this.fedoraUri = URI.create(fedoraUri);
		this.client.getCredentialsProvider().setCredentials(
				new AuthScope(this.fedoraUri.getHost(),
						this.fedoraUri.getPort()),
				new UsernamePasswordCredentials(user, pass));
	}

	public static void main(String[] args) {
		String uri = args[0];
		String user = args[1];
		String pass = args[2];
		int numDatatstreams = Integer.parseInt(args[3]);
		int size = Integer.parseInt(args[4]);
		TestIngester ingester = new TestIngester(uri, user, pass);
		try {
			String objectId = ingester.ingestObject("test-1");
			System.out.println("ingested test object");
			for (int i = 0; i < numDatatstreams; i++) {
				ingester.ingestDatastream(objectId, "ds-" + (i + 1), size);
			}
			System.out.println("ingested datastreams");
			ingester.updateAllDatastreams(objectId, size);
			System.out.println("updated datastreams");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void updateAllDatastreams(String objectId, int size)
			throws Exception {
		HttpGet get = new HttpGet(this.fedoraUri + "/objects/" + objectId
				+ "/datastreams?format=xml");
		HttpResponse resp = client.execute(get);
		if (resp.getStatusLine().getStatusCode() != 200) {
			throw new Exception("Unable to get object with id " + objectId
					+ ". fedora returned " + resp.getStatusLine());
		}
		String xml = IOUtils.toString(resp.getEntity().getContent());
		get.releaseConnection();
		Matcher m = Pattern.compile("datastream dsid=\"ds-.*?\"").matcher(xml);
		while (m.find()) {
			String dsId = xml.substring(m.start() + 17, m.end() - 1);
			updateDatastream(objectId, dsId, size);
		}
	}

	private void updateDatastream(String objId,String dsId, int size) throws Exception{
		HttpPut put = new HttpPut(this.fedoraUri + "/objects/" + objId + "/datastreams/" + dsId);
		put.setEntity(new ByteArrayEntity(getRandomBytes(size)));
		long start = System.currentTimeMillis();
		HttpResponse resp = client.execute(put);
		System.out.println((System.currentTimeMillis() - start));
		put.releaseConnection();
		if (resp.getStatusLine().getStatusCode() != 200){
			throw new Exception("Unabel to update datastream " + dsId + ". Fedora returned " + resp.getStatusLine());
		}
	}

	private void ingestDatastream(String objectId, String label, int size)
			throws Exception {
		HttpPost post = new HttpPost(fedoraUri.toASCIIString() + "/objects/"
				+ objectId + "/datastreams/" + label
				+ "?versionable=false&controlGroup=M");
		post.setHeader("Content-Type", "application/octet-stream");
		post.setEntity(new ByteArrayEntity(getRandomBytes(size)));
		long start = System.currentTimeMillis();
		HttpResponse resp = client.execute(post);
		post.releaseConnection();
		if (resp.getStatusLine().getStatusCode() != 201) {
			throw new Exception("Unable to ingest datastream " + label
					+ " fedora returned " + resp.getStatusLine());
		}
//		System.out.println((System.currentTimeMillis() - start));
	}

	private byte[] getRandomBytes(int size) {
		byte[] data = new byte[size];
		Random r = new Random();
		r.nextBytes(data);
		return data;
	}

	private String ingestObject(String label) throws Exception {
		HttpPost post = new HttpPost(
				fedoraUri.toASCIIString()
						+ "/objects/new?format=info:fedora/fedora-system:FOXML-1.1&label="
						+ label);
		HttpResponse resp = client.execute(post);
		String answer = IOUtils.toString(resp.getEntity().getContent());
		post.releaseConnection();

		if (resp.getStatusLine().getStatusCode() != 201) {
			throw new Exception("Unable to ingest object, fedora returned "
					+ resp.getStatusLine().getStatusCode());
		}
		return answer;
	}
}
