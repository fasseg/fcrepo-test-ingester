package org.fcrepo.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.Principal;
import java.text.DecimalFormat;
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
	private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");
	private final DefaultHttpClient client = new DefaultHttpClient();

	private final URI fedoraUri;
	private final OutputStream ingestOut;
	private final OutputStream updateOut;

	public TestIngester(String fedoraUri, String user, String pass)
			throws IOException {
		super();
		ingestOut = new FileOutputStream("ingest.log");
		updateOut = new FileOutputStream("update.log");
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
		int numDatastreams = Integer.parseInt(args[3]);
		int size = Integer.parseInt(args[4]);
		TestIngester ingester = null;
		System.out.println("generating " + numDatastreams + " datastreams with size " + size);
		try {
			ingester = new TestIngester(uri, user, pass);
			String objectId = ingester.ingestObject("test-1");
			System.out.println("ingested test object");
			for (int i = 0; i < numDatastreams; i++) {
				ingester.ingestDatastream(objectId, "ds-" + (i + 1), size);
				float percent = (float) (i + 1) / (float) numDatastreams * 100f;
				System.out.print("\r" + FORMATTER.format(percent) + "%");
			}
			System.out.println(" - ingest datastreams finished");
			ingester.updateAllDatastreams(objectId, size);
			System.out.println(" - update datastreams finished");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ingester.shutdown();
		}
	}

	private void shutdown() {
		IOUtils.closeQuietly(ingestOut);
		IOUtils.closeQuietly(updateOut);
	}

	private void updateAllDatastreams(String objectId, int size)
			throws Exception {
		HttpGet get = new HttpGet(this.fedoraUri + "/objects/" + objectId
				+ "/datastreams?format=xml&versionable=false");
		HttpResponse resp = client.execute(get);
		if (resp.getStatusLine().getStatusCode() != 200) {
			throw new Exception("Unable to get object with id " + objectId
					+ ". fedora returned " + resp.getStatusLine());
		}
		String xml = IOUtils.toString(resp.getEntity().getContent());
		get.releaseConnection();
		Matcher m = Pattern.compile("datastream dsid=\"ds-.*?\"").matcher(xml);
		int numDatastreams = 0;
		while (m.find()) {
			numDatastreams++;
		}
		m.reset();
		int count = 0;
		while (m.find()) {
			String dsId = xml.substring(m.start() + 17, m.end() - 1);
			updateDatastream(objectId, dsId, size);
			float percent = (float) ++count / (float) numDatastreams * 100f;
			System.out.print("\r" + FORMATTER.format(percent) + "%");
		}
	}

	private void updateDatastream(String objId, String dsId, int size)
			throws Exception {
		HttpPut put = new HttpPut(this.fedoraUri + "/objects/" + objId
				+ "/datastreams/" + dsId);
		put.setEntity(new ByteArrayEntity(getRandomBytes(size)));
		long start = System.currentTimeMillis();
		HttpResponse resp = client.execute(put);
		IOUtils.write((System.currentTimeMillis() - start) + "\n", updateOut);
		put.releaseConnection();
		if (resp.getStatusLine().getStatusCode() != 200) {
			throw new Exception("Unabel to update datastream " + dsId
					+ ". Fedora returned " + resp.getStatusLine());
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
		IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
		post.releaseConnection();
		if (resp.getStatusLine().getStatusCode() != 201) {
			throw new Exception("Unable to ingest datastream " + label
					+ " fedora returned " + resp.getStatusLine());
		}
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
