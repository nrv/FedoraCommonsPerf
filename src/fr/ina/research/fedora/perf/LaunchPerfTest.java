/*
 * Copyright 2012 Institut National de l'Audiovisuel.
 * 
 * This file is part of FedoraCommonsPerf.
 * 
 * FedoraCommonsPerf is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * FedoraCommonsPerf is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with FedoraCommonsPerf. If not, see <http://www.gnu.org/licenses/>.
 */

package fr.ina.research.fedora.perf;

import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.FedoraCredentials;
import com.yourmediashelf.fedora.client.request.FindObjects;
import com.yourmediashelf.fedora.client.response.FindObjectsResponse;

import fr.ina.research.fedora.perf.utils.FOXMLGenerator;
import fr.ina.research.fedora.perf.utils.FedoraTask;

/**
 * LaunchPerfTest
 * 
 * @author Nicolas HERVE - nherve@ina.fr
 */
public class LaunchPerfTest {
	public final static boolean USE_SINGLE_CONNECTION = false;

	public final static String FEDORA_LOGIN = "fedoraAdmin";
	public final static String FEDORA_PASSWORD = "fedoraAdmin";
	public final static String FEDORA_URL = "your server here";

	public final static String NAMESPACE = "fake";

	private final static int NB_DOCUMENTS = 10000;
	private final static int NB_THREADS = 5;

	public static FedoraClient connectFedora() throws MalformedURLException {
		System.out.println("New Fedora connection established");
		return new FedoraClient(new FedoraCredentials(FEDORA_URL, FEDORA_LOGIN, FEDORA_PASSWORD));
	}

	public static void main(String[] args) {
		LaunchPerfTest test = new LaunchPerfTest();

		try {
			String v = test.connect();
			System.out.println("Connected to " + FEDORA_URL + " running Fedora Commons version " + v);

			test.purgeAllFakeDocuments();
			System.out.println("Purge finished");

			test.ingestFakeDocuments(NB_DOCUMENTS);
			System.out.println("Ingestion of " + NB_DOCUMENTS + " documents finished");

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (FedoraClientException e) {
			e.printStackTrace();
		} finally {
			test.close();
		}
	}

	private FedoraClient client;
	private FOXMLGenerator generator;
	private ThreadPoolExecutor threadPool;

	public LaunchPerfTest() {
		super();

		threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(NB_THREADS);
		generator = new FOXMLGenerator();
		client = null;
	}

	private void close() {
		if (threadPool != null) {
			threadPool.shutdown();
		}
	}

	private String connect() throws MalformedURLException, FedoraClientException {
		client = connectFedora();
		return client.getServerVersion();
	}

	private void ingestFakeDocuments(int nbDocuments) {
		for (int i = 0; i < nbDocuments; i++) {
			String foxml = generator.getNextFOXML();
			FedoraTask task = new FedoraTask(foxml, null);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			threadPool.submit(task);
		}

		waitTasks();

	}

	private void purgeAllFakeDocuments() throws FedoraClientException {
		Set<String> pids = queryPids("pid~" + NAMESPACE + ":*", -1);

		System.out.println("Found " + pids.size() + " objects to purge");

		for (String pid : pids) {
			FedoraTask task = new FedoraTask(null, pid);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			threadPool.submit(task);
		}

		waitTasks();
	}

	private Set<String> queryPids(String query, int maxResults) throws FedoraClientException {
		boolean found;
		boolean again;
		Set<String> result = new HashSet<String>();
		String sessionToken = null;
		int mtq = 100;
		if ((maxResults > 0) && (maxResults < mtq)) {
			mtq = maxResults;
		}
		do {
			FindObjects findObjectsQuery = new FindObjects().query(query).maxResults(mtq).pid();
			if (sessionToken != null) {
				findObjectsQuery = findObjectsQuery.sessionToken(sessionToken);
			}

			FindObjectsResponse findObjectsResponse = findObjectsQuery.execute(client);
			sessionToken = findObjectsResponse.getToken();

			if (sessionToken == null) {
				again = false;
			} else {
				again = true;
			}

			found = false;
			for (String pid : findObjectsResponse.getPids()) {
				result.add(pid);
				found = true;
			}
		} while (found && again && ((maxResults <= 0) || (result.size() < maxResults)));
		return result;
	}

	private void waitTasks() {
		while (!(threadPool.getQueue().isEmpty() && (threadPool.getActiveCount() == 0))) {
			try {
				System.out.println("~ waiting (" + threadPool.getQueue().size() + " / " + threadPool.getActiveCount() + ")");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
