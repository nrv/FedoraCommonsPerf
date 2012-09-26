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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Random;
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
import fr.ina.research.fedora.perf.utils.FedoraTask.Type;

/**
 * LaunchPerfTest
 * 
 * @author Nicolas HERVE - nherve@ina.fr
 */
public class LaunchPerfTest {
	public final static String FEDORA_LOGIN = "fedoraAdmin";
	public final static String FEDORA_PASSWORD = "fedoraAdmin";
	public final static String FEDORA_URL = "http://localhost/fedora";
	public final static String NAMESPACE = "fake";
	private final static int NB_DOCUMENTS = 10000;
	private final static int NB_THREADS = 20;
	public final static String SOURCE = "EXT_ID_";
	public final static boolean USE_SINGLE_CONNECTION = true;

	public static FedoraClient connectFedora() throws MalformedURLException {
		System.out.println("New FedoraClient");
		return new FedoraClient(new FedoraCredentials(FEDORA_URL, FEDORA_LOGIN, FEDORA_PASSWORD));
	}

	public static void main(String[] args) {
		LaunchPerfTest test = new LaunchPerfTest();

		try {
			test.connect();

			test.purgeAllFakeDocuments();
			test.ingestFakeDocuments(NB_DOCUMENTS);
			//test.ingestRandomDocuments(NB_DOCUMENTS, true);
			//test.queryFakeDocuments(NB_DOCUMENTS * 5, false);

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (FedoraClientException e) {
			e.printStackTrace();
		} finally {
			test.close();
		}
	}

	public static Set<String> queryPids(FedoraClient client, String query, int maxResults) throws FedoraClientException {
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
				System.out.println("sessionToken : " + sessionToken);
				findObjectsQuery = findObjectsQuery.sessionToken(sessionToken);
			}

			FindObjectsResponse findObjectsResponse = null;
			try {
				findObjectsResponse = findObjectsQuery.execute(client);
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
			} finally {
				if (findObjectsResponse != null) {
					try {
						findObjectsResponse.getEntityInputStream().close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} while (found && again && ((maxResults <= 0) || (result.size() < maxResults)));
		return result;
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

	private void connect() throws MalformedURLException, FedoraClientException {
		client = connectFedora();
		System.out.println("Connected to " + FEDORA_URL + " running Fedora Commons version " + client.getServerVersion());
	}

	private void ingestFakeDocuments(int nbDocuments) {
		for (int i = 0; i < nbDocuments; i++) {
			String foxml = generator.getNextFOXML();
			FedoraTask task = new FedoraTask(Type.INGEST, foxml);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			threadPool.submit(task);
		}

		waitTasks();
		
		System.out.println("Ingestion of " + nbDocuments + " documents finished");
	}

	private void ingestRandomDocuments(int nbDocuments, boolean allowDuplicate) {
		int maxId = nbDocuments / 5;
		Random rd = new Random();
		for (int i = 0; i < nbDocuments; i++) {
			String foxml = generator.getNextFOXML(rd.nextInt(maxId));
			FedoraTask task = new FedoraTask(Type.INGEST, foxml);
			task.setAllowDuplicate(allowDuplicate);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			threadPool.submit(task);
		}

		waitTasks();
		
		System.out.println("Ingestion of " + nbDocuments + " random documents finished");
	}

	private void purgeAllFakeDocuments() throws FedoraClientException {
		Set<String> pids = queryPids(client, "pid~" + NAMESPACE + ":*", -1);

		System.out.println("Found " + pids.size() + " objects to purge");

		for (String pid : pids) {
			FedoraTask task = new FedoraTask(Type.PURGE, pid);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			threadPool.submit(task);
		}

		waitTasks();

		System.out.println("Purge finished");
	}

	private void queryFakeDocuments(int nb, boolean singleThread) throws FedoraClientException {
		Random rd = new Random();
		for (int i = 0; i < nb; i++) {
			String source = SOURCE + rd.nextInt(NB_DOCUMENTS * 2);
			FedoraTask task = new FedoraTask(Type.QUERY, source);
			if (USE_SINGLE_CONNECTION) {
				task.setGlobalClient(client);
			}
			if (singleThread) {
				task.run();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				threadPool.submit(task);
			}
			
		}

		if (!singleThread) {
			waitTasks();
		}
		
		System.out.println("Querying of " + nb + " documents finished");
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
