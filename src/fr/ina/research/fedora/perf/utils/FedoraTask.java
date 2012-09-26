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

package fr.ina.research.fedora.perf.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.request.Ingest;
import com.yourmediashelf.fedora.client.request.PurgeObject;
import com.yourmediashelf.fedora.client.response.FedoraResponse;
import com.yourmediashelf.fedora.client.response.IngestResponse;

import fr.ina.research.fedora.perf.LaunchPerfTest;

/**
 * FedoraTask
 * 
 * @author Nicolas HERVE - nherve@ina.fr
 */
public class FedoraTask implements Runnable {
	public enum Type {
		INGEST, PURGE, QUERY
	}

	private static ThreadLocal<FedoraClient> localClient = new ThreadLocal<FedoraClient>() {

		@Override
		protected FedoraClient initialValue() {
			try {
				return LaunchPerfTest.connectFedora();
			} catch (MalformedURLException e) {
				e.printStackTrace();
				return null;
			}
		}
	};

	private boolean allowDuplicate;;
	private FedoraClient globalClient;
	private String parameter;
	private Type type;

	public FedoraTask(Type type, String parameter) {
		super();
		this.type = type;
		this.parameter = parameter;
		setAllowDuplicate(true);
	}

	private void ingest(FedoraClient client) {
		IngestResponse ingestResponse = null;
		try {
			ingestResponse = new Ingest().content(parameter).execute(client);
			if (ingestResponse.getStatus() != 201) {
				System.err.println("Problem when ingesting foxml - status code " + ingestResponse.getStatus());
			}
		} catch (FedoraClientException e) {
			e.printStackTrace();
		} finally {
			if (ingestResponse != null) {
				try {
					ingestResponse.getEntityInputStream().close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void purge(FedoraClient client) {
		FedoraResponse purgeResponse = null;
		try {
			purgeResponse = new PurgeObject(parameter).execute(client);
			if (purgeResponse.getStatus() != 200) {
				System.err.println("Problem when purging object '" + parameter + "' - status code " + purgeResponse.getStatus());
			}
		} catch (FedoraClientException e) {
			e.printStackTrace();
		} finally {
			if (purgeResponse != null) {
				try {
					purgeResponse.getEntityInputStream().close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String query(FedoraClient client) {
		try {
			Set<String> somePids = LaunchPerfTest.queryPids(client, "source~" + parameter, -1);
			if ((somePids == null) || (somePids.size() == 0)) {
				return null;
			}
			return somePids.iterator().next();
		} catch (FedoraClientException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void run() {
		FedoraClient client = globalClient;

		if (client == null) {
			client = localClient.get();
		}

		if (type == Type.INGEST) {
			ingest(client);
		} else if (type == Type.PURGE) {
			purge(client);
		} else if (type == Type.QUERY) {
			query(client);
		}
	}

	public void setAllowDuplicate(boolean allowDuplicate) {
		this.allowDuplicate = allowDuplicate;
	}

	public void setGlobalClient(FedoraClient client) {
		this.globalClient = client;
	}
}
