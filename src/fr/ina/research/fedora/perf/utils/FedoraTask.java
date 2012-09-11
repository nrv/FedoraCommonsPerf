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
	private FedoraClient globalClient;
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
	
	private String foxmlToIngest;
	private String pidToPurge;

	public FedoraTask(String foxmlToIngest, String pidToPurge) {
		super();
		this.foxmlToIngest = foxmlToIngest;
		this.pidToPurge = pidToPurge;
	}

	public void run() {
		FedoraClient client = globalClient;

		if (client == null) {
			client = localClient.get();
		}

		if (foxmlToIngest != null) {
			IngestResponse ingestResponse = null;
			try {
				ingestResponse = new Ingest().content(foxmlToIngest).execute(client);
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
		} else if (pidToPurge != null) {
			FedoraResponse purgeResponse = null;
			try {
				purgeResponse = new PurgeObject(pidToPurge).execute(client);
				if (purgeResponse.getStatus() != 200) {
					System.err.println("Problem when purging object '" + pidToPurge + "' - status code " + purgeResponse.getStatus());
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
	}

	public void setGlobalClient(FedoraClient client) {
		this.globalClient = client;
	}
}
