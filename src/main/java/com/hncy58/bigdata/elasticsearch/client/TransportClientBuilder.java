package com.hncy58.bigdata.elasticsearch.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.hncy58.bigdata.elasticsearch.util.Config;

public class TransportClientBuilder {
	private String clusterServers;
	private String clusterName;
	
	public TransportClientBuilder() {
		clusterServers = Config.getStringValue("/config/elasticsearch/clusterServers");
		clusterName = Config.getStringValue("/config/elasticsearch/clusterName");
	}
	
	public TransportClientBuilder(final String clusterServers, final String clusterName) {
		this.clusterServers = clusterServers;
		this.clusterName = clusterName;
	}
	
	public TransportClient build() {
		String[] servers = clusterServers.split(",");
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		TransportClient client = new PreBuiltTransportClient(settings);

		for (String server : servers) {
			try {
				String[] hostPort = server.split(":");
				client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.valueOf(hostPort[1])));
			} catch (UnknownHostException e) {
			}
		}
		return client;
	}
}
