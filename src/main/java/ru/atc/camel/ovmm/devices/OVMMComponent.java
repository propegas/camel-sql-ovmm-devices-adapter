package ru.atc.camel.ovmm.devices;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class OVMMComponent extends UriEndpointComponent {

	public OVMMComponent() {
		super(OVMMEndpoint.class);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		
		OVMMEndpoint endpoint = new OVMMEndpoint(uri, remaining, this);		
		OVMMConfiguration configuration = new OVMMConfiguration();
		
		// use the built-in setProperties method to clean the camel parameters map
		setProperties(configuration, parameters);
		
		endpoint.setConfiguration(configuration);		
		return endpoint;
	}
}