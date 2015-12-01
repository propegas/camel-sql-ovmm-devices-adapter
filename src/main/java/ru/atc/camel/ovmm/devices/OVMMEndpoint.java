package ru.atc.camel.ovmm.devices;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

@UriEndpoint(scheme="ovmm", title="OVMM", syntax="ovmm://operationPath", consumerOnly=true, consumerClass=OVMMConsumer.class, label="restna")
public class OVMMEndpoint extends DefaultPollingEndpoint {

	public OVMMEndpoint(String uri, String operationPath, OVMMComponent component) {
		super(uri, component);
		this.operationPath = operationPath;
	}
	
	private String operationPath;

	@UriParam
	private OVMMConfiguration configuration;

	public Producer createProducer() throws Exception {
		throw new UnsupportedOperationException("OVMMProducer is not implemented");
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		OVMMConsumer consumer = new OVMMConsumer(this, processor);
        return consumer;
	}

	public boolean isSingleton() {
		return true;
	}

	public String getOperationPath() {
		return operationPath;
	}

	public void setOperationPath(String operationPath) {
		this.operationPath = operationPath;
	}

	public OVMMConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(OVMMConfiguration configuration) {
		this.configuration = configuration;
	}
	
}