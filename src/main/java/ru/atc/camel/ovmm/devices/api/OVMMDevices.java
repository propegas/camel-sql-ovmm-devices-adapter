package ru.atc.camel.ovmm.devices.api;

public class OVMMDevices {

    private String key;
    private String name;
    private String wwn;
    private String operationalStatus;
    private String state;

	
	
	@Override
	public String toString() {
		return key + " " + name + " " + wwn;
	}


	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}

	
	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getWwn() {
		return wwn;
	}


	public void setWwn(String wwn) {
		this.wwn = wwn;
	}


	public String getOperationalStatus() {
		return operationalStatus;
	}


	public void setOperationalStatus(String operationalStatus) {
		this.operationalStatus = operationalStatus;
	}


	public String getState() {
		return state;
	}


	public void setState(String state) {
		this.state = state;
	}
}