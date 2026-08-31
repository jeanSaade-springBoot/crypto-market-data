package com.crypto.marketdata;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "market-data.binance")
public class MarketDataProperties {
    private String restBaseUrl = "https://data-api.binance.vision";
    private String websocketBaseUrl = "wss://data-stream.binance.vision:443";
    private List<String> intervals = new ArrayList<>(List.of("1m", "5m", "1h", "4h"));
    private int bootstrapLimit = 500;
    private long reconnectHealthSeconds = 15;
    private long configRefreshSeconds = 30;


    public String getRestBaseUrl() { return restBaseUrl; }
    public void setRestBaseUrl(String restBaseUrl) { this.restBaseUrl = restBaseUrl; }
    public String getWebsocketBaseUrl() { return websocketBaseUrl; }
    public void setWebsocketBaseUrl(String websocketBaseUrl) { this.websocketBaseUrl = websocketBaseUrl; }
    public List<String> getIntervals() { return intervals; }
    public void setIntervals(List<String> intervals) { this.intervals = intervals; }
    public int getBootstrapLimit() { return bootstrapLimit; }
    public void setBootstrapLimit(int bootstrapLimit) { this.bootstrapLimit = bootstrapLimit; }
    public long getReconnectHealthSeconds() { return reconnectHealthSeconds; }
    public void setReconnectHealthSeconds(long reconnectHealthSeconds) { this.reconnectHealthSeconds = reconnectHealthSeconds; }
    public long getConfigRefreshSeconds() { return configRefreshSeconds; }
    public void setConfigRefreshSeconds(long configRefreshSeconds) { this.configRefreshSeconds = configRefreshSeconds; }
}
