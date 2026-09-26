package com.crypto.marketdata;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
@Component
public class PeriodicReconciliationService {
    private final CoinConfigurationReader coins;private final RecoveryCoordinator recovery;
    public PeriodicReconciliationService(CoinConfigurationReader coins,RecoveryCoordinator recovery){this.coins=coins;this.recovery=recovery;}
    @Scheduled(fixedDelayString="${market-data.binance.reconciliation-delay-ms:60000}")
    public void reconcile(){recovery.reconcile(coins.enabledSymbols());}
}
