package com.crypto.marketdata;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.*;
class OwnershipWiringTest {
    @Configuration(proxyBeanMethods=false)
    @Import({OwnershipGate.class,OwnershipLeaseStore.class,OwnershipCoordinator.class,CandleStore.class,StreamPublisher.class,AdmittedCandleWriter.class,GapRepairService.class,RecoveryCoordinator.class,
        BinanceWebSocketManager.class,BinanceStreamUrlBuilder.class,CoinConfigurationReader.class,CollectorIngestionHealth.class,CollectorPoolMetrics.class})
    static class Config {
        @Bean MarketDataProperties properties(){return new MarketDataProperties();}
        @Bean com.fasterxml.jackson.databind.ObjectMapper mapper(){return new com.fasterxml.jackson.databind.ObjectMapper();}
        @Bean BinanceRestClient client(){return org.mockito.Mockito.mock(BinanceRestClient.class);}
    }
    @Test void privateOwnershipPoolDoesNotReplaceBootDatasourceOrTransactionManager(){
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class,JdbcTemplateAutoConfiguration.class,DataSourceTransactionManagerAutoConfiguration.class))
            .withUserConfiguration(Config.class).withPropertyValues("spring.datasource.url=jdbc:h2:mem:wiring;MODE=MySQL", "spring.datasource.username=sa", "market-data.stream.enabled=false")
            .run(context->{assertThat(context).hasNotFailed().hasSingleBean(DataSource.class).hasSingleBean(JdbcTemplate.class);
                assertThat(context.getBean(DataSourceTransactionManager.class).getDataSource()).isSameAs(context.getBean(DataSource.class));
                assertThat(context.getBean(JdbcTemplate.class).getDataSource()).isSameAs(context.getBean(DataSource.class));
                assertThat(context.getBean(OwnershipGate.class).state()).isEqualTo(OwnershipGate.State.FEED_DISABLED);
            });
    }
}
