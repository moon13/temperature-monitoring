package com.algaworks.algasensors.temperature.monitoring.infraestructure.rabbitmq;

import com.algaworks.algasensors.temperature.monitoring.api.model.TemperatureLogData;
import com.algaworks.algasensors.temperature.monitoring.domain.service.SensorAlertingService;
import com.algaworks.algasensors.temperature.monitoring.domain.service.TemperatureMonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static com.algaworks.algasensors.temperature.monitoring.infraestructure.rabbitmq.RabbitMQConfig.QUEUE_ALERTING;
import static com.algaworks.algasensors.temperature.monitoring.infraestructure.rabbitmq.RabbitMQConfig.QUEUE_PROCESS_TEMPERATURE;

@Slf4j
@RequiredArgsConstructor
@Component
public class RabbitMQListener {

    private final TemperatureMonitoringService temperatureMonitoringService;
    private final SensorAlertingService sensorAlertingServer;


    @SneakyThrows
    @RabbitListener(queues = QUEUE_PROCESS_TEMPERATURE, concurrency = "2-3")
    public void handlePRocessingTemperature(@Payload TemperatureLogData temperatureLogData,
                       @Headers Map<String,Object> headers

    ){
/*        TSID sensorId = temperatureLogData.getSensorId();
        Double temperature = temperatureLogData.getValue();

        log.info("Temperature updated. SensorId {} Temp {}", sensorId, temperature);
        log.info("Headers :{}", headers.toString());*/
        temperatureMonitoringService.processTemperatureReading(temperatureLogData);
     //   Thread.sleep(Duration.ofSeconds(5));
    }

    @SneakyThrows
    @RabbitListener(queues = QUEUE_ALERTING, concurrency = "2-3")
    public void handleAlerting(@Payload TemperatureLogData temperatureLogData,
                       @Headers Map<String,Object> headers

    ){
         sensorAlertingServer.handleAlert(temperatureLogData);
        //log.info("Alerting : SensorID{} Temp {}",temperatureLogData.getSensorId(),temperatureLogData.getValue());
        Thread.sleep(Duration.ofSeconds(5));

    }
}
