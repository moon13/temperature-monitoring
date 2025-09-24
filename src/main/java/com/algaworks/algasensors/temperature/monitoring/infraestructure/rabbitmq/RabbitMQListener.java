package com.algaworks.algasensors.temperature.monitoring.infraestructure.rabbitmq;

import com.algaworks.algasensors.temperature.monitoring.api.model.TemperatureLogData;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static com.algaworks.algasensors.temperature.monitoring.infraestructure.rabbitmq.RabbitMQConfig.QUEUE;

@Slf4j
@RequiredArgsConstructor
@Component
public class RabbitMQListener {

    @SneakyThrows
    @RabbitListener(queues = QUEUE)
    public void handle(@Payload TemperatureLogData temperatureLogData,
                       @Headers Map<String,Object> headers

    ){
        TSID sensorId = temperatureLogData.getSensorId();
        Double temperature = temperatureLogData.getValue();

        log.info("Temperature updated. SensorId {} Temp {}", sensorId, temperature);
        log.info("Headers :{}", headers.toString());
        Thread.sleep(Duration.ofSeconds(5));




    }
}
