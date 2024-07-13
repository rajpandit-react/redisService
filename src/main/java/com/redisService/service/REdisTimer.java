package com.redisService.service;

import com.redisService.model.Employee;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
public class REdisTimer {

    @Autowired
    @Qualifier("entity")
    private ReactiveRedisTemplate<String, Employee> reactiveRedisTemplate;

    @PostConstruct
    public void timerRedis(){
        Flux.interval(Duration.ofSeconds(3))
                .flatMap(e -> performTask())
                .subscribe();
    }

    private Mono<Void> performTask(){
        return reactiveRedisTemplate.opsForStream().read(Consumer.from("mygroup", "employee"), StreamOffset.create("stream::employee", ReadOffset.lastConsumed())).map(data -> {
            System.out.println("data timer=> "+data);
            return data;
        }).then();
    }
}
