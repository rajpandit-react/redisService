package com.redisService.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redisService.model.Employee;
import com.redisService.model.RedisLeftPush;
import io.lettuce.core.XReadArgs;
import lombok.AllArgsConstructor;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.Channel;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/redis")
@AllArgsConstructor
public class RedisController {

    @Autowired
    private ReactiveStringRedisTemplate stringRedisTemplate;

    @Autowired
    @Qualifier("entity")
    private ReactiveRedisTemplate<String, Employee> reactiveRedisTemplate;
//
//    @Autowired
//    @Qualifier("hash")
//    private ReactiveRedisTemplate<String, Objects> objectsReactiveRedisTemplate;


    @PostMapping("/post")
    public void postToRedis(@RequestBody JSONObject json){
        System.out.println("json=> "+json);
        stringRedisTemplate.opsForValue()
                .set(json.get("key").toString(), json.get("value").toString()).subscribe();
    }

    @GetMapping("/get/{key}")
    public Mono<String> getFromRedis(@PathVariable("key") String key){
        return stringRedisTemplate.opsForValue().get(key)
                .map(data -> {
                    return data;
                });
    }

//    @PostMapping("/lpush")
//    public void lpush(@RequestBody RedisLeftPush json){
//        System.out.println("json===> "+json);
//
//        stringRedisTemplate.opsForList().leftPushAll(json.getKey(), json.getList()).log().subscribe();
//    }

    @PostMapping("/hash")
    public void pushMap(){
        ReactiveStreamOperations<String, String, String> streamOperations = stringRedisTemplate.opsForStream();
        ReactiveHashOperations<String, String, String> hashOperations = stringRedisTemplate.opsForHash();
        String stream = "stream::hash::test";
        HashMap<String, String> map = new HashMap<>();
        map.put("model", "2023");
        map.put("brand", "tata");
        map.put("type", "disck drum");

        StringRecord record = StreamRecords.string(map).withStreamKey(stream);
        streamOperations.add(record).map(data ->{
            System.out.println(data);
            System.out.println(stream);
            stringRedisTemplate.opsForStream().read(StreamOffset.fromStart(stream))
                    .filter(record1 -> record1.getId().equals(data))
                    .map(record1 -> {
                        record1.mapEntries(d -> {
                            System.out.println(d.getKey()+" "+d.getValue());
                            return  d;
                        });
                        return record1;
                    }).subscribe();
            return data;
        }).subscribe();
    }


    @PostMapping("/employee")
    public void emplyeePushToRedis(@RequestBody Employee employee){
        String stremKey = "stream::employee";

        reactiveRedisTemplate.opsForSet().add("employee1", employee)
                .map(data -> {
                    System.out.println("data=> "+data);
                    return data;
                }).subscribe();

        reactiveRedisTemplate.opsForSet().members("employee1")
                .map(data -> {
                    System.out.println("data => "+data);
                    return data;
                }).subscribe();

        reactiveRedisTemplate.opsForSet().intersect(List.of("employee","employee1"))
                .map(data -> {
                    System.out.println("intersection=> "+data);
                    return data;
                }).subscribe();

        reactiveRedisTemplate.opsForSet().union(List.of("employee", "employee1"))
                .map(data -> {
                    System.out.println("union=> "+data);
                    return data;
                }).subscribe();
    }

    @PostMapping("/publisher")
    public Mono<Void> publishMessage(@RequestBody Employee employee){
        String topic = "my-topic";
        return reactiveRedisTemplate.convertAndSend(new ChannelTopic(topic).getTopic(), employee).then();

    }

    @PostMapping("/lpush")
    public Mono<Void> pushToList(@RequestBody Employee employee){
        String list = "employee-list";
        return reactiveRedisTemplate.opsForList().leftPush(list, employee)
                .then();

    }

    @GetMapping("/range-list")
    public Flux<Employee> getList(){
        reactiveRedisTemplate.opsForList().size("employee-list")
                .map(size -> {
                    System.out.println("size=> "+size);
                    return size;
                }).subscribe();

        return reactiveRedisTemplate.opsForList().range("employee-list", 0, -1);

    }

    @PostMapping("/hash1")
    public Mono<Void> addToHash(@RequestBody Employee employee){
        String key = "my-hash";
        reactiveRedisTemplate.opsForHash().get(key, "102").map(data -> {
            System.out.println("data=> "+data);
            return data;
        }).subscribe();

        reactiveRedisTemplate.opsForHash().entries(key).map(data -> {
            System.out.println(data.getKey()+" => "+data.getValue());
            return data;
        }).subscribe();

        reactiveRedisTemplate.opsForHash().size(key).map(size-> {
            System.out.println(size);
            return size;
        }).subscribe();
//
//        reactiveRedisTemplate.opsForHash().keys(key).map(keys -> {
//            System.out.println("keys=> "+keys);
//            return keys;
//        }).subscribe();

        reactiveRedisTemplate.opsForHash().multiGet(key, List.of("101", "102", "801", "8888", "8521", "8745")).map(multiget-> {
            System.out.println("multiget=> "+multiget);
            return multiget;
        }).subscribe();

//
//        Flux.range(0,10).map(range ->{
//            employee.setId(String.valueOf((500+range)));
//            return employee;
//        }).collect(Collectors.toMap(Employee::getId, e-> e))
//                .map(data-> {
//                    reactiveRedisTemplate.opsForHash().putAll(key,data).subscribe();
//                    return data;
//                }).subscribe();


//        reactiveRedisTemplate.opsForHash().delete(key).subscribe();
        reactiveRedisTemplate.opsForHash().values(key).collectList().map(data -> {
            System.out.println("value=> "+data);
            return data;
        }).subscribe();


        reactiveRedisTemplate.opsForHash().remove(key, "501").subscribe();
        return reactiveRedisTemplate.opsForHash().put(key, employee.getId(), employee).then();
    }


    @PostMapping("/stream")
    public void streamRedisTest(@RequestBody Employee employee){
        String stream = "stream::employee";
        HashMap<String, Employee> employeeHashMap = new HashMap<>();
        employeeHashMap.put(employee.getId(), employee);
        reactiveRedisTemplate.opsForStream().add(stream, employeeHashMap).map(recordId -> {
            System.out.println("recordId=> "+recordId);
            return recordId;
        }).subscribe();
//
//        reactiveRedisTemplate.opsForStream().read(StreamReadOptions.empty().block(Duration.ZERO), StreamOffset.fromStart(stream))
//                .map(e -> {
//                    System.out.println(e);
//                    e.mapEntries(map -> {
//                        System.out.println(map.getKey()+" "+map.getValue());
//                        return map;
//                    });
//                    return e;
//                }).subscribe();

//
//        reactiveRedisTemplate.opsForStream().read(Consumer.from("mygroup", "employee"), StreamOffset.create(stream, ReadOffset.lastConsumed())).map(data -> {
//            System.out.println("data=> "+data);
//            return data;
//        }).subscribe();
    }

    private final ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;


    @GetMapping(produces = "text/event-stream")
    public Flux<String> getMessages() {
//        // Replace this with your Kafka consumer logic
//        return Flux.just("Message 1", "Message 2", "Message 3")
//                .delayElements(java.time.Duration.ofSeconds(1));

        return reactiveKafkaConsumerTemplate.receiveAutoAck()
                .flatMap(record -> Flux.just(record.value()))
                .delayElements(Duration.ofSeconds(1));
    }
}
