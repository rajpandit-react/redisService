package com.redisService.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redisService.model.Employee;
import lombok.SneakyThrows;
import org.json.simple.JSONObject;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.nio.charset.StandardCharsets;

public class EmployeeSerializer<T> implements RedisSerializer<T> {

    private final Class<T> targetType;
    private final ObjectMapper objectMapper;

    public EmployeeSerializer(Class<T> targetType){
        this.targetType = targetType;
        objectMapper = new ObjectMapper();
    }

    @SneakyThrows
    @Override
    public byte[] serialize(T value) throws SerializationException {
        if(value == null){
            return null;
        }

        System.out.println("value=> "+value.getClass().getName());
//
//        if (value == Employee.class){
//            String jsonString = ((Employee)value).toJson();
//            return jsonString.getBytes(StandardCharsets.UTF_8);
//        }else {
//            return value.toString().getBytes(StandardCharsets.UTF_8);
//        }

        return objectMapper.writeValueAsBytes(value);


    }

    @SneakyThrows
    @Override
    public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null){
            return null;
        }
//
//        System.out.println("targetType=> "+targetType.getName()+" => "+targetType.getClass().getName());
        String jsonString = new String(bytes, StandardCharsets.UTF_8);
//        if (targetType == JSONObject.class){
//            return targetType.cast(JSONObject.escape(jsonString));
//        }else{
//            return objectMapper.readValue(jsonString, targetType);
//        }

        return objectMapper.readValue(jsonString, targetType);
    }
}
