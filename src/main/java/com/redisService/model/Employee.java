package com.redisService.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Employee implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String id;
    private int status;
    private String name;
    private String mob;
    private Address address;

    public String toJson(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        }catch (Exception ex){
            throw new RuntimeException("error 0");
        }
    }

    public static Employee fromJson(String json){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, Employee.class);
        }catch (Exception ex){
            throw new RuntimeException("error 1");
        }
    }
}
