package com.redisService.model;

import lombok.Data;

import java.util.List;

@Data
public class RedisLeftPush{
    private String key;
    private List<String> list;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }
}
