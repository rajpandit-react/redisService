package com.redisService.config;

import com.redisService.model.Employee;
import com.redisService.service.EmployeeSerializer;
import com.redisService.service.MessageSubscriber;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.*;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Component
public class RedisConfig {

    @Autowired
    private MessageSubscriber subscriber;

    @Bean
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory(){
        return new LettuceConnectionFactory("localhost", 6379);
    }

    @Bean("entity")
    public ReactiveRedisTemplate<String, ?> reactiveRedisTemplate(
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
        return createReactiveRedisTemplate(reactiveRedisConnectionFactory);
    }

//
//    public <T> ReactiveRedisTemplate<String, T> createReactiveRedisTemplate(ReactiveRedisConnectionFactory reactiveRedisConnectionFactory, Class<T> targetType){
//        StringRedisSerializer keyRedisSerializer = new StringRedisSerializer();
//        Jackson2JsonRedisSerializer<T> valueSerializer = new Jackson2JsonRedisSerializer<T>(targetType);
//        RedisSerializationContext.RedisSerializationContextBuilder<String, T> builder = RedisSerializationContext.newSerializationContext(keyRedisSerializer);
//        RedisSerializationContext<String, T> context =
//                builder.value(valueSerializer)
//                        .hashValue(new EmployeeSerializer<>(targetType))
//                        .value(new EmployeeSerializer<>(targetType)).build();
//        return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory, context);
//    }

    @Bean
    public MessageListenerAdapter messageListenerAdapter(){
        return new MessageListenerAdapter(subscriber);
    }

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory, MessageListenerAdapter messageListenerAdapter){
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        container.addMessageListener(messageListenerAdapter, new ChannelTopic("my-topic"));
        return container;
    }


    public <T> ReactiveRedisTemplate<String, T> createReactiveRedisTemplate(ReactiveRedisConnectionFactory reactiveRedisConnectionFactory){
        StringRedisSerializer keyRedisSerializer = new StringRedisSerializer();
        Jackson2JsonRedisSerializer<T> valueSerializer = new Jackson2JsonRedisSerializer<T>((Class<T>) Object.class);
        RedisSerializationContext.RedisSerializationContextBuilder<String, T> builder = RedisSerializationContext.newSerializationContext(keyRedisSerializer);
        RedisSerializationContext<String, T> context =
                builder.value(valueSerializer)
                        .hashValue(new EmployeeSerializer<>(Object.class))
                        .value((RedisSerializer<T>) new EmployeeSerializer<>(Object.class)).build();
        return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory, context);
    }

//
//    @Bean("object")
//    public ReactiveRedisTemplate<String, Object> objectReactiveRedisTemplate(
//            ReactiveRedisConnectionFactory connectionFactory) {
//        StringRedisSerializer stringSerializer = new StringRedisSerializer();
//        GenericJackson2JsonRedisSerializer jsonSerializer = new GenericJackson2JsonRedisSerializer();
//
//        RedisSerializationContext<String, Object> serializationContext =
//                RedisSerializationContext.<String, Object>newSerializationContext(stringSerializer)
//                        .hashKey(stringSerializer)
//                        .hashValue(jsonSerializer)
//                        .build();
//
//        return new ReactiveRedisTemplate<>(connectionFactory, serializationContext);
//    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(){
        return new ReactiveKafkaConsumerTemplate<>((KafkaReceiver<String, String>) consumerFactory());
    }

    private Map<String, Object> consumerConfig(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return properties;
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(){
        return new DefaultKafkaConsumerFactory<>(consumerConfig(), new StringDeserializer(), new ErrorHandlingDeserializer<>(new StringDeserializer()));
    }
}
