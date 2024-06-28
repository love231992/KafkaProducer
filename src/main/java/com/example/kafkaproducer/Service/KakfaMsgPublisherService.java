package com.example.kafkaproducer.Service;

import com.example.kafkaproducer.Dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KakfaMsgPublisherService {

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMsgToTopic(String msg){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("Spring-topic", msg);
        future.whenComplete((result,ex) ->{
           if (ex ==null){
               System.out.println("Sent msg :"+msg+" with offset : "+result.getRecordMetadata().offset());
           } else {
               System.out.println("Unable to send msg : "+msg+" due to "+ex.getMessage());
           }
        });
    }

    public void sendObjectToTopic(Customer customer){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("Object-topic1", customer);
        future.whenComplete((result,ex) ->{
           if (ex ==null){
               System.out.println("Sent msg :"+customer.toString()+" with offset : "+result.getRecordMetadata().offset());
           } else {
               System.out.println("Unable to send msg : "+customer.toString()+" due to "+ex.getMessage());
           }
        });
    }
}
