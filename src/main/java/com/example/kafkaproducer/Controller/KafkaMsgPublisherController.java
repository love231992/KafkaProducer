package com.example.kafkaproducer.Controller;

import com.example.kafkaproducer.Service.KakfaMsgPublisherService;
import com.example.kafkaproducer.Dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class KafkaMsgPublisherController {
    @Autowired
    private KakfaMsgPublisherService kakfaMsgPublisherService;

    @GetMapping("/publish/{msg}")
    public ResponseEntity<?> publishMsg(@PathVariable String msg){
        for (int i = 0; i < 10; i++) {
            kakfaMsgPublisherService.sendMsgToTopic(msg + i);
        }
       return ResponseEntity.status(HttpStatus.OK).body("Msg Sent successfully");
    }

    @GetMapping("/publishObject")
    public ResponseEntity<?> publishMsg(@RequestBody Customer customer){
            kakfaMsgPublisherService.sendObjectToTopic(customer);
       return ResponseEntity.status(HttpStatus.OK).body("Object Sent Successfully");
    }
}
