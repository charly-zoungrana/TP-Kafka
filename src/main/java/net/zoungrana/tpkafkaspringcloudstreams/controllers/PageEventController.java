package net.zoungrana.tpkafkaspringcloudstreams.controllers;
import lombok.RequiredArgsConstructor;
import net.zoungrana.tpkafkaspringcloudstreams.events.PageEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;


@RestController
@RequiredArgsConstructor
public class PageEventController {

    private final StreamBridge streamBridge;

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam(name="name") String name,@RequestParam(name="topic") String topic){
        PageEvent pageEvent=new PageEvent(
                name,
                Math.random()>0.5?"U1":"U2",
                new Date(),
                10 + new Random().nextInt(10000)
        );

        streamBridge.send(topic,pageEvent);

        return pageEvent;
    }
}
