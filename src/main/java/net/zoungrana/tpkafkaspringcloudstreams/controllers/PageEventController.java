package net.zoungrana.tpkafkaspringcloudstreams.controllers;
import lombok.RequiredArgsConstructor;
import net.zoungrana.tpkafkaspringcloudstreams.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


@RestController
@RequiredArgsConstructor
public class PageEventController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

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

    @GetMapping(path = "/analytics",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence->{
                    Map<String,Long> analysticsResult=new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());
                    Instant now=Instant.now();
                    Instant from=now.minusMillis(5000);
                    KeyValueIterator<Windowed<String>, Long> allResults = windowStore.fetchAll(from, now);
                    //WindowStoreIterator<Long> fetchAll = windowStore.fetch(page, from, now);
                    while (allResults.hasNext()){
                        KeyValue<Windowed<String>, Long> next = allResults.next();
                        analysticsResult.put(next.key.key(),next.value);
                    }
                    return analysticsResult;
                });
    }
}
