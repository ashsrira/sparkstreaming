package com.kafka.producer;

import com.twitter.record.MyTwitterRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class StreamingClient{

    public static void main(String[] args) throws Exception {
        /* Init the kafka producer */
        final MyKafkaProducer producer = new MyKafkaProducer();
        final ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("")
                .setOAuthConsumerSecret("")
                .setOAuthAccessToken("")
                .setOAuthAccessTokenSecret("");
        System.setProperty("java.net.useSystemProxies", "true");


        final StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                String location = null;
                String name = status.getUser().getName();
                String tweet = status.getText();
                Long id = status.getUser().getId();
                try {
                    location = status.getPlace().getCountry();
                }
                catch (java.lang.NullPointerException e) {
                    e.printStackTrace();
                }
                MyTwitterRecord record = new MyTwitterRecord();
                record.setId(id);
                record.setName(name);
                record.setTweet(tweet);
                record.setLocation(location);
                try {
                    producer.sendMessageToKafkaBroker(record);
                }
                catch (Exception e) {
                   e.printStackTrace();
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long l, long l1) {
                System.out.println("Got scrub_geo event userId:" + l + " upToStatusId:" + l1);
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                System.out.println("Got stall warning:" + stallWarning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        FilterQuery query = new FilterQuery();
        query.track("jobs,hiring");
        twitterStream.filter(query);
        System.out.println("DONE");
    }
}