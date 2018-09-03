package com.kafka.producer;
import java.net.*;
import java.io.*;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import twitter4j.*;
import twitter4j.Query;
import twitter4j.conf.ConfigurationBuilder;

public class streamingClient{

    private static final String SEARCH_TERM    = "job";
    private static final int TWEETS_PER_QUERY  = 100;

    public static void getRateLimitStatus(Twitter twitter) {
        try {
            Map<String ,RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus();
            for (String endpoint : rateLimitStatus.keySet()) {
                RateLimitStatus status = rateLimitStatus.get(endpoint);
                System.out.println("Endpoint: " + endpoint);
                System.out.println(" Limit: " + status.getLimit());
                System.out.println(" Remaining: " + status.getRemaining());
                System.out.println(" ResetTimeInSeconds: " + status.getResetTimeInSeconds());
                System.out.println(" SecondsUntilReset: " + status.getSecondsUntilReset());
            }
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get rate limit status: " + te.getMessage());
            System.exit(-1);
        }
    }

    public static Query getQuery() {
        /* function to get query */
        Query q = new Query(SEARCH_TERM);
        q.setResultType(Query.RECENT);
        q.setCount(10);
        q.setLang("ENG");

        return q;
    }
    public static void main(String[] args) throws Exception {
        /* Init the kafka producer */
        final kafkaProducer x = new kafkaProducer();
        final ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("")
                .setOAuthConsumerSecret("")
                .setOAuthAccessToken("")
                .setOAuthAccessTokenSecret("");
        System.setProperty("java.net.useSystemProxies", "true");
        final StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                System.out.println(status.getUser().getName() + " : " + status.getText());
                //convert to AVRO

                // Create the twitter record

                twitterRecord record = new twitterRecord();
                record.setName(status.getUser().getName());
                record.setTweet(status.getText());
                record.setId(status.getUser().getId());
                try {
                    record.setLocation(status.getPlace().toString());
                }
                catch (Exception e) {
                    record.setLocation("NULL");
                }
                try {
                    byte[] data = SerializationUtils.serialize(record);
                    x.sendMessage2(data);
                }
                catch (Exception e) {
                    System.out.println(e.toString());
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