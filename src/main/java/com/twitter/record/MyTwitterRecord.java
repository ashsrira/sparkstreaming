package com.twitter.record;

public class MyTwitterRecord {

    private String name;
    private String tweet;
    private String location;
    private Long id;

    public MyTwitterRecord() {

    }


    public MyTwitterRecord(String name, String tweet, Long id, String location){
        this.name = name;
        this.tweet = tweet;
        this.id = id;
        this.location = location;
    }

    public MyTwitterRecord(String name, String tweet){
        this.name = name;
        this.tweet = tweet;
    }

    public String getName() {
        return name;
    }

    public Long getId() {
        return id;
    }

    public String getLocation() {
        return location;
    }

    public String getTweet() {
        return tweet;
    }

    public void setName(java.lang.String name) {
        this.name = name;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override public String toString() {
        return "(" + name + ", " + tweet + ", " + id + ", " + location + ")";
    }
}
