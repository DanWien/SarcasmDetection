package com.myproject;
import java.util.List;

public class Review {
    private String id;
    private String link;
    private String title;
    private String text;
    private int rating;
    private String author;
    private String date;


    // Getters and setters (or use Lombok for automatic generation)
    public String getText() {
        return text;
    }
    public String getLink() {
        return link;
    }
    public int getRating() {
        return rating;
    }
}

class ReviewsGroup {
    String title; // Main title for all reviews
    List<Review> reviews;
    public List<Review> getReviews() {
        return reviews;
    }
    public String getTitle() {
        return title;
    }

}
