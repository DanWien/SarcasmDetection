package com.myproject;
import com.google.gson.Gson;
import software.amazon.awssdk.services.sqs.model.Message;
import java.io.*;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Worker {
    static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    private static String workersTaskQueueUrl;
    private static String workersAnswersQueueUrl;
    final static AWS aws = AWS.getInstance();
    private static volatile boolean jobDone = false;


    public Worker() {
        workersTaskQueueUrl = aws.getQueueUrl("TasksQueue");
        workersAnswersQueueUrl = aws.getQueueUrl("AnswersQueue");
    }
    public String handleJob(String fileName, String bucketName) {
        String content;
        try (BufferedReader reader = aws.getContent(fileName, bucketName)) {
            StringBuilder contentBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                contentBuilder.append(line);
            }
            content = contentBuilder.toString();
        } catch (IOException e) {
            System.err.println("Failed to read content: " + e.getMessage());
            return "";
        }

        List<String> results = new ArrayList<>();
        Gson gson = new Gson();
        Type reviewListType = new TypeToken<List<Review>>() {}.getType();
        List<Review> reviews = gson.fromJson(content, reviewListType);
        for(Review r : reviews){
            int sentiment = sentimentAnalysisHandler.findSentiment(r.getText());
            List<String> nerList = namedEntityRecognitionHandler.getEntities(r.getText());
            boolean sarcasm = Math.abs(sentiment-r.getRating()) > 2;
            String result = formatReviewResult(r, sentiment, nerList, sarcasm);
            results.add(result);
        }
        String localFileName = "resultsFile_"+System.currentTimeMillis();
        File resultFile = new File(localFileName);
        try {
            FileWriter fileWriter = new FileWriter(resultFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (String result : results) {
                bufferedWriter.write(result);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
            System.out.println("Job done : Successfully processed the file.");
        } catch (IOException e) {
                System.out.println("An error occurred while writing to the file: " + e.getMessage());
                return "";
        }
        String resultFileName = "processed_reviews_" + UUID.randomUUID() + ".txt";
        aws.uploadFile(resultFileName, Paths.get(localFileName), bucketName);
        resultFile.delete();
        return resultFileName;
    }

    public static String formatReviewResult(Review review, int sentiment, List<String> nerList, boolean sarcasm) {
        Gson gson = new Gson();
        HashMap resultObject = new HashMap<String, Object>();
        resultObject.put("reviewLink", review.getLink());
        resultObject.put("sentiment", sentiment);
        resultObject.put("sarcasm", sarcasm);
        resultObject.put("namedEntities", nerList);
        return gson.toJson(resultObject);
    }

    public static void ensureVisibility(Message message) {
        while (!jobDone) {
            try {
                Thread.sleep(15000);
                System.out.println("Second Thread: Woke up to extend visibility for message_" + message.messageId()+ " to remain hidden while being handled");
                aws.changeVisibility(workersTaskQueueUrl, message, 30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis() - startTime < 90000) {
            Message message = aws.waitForMessage(workersTaskQueueUrl);
            if (message == null) {
                continue;
            }
            System.out.println("Received message_" + message.messageId()+ " from Manager");
            executor.submit(() -> ensureVisibility(message));
            String messageBody = message.body();
            Gson gson = new Gson();
            Type type = new TypeToken<HashMap<String, Object>>() {
            }.getType();
            HashMap<String, Object> messageData = gson.fromJson(messageBody, type);
            String jobFileName = (String) messageData.get("jobFileName");
            String bucketName = (String) messageData.get("bucketName");
            String inputFileName = (String) messageData.get("inputFileName");
            String resultFileName = worker.handleJob(jobFileName, bucketName);
            if (resultFileName.equals("")) {
                executor.shutdown();
                System.out.println("Failed to upload the result file to bucket " + bucketName);
                return;
            }
            messageData = new HashMap<String, Object>();
            messageData.put("resultFileName", resultFileName);
            messageData.put("bucketName", bucketName);
            messageData.put("inputFileName", inputFileName);
            messageBody = gson.toJson(messageData);
            aws.sendMessage(messageBody, workersAnswersQueueUrl);
            try {
                jobDone = true;
                aws.deleteMessage(message, workersTaskQueueUrl);
            } catch (Exception e) {
                System.out.println("Failed deleting message: " + message.messageId() + " from workers queue");
                e.printStackTrace();
            }
            startTime = System.currentTimeMillis();
            jobDone = false;
            System.out.println("updated startTime to ::" +startTime);
        }
        System.out.println("Finished working");
        executor.shutdown();
    }
}



