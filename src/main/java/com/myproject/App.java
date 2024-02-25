package com.myproject;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import com.google.gson.Gson;

public class App {
    final static AWS aws = AWS.getInstance();
    private static final String bucketName = "local-bucket-" + System.currentTimeMillis();
    public static void main(String[] args) {
        if (aws.shutDownFlag) {
            System.out.println("Manager began shutdown process, no longer accepting messages. Try again briefly.");
            return;
        }
        int n = Integer.parseInt(args[args.length-2]);
        boolean terminate = args[args.length-1].equals("y");
        int inputFileCount = (args.length - 2) / 2;
        int filesProcessed = 0;
        try {
            setup();
            System.out.println("Bucket successfully created");
//            aws.createLocalManager();
            aws.checkForManager();
            String queueUrl = aws.getQueueUrl("LocalApps");
            Gson gson = new Gson();
            for(int i = 0; i<inputFileCount; i++) {
                String prefix = args[i].substring(0,args[i].indexOf(".")) + "_";
                String s3FileName = prefix + UUID.randomUUID() + ".txt";
                boolean uploaded = aws.uploadFile(s3FileName, Paths.get(args[i]), bucketName); //random name to the input file of the local because it may not be unique
                if(!uploaded)
                    return;
                HashMap messageData = new HashMap<String, Object>();
                messageData.put("inputFileName", s3FileName);
                messageData.put("outputFileName", args[i + inputFileCount]);
                messageData.put("reviewsPerWorker", n);
                messageData.put("terminate", terminate && i == inputFileCount - 1);
                messageData.put("bucketName", bucketName);
                String messageBody = gson.toJson(messageData);
                if (aws.shutDownFlag) {
                    exit();
                    return;
                }
                aws.sendMessage(messageBody, queueUrl);
            }
            String summaryQueueUrl = aws.getQueueUrl("managerSummaries");
            while (filesProcessed < inputFileCount){
                Message message = aws.waitForMessage(summaryQueueUrl);
                if (message == null )
                    continue;
                String messageBody = message.body();
                Type type = new TypeToken<HashMap<String, Object>>(){}.getType();
                HashMap messageData = gson.fromJson(messageBody, type);
                String bucket = (String) messageData.get("bucketName");
                if(bucket.equals(bucketName)) {
                    System.out.println("Received summary message");
                    String resultsFile = (String) messageData.get("summaryFileName");
                    String outputFileName = (String) messageData.get("outputFileName");
                    createHTML(resultsFile, outputFileName);
                    filesProcessed++;
                    try {
                        aws.deleteMessage(message, summaryQueueUrl);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else  {
                    System.out.println("Received message designated for a different bucket. Resetting visibility.");
                    aws.changeVisibility(summaryQueueUrl,message,0);
                }
            }

            exit();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createHTML(String fileName, String outputFileName) {
        BufferedReader reader = aws.getContent(fileName,bucketName);
        Gson gson = new Gson();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName + "_" + System.currentTimeMillis() + ".html"))) {

            writer.write("<html><body>");
            String line;
            while ((line = reader.readLine()) != null) {
                ReviewResult result = gson.fromJson(line, ReviewResult.class);
                String color = getColorForSentiment(result.sentiment);
                // Writing HTML with links colored based on sentiment and listing named entities and sarcasm detection
                writer.write(String.format("<p><a href='%s' style='color:%s'>%s</a> %s [%s]</p>%n",
                        result.reviewLink, color, result.reviewLink,
                        result.namedEntities.toString(),
                        result.sarcasm ? "Sarcasm detected" : ""));
            }

            writer.write("</body></html>");
            System.out.println("Successfully created HTML file for: " + outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        System.out.println("[DEBUG] Creating bucket");
        aws.createBucket(bucketName);
    }

    private static String getColorForSentiment(int sentiment) {
        switch (sentiment) {
            case 0: return "darkred";
            case 1: return "red";
            case 2: return "black";
            case 3: return "lightgreen";
            case 4: return "darkgreen";
            default: return "";
        }
    }

    private static void exit() {
        System.out.println("Deleting all objects in bucket: " + bucketName);
        aws.deleteAllObjectsInBucket(bucketName);
        System.out.println("Deleting bucket: " + bucketName);
        aws.deleteBucket(bucketName);
        System.out.println("Bucket deleted successfully.");
        System.out.println("Now exiting.");
    }

    static class ReviewResult {
        String reviewLink;
        int sentiment;
        List<String> namedEntities;
        boolean sarcasm;
    }
}
