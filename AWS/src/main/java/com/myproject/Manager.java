package com.myproject;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.gson.Gson;

public class Manager {

    final static AWS aws = AWS.getInstance();

    private static String localsQueueUrl;
    private static String workersTaskQueueUrl;
    private static String workersAnswersQueueUrl;
    private static String managerSummaryQueueUrl;
    private static Map<String, JobTracker> jobTrackers;
    private static ExecutorService executor;
    private static volatile boolean terminateRequested = false;

    private static String workerScript =
            "#!/bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install ec2-instance-connect\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "aws s3 cp s3://this-is-my-first-bucket-dn2/Worker.jar ./Worker.jar\n" +
            "sudo java -Xms4g -Xmx4g -jar Worker.jar\n" +
            "shutdown -h now";


    public Manager() {
        this.localsQueueUrl = aws.createQueue("LocalApps");
        this.workersTaskQueueUrl = aws.createQueue("TasksQueue");
        this.workersAnswersQueueUrl = aws.createQueue("AnswersQueue");
        this.managerSummaryQueueUrl = aws.createQueue("managerSummaries");
        this.jobTrackers = Collections.synchronizedMap(new HashMap<>());
        this.executor = Executors.newFixedThreadPool(5);
    }

    public static void main(String[] args) {
        System.out.println("Starting manager's main method");
        Manager manager = new Manager();
        System.out.println("Now waiting for messages");
        manager.executor.submit(() -> listenToLocalAppQueue());
    }
    private static void listenToLocalAppQueue() {
        while (!terminateRequested) {
            // Logic to listen to the queue and submit parsing/preparation tasks
            Message message = aws.waitForMessage(localsQueueUrl);
            if (message != null) {
                System.out.println("Message received from a local app, processing...");
                executor.submit(() -> processMessage(message));
                try {
                    System.out.println("Message is being processed, removing the message from the LocalsQue");
                    aws.deleteMessage(message, localsQueueUrl);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else System.out.println("Didn't get a message in this cycle");
        }
        System.out.println("Left the listenToLocalAppQueue while loop - no longer listening.");
    }

    private static void processMessage(Message message) {
        String messageBody = message.body();
        //process message, extract input file, output file etc.
        Gson gson = new Gson();
        Type type = new TypeToken<HashMap<String, Object>>() {}.getType();
        HashMap<String, Object> messageData = gson.fromJson(messageBody, type);
        String inputFileName = (String) messageData.get("inputFileName");
        String outputFileName = (String) messageData.get("outputFileName");
        int n = ((Double) messageData.get("reviewsPerWorker")).intValue(); // Gson converts numbers to Double
        boolean terminate = (Boolean) messageData.get("terminate");
        String bucketName = (String) messageData.get("bucketName");
        System.out.println("Parsed message: inputFileName: " + inputFileName + ", reviewsPerWorker: " + n + ", bucketName: " + bucketName);
        BufferedReader reader = aws.getContent(inputFileName, bucketName);
        System.out.println("Retrieved content of file: " + inputFileName + " from bucket:" + bucketName);
        String jsonContent = reader.lines().collect(Collectors.joining("\n"));
        String[] jsonObjects = jsonContent.split("(?=\\{\\\"title\\\":)");
        List<ReviewsGroup> titles = new ArrayList<>();
        for (String jsonObject : jsonObjects) {
            if (!jsonObject.trim().isEmpty()) {
                ReviewsGroup group = gson.fromJson(jsonObject, ReviewsGroup.class);
                titles.add(group);
            }
        }
        System.out.println("Created a list of all the reviews, assigning Jobs to Workers now.");
        executor.submit(() -> splitWorkAndStartWorkers(titles,n,bucketName,inputFileName, outputFileName));
        if(terminate) {
            terminateRequested = true;
            System.out.println("Terminate request: No longer listening to messages from local apps");
        }
    }

    private static void splitWorkAndStartWorkers(List<ReviewsGroup> titles, int n , String bucketName, String inputFileName, String outputFileName) {
        System.out.println("Splitting file: " + inputFileName + " from bucket: " + bucketName);
        Gson gson = new Gson();
        int count = 0;
        List<Review> mergedReviews = new ArrayList<>();
        for (ReviewsGroup g : titles) {
            List<Review> reviews = g.getReviews();
            mergedReviews.addAll(reviews);
        }
        for (int i = 0; i < mergedReviews.size(); i += n / 2) {
            // Determine the end index for the sublist, ensuring we don't exceed the list's bounds
            int end = Math.min(i + n / 2, mergedReviews.size());
            List<Review> reviewSublist = mergedReviews.subList(i, end);
            String json = gson.toJson(reviewSublist);
            String jobFileName = "job_" + (++count) + "_" + inputFileName + ".json";
            try {
                Path jobFilePath = Files.createTempFile(jobFileName, ".json");
                Files.write(jobFilePath, json.getBytes(StandardCharsets.UTF_8));
                System.out.println("Uploading job number " + count + " named: " + jobFileName + " to bucket: " + bucketName);
                boolean uploaded = aws.uploadFile(jobFileName, jobFilePath, bucketName);
                if(!uploaded) {
                    System.out.println("Failed to upload " + jobFileName + "to bucket: " + bucketName);
                }
            }
            catch (IOException e){
                System.out.println("Error occurred while uploading : " + e.getMessage());
            }

            HashMap<String, String> workerMessageData = new HashMap<>();
            workerMessageData.put("bucketName", bucketName);
            workerMessageData.put("jobFileName", jobFileName);
            workerMessageData.put("inputFileName", inputFileName);
            String workerMessageBody = gson.toJson(workerMessageData);
            aws.sendMessage(workerMessageBody, workersTaskQueueUrl);
        }

        for (int i = 0; i <= count/2; i++) {
            aws.startWorkerInstance(workerScript);
            aws.createLocalWorker();
            try {
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {
                System.out.println("Interrupted Sleep");
            }
        }
        JobTracker tracker = new JobTracker(inputFileName, count, bucketName, outputFileName);
        jobTrackers.put(inputFileName, tracker);
        executor.submit(() -> receiveMessagesFromWorkers(jobTrackers));
        if(terminateRequested && !aws.shutDownFlag) {
            System.out.println("ShutDownFlag changed to true.");
            aws.shutDownFlag = true;
            shutdownProcess();
        }
    }

    public static void receiveMessagesFromWorkers(Map<String, JobTracker> jobTrackers) {
        Gson gson = new Gson();
        while (!jobTrackers.isEmpty() && !Thread.currentThread().isInterrupted()) {
            Message messageFromWorker = aws.waitForMessage(workersAnswersQueueUrl);
            if (messageFromWorker == null) continue;
            System.out.println("Received message from worker");
            String messageBody = messageFromWorker.body();
            Type type = new TypeToken<HashMap<String, String>>() {}.getType();
            HashMap<String, String> workerResponse = gson.fromJson(messageBody, type);
            String partId = workerResponse.get("resultFileName");
            String inputFileName = workerResponse.get("inputFileName");

            JobTracker tracker = jobTrackers.get(inputFileName);
            if (tracker != null) {
                tracker.updatePartResult(partId);

                if (tracker.isComplete()) {
                    System.out.println("All parts of file: " + inputFileName + " were successfully processed");
                    // All parts of this job are complete
                    aggregateResults(tracker);
                    jobTrackers.remove(inputFileName); // Remove the tracker if the job is complete
                }
            }
            try {
                aws.deleteMessage(messageFromWorker, workersAnswersQueueUrl);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void shutdownProcess() {
        try {
            while (aws.countActiveWorkers() > 0) {
                System.out.println("Waiting for workers to finish...");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Manager interrupted while waiting for workers to finish.");
                    return;
                }
            }

            System.out.println("Beginning shutdown process.");
            // Wait for all jobTrackers to mark their jobs as complete
            while (!jobTrackers.isEmpty()) {
                Thread.sleep(5000);
            }
            System.out.println("All tasks completed. Deleting queues...");
            aws.deleteQueue(localsQueueUrl);
            aws.deleteQueue(workersTaskQueueUrl);
            aws.deleteQueue(workersAnswersQueueUrl);
            aws.deleteQueue(managerSummaryQueueUrl);
            executor.shutdown();

            System.out.println("Manager shutdown complete.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Shutdown process was interrupted: " + e.getMessage());
        }
    }

    public static void aggregateResults(JobTracker tracker) {
        String bucketName = tracker.getBucketName();
        String InputFileName = tracker.getInputFileName();
        System.out.println("Aggregating results of file: " + InputFileName);
        List<String> processedPartFileNames = tracker.getProcessedParts();
        Path summaryFilePath = null;
        try {
            summaryFilePath = Files.createTempFile("summary", ".json");
            try (BufferedWriter writer = Files.newBufferedWriter(summaryFilePath, StandardOpenOption.CREATE)) {
                for (String partFileName : processedPartFileNames) {
                    BufferedReader reader = aws.getContent(partFileName, bucketName);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine(); // Maintain the file format with one JSON object per line
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error while creating summary file: " + e.getMessage());
            return;
        }
        String summaryFileName = "final_summary_for_" + InputFileName + ".json";
        aws.uploadFile(summaryFileName, summaryFilePath, bucketName);
        System.out.println("Aggregated results uploaded to: " + summaryFileName);

        Gson gson = new Gson();
        HashMap<String, String> localAppMessageData = new HashMap<>();
        localAppMessageData.put("bucketName", bucketName);
        localAppMessageData.put("summaryFileName", summaryFileName);
        localAppMessageData.put("outputFileName", tracker.getOutputFileName());
        String localAppMessageBody = gson.toJson(localAppMessageData);
        aws.sendMessage(localAppMessageBody, managerSummaryQueueUrl);
        // Cleanup temporary summary file
        try {
            Files.deleteIfExists(summaryFilePath);
        } catch (IOException e) {
            System.out.println("Error while deleting summary file: " + e.getMessage());
        }
    }


    public static class JobTracker {
        private final String inputFileName;
        private final String outputFileName;
        private final int totalParts;
        private final List<String> processedParts;
        private final String bucketName;

        public JobTracker(String inputFileName, int totalParts, String bucketName, String outputFileName) {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            this.totalParts = totalParts;
            this.bucketName = bucketName;
            this.processedParts = Collections.synchronizedList(new ArrayList<>());
        }

        public synchronized void updatePartResult(String partFileName) {
            processedParts.add(partFileName);
        }

        public boolean isComplete() {
            return processedParts.size() == totalParts;
        }

        public List<String> getProcessedParts() {
            return processedParts;
        }

        public String getInputFileName() {
            return inputFileName;
        }

        public String getBucketName() {
            return bucketName;
        }

        public String getOutputFileName() {
            return outputFileName;
        }
    }
}


