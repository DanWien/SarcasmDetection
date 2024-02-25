package com.myproject;
import com.amazonaws.services.s3.AmazonS3Client;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-00e95a9222311e8ed";

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();
    private static final int  RETRY_LIMIT = 3;
    private final String WORKER_TAG_VALUE = "WORKER";
    private final int MAX_WORKERS = 8;
    public volatile boolean shutDownFlag = false;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    // S3
    public void createBucket(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String createEC2(String script, String tagName, int numberOfInstances) {
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Role")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s",
                    instanceId);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public boolean uploadFile(String fileName, Path path, String bucketName) {
        int attempt = 1;
        while(attempt <= RETRY_LIMIT){
            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(fileName)
                        .build();
                s3.putObject(putObjectRequest, RequestBody.fromFile(path));      // for later debugging might be an issue with requestBody VS the path only

                System.out.println("File " + fileName + " uploaded successfully to bucket: " + bucketName);
                return true;
            } catch (S3Exception e) {
                System.out.println(":Attempt number " + attempt + " failed: \n" + e.getMessage());
                attempt++;
                if(attempt > RETRY_LIMIT) {
                    System.out.println("Failed to upload file after " + RETRY_LIMIT + " attempts.");
                    return false;
                }
                try {
                    // Wait before retrying, wait time increases with each attempt
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        System.out.println("failed to upload file: " + fileName + " to bucket: " + bucketName);
        return false;
    }

    public void sendMessage(String messageBody, String queueUrl) {
        int attempt = 1;
        while(attempt <= RETRY_LIMIT) {
            try {
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(messageBody)
                        .build());
                return;
            } catch (SqsException e) {
                System.out.println("Attempt number " + attempt + " failed: \n" + e.getMessage());
                attempt++;
                if(attempt > RETRY_LIMIT) {
                    System.out.println("Failed to send message after " + RETRY_LIMIT + " attempts.");
                    return;
                }
                try {
                    // Wait before retrying, wait time increases with each attempt
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public String createQueue(String queueName) {
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            sqs.createQueue(createQueueRequest);
            return getQueueUrl(queueName);
        } catch (SqsException e) {
            System.out.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    public void createLocalManager() {
        System.out.println("creating local manager");
    }

    public void createLocalWorker() {
        System.out.println("creating worker");
    }

    public void checkForManager() {
        // Filter for instances tagged as "MANAGER"
        Filter tagFilter = Filter.builder().name("tag:Role").values("MANAGER").build();

        // Filter for instances that are in the running state
        Filter statusFilter = Filter.builder().name("instance-state-name").values("running").build();

        DescribeInstancesRequest searchManagerRequest = DescribeInstancesRequest.builder()
                .filters(tagFilter, statusFilter)
                .build();
        DescribeInstancesResponse searchManagerResponse = ec2.describeInstances(searchManagerRequest);

        boolean managerFoundAndRunning = false;

        // Iterate through all reservations and instances
        for (Reservation reservation : searchManagerResponse.reservations()) {
            for (Instance instance : reservation.instances()) {
                // Check if the instance is running (instance state code 16 means running)
                if (instance.state().code() == 16) {
                    managerFoundAndRunning = true;
                    break; // Manager found running, no need to check further
                }
            }
            if (managerFoundAndRunning) break; // Break outer loop if manager found running
        }

        String managerScript =
                "#!/bin/bash\n" +
                "sudo yum update -y\n" +
                "sudo yum install ec2-instance-connect\n" +
                "sudo yum install -y java-21-amazon-corretto\n" +
                "aws s3 cp s3://this-is-my-first-bucket-dn2/Manager.jar ./Manager.jar\n" +
                "sudo java -Xms4g -Xmx4g -jar Manager.jar\n" +
                "shutdown -h now";

        if (!managerFoundAndRunning) {
            System.out.println("No running Manager found. Creating a new one.");
            String managerInstanceId = createEC2(managerScript, "MANAGER", 1);
            if (managerInstanceId.equals(""))
                System.out.println("Failed to create a manager");
            else System.out.println("\nManager created");
        } else {
            System.out.println("Manager is already running");
        }
    }


    public String getQueueUrl(String queueName) {
        while (true) {
            try {
                GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
                String queueUrl = getQueueUrlResponse.queueUrl();
                return queueUrl;
            } catch (QueueDoesNotExistException e) {
                continue;
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }

    public Message waitForMessage(String queueUrl) {
        int attempt = 1;
        while(attempt <= RETRY_LIMIT){
//            System.out.println("Waiting for a message from " + queueUrl + " Queue");
            try {
                ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .maxNumberOfMessages(1)
                                .waitTimeSeconds(20)
                                .build()
                );
                if (!receiveMessageResponse.messages().isEmpty()) {
                    return receiveMessageResponse.messages().get(0);
                }
                else attempt++;
            } catch (SqsException e) {
                System.out.println("Attempt number " + attempt + " failed: \n" + e.getMessage());
                attempt++;
                if(attempt > RETRY_LIMIT) {
                    System.out.println("Failed to receive message after " + RETRY_LIMIT + " attempts.");
                    return null;
                }
                try {
                    Thread.sleep(attempt*1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return null;
    }

    public void deleteMessage(Message message, String queueUrl){
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            System.out.println("Failed deleting message: " + message.messageId() + e.getMessage());
        }
    }


    public BufferedReader getContent(String fileName, String bucketName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3.getObject(getObjectRequest), StandardCharsets.UTF_8));
            return reader;
        } catch (S3Exception e){
            System.out.println("Error occurred while downloading the file: " + e.getMessage());
        }
        return null;
    }

    public void changeVisibility(String queueUrl, Message message, int i) {
        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .visibilityTimeout(i)
                .build());
    }

    public int countActiveWorkers() {
        // Filter for instances tagged as workers and in the running state
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(Filter.builder().name("tag:Role").values(WORKER_TAG_VALUE).build(),
                        Filter.builder().name("instance-state-name").values("running").build())
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(request);

        int count = 0;
        for (Reservation reservation : response.reservations()) {
            count += reservation.instances().size();
        }
        return count;
    }


    public void startWorkerInstance(String workerScript) {
        if (countActiveWorkers() < MAX_WORKERS) {
            createEC2(workerScript, WORKER_TAG_VALUE, 1);
        }
    }

    public void deleteQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);
        } catch (SqsException e) {
            System.out.println("Failed deleting que: " + queueUrl + "because: " + e.getMessage());
        }
    }


    public void deleteAllObjectsInBucket(String bucketName) {
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
        ListObjectsResponse res = s3.listObjects(listObjects);
        for (S3Object s3Object : res.contents()) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build());
        }
    }

    public void deleteBucket(String bucketName) {
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
    }

}
