import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
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

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;


public class AWSHandler {
    private static final String AMI_ID = "ami-00068cd7555f543d5";
    private static final String ORI_KEY_PAIR = "dsps";
    private static final String BENTZI_KEY_PAIR = "dsps";
    private static final String ORI_ROLE = "arn:aws:iam::049413562759:instance-profile/admin";
    private static final String BENTZI_ROLE = "arn:aws:iam::353189555793:instance-profile/admin";
    private static final String SECURITY_GROUP = "launch-wizard-1";
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    private static Ec2Client ec2;
    public static Boolean isBentzi = false;
    private static S3Client s3;
    private static Region region = Region.US_EAST_1;
    private static SqsClient sqs;


    public static void ec2EstablishConnection() {
        ec2 = Ec2Client.create();
    }

    public static List<Instance> ec2IsInstanceRunning(String instanceName) {

        List<Reservation> reservList = ec2.describeInstances().reservations();

        //iterate on reservList and call

        for (Reservation reservation : reservList) {
            List<Instance> instances = reservation.instances();
            for (Instance instance : instances) {
                List<Tag> tags = instance.tags();
                List<Tag> nameTags = tags.stream().filter(tag ->
                        (tag.key().equals("name") && tag.value().equals(instanceName)
                        )).collect(Collectors.toList());
                if (nameTags.size() > 0 && instance.state().name().equals(InstanceStateName.RUNNING)) {
                    return instances;
                }
            }
        }

        return null;
    }

    public static List<Instance> ec2CreateInstance(String instanceName, Integer numOfInstance, String fileToRun, String bucketName, List<String> args, InstanceType instanceType) {
        List<Instance> instances = ec2IsInstanceRunning(instanceName);
        if (instances != null) {
            logger.info("Found an EC2 running instance, not creating a new one");
            return instances;
        }
        RunInstancesRequest request = RunInstancesRequest.builder()
                .imageId(AMI_ID)
                .instanceType(instanceType)
                .minCount(1)
                .maxCount(numOfInstance)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(isBentzi ? BENTZI_ROLE : ORI_ROLE).build())
                .securityGroups(SECURITY_GROUP)
                .userData(generateExecutionScript(bucketName, fileToRun, args))
                .keyName(isBentzi ? BENTZI_KEY_PAIR : ORI_KEY_PAIR)
                .build();


        RunInstancesResponse runInstancesResponse = ec2.runInstances(request);
        String instance_id = runInstancesResponse.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("name")
                .value(instanceName)
                .build();

        CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                .resources(instance_id)
                .tags(tag)
                .build();
        try {
            ec2.createTags(createTagsRequest);

            logger.info(
                    "Successfully started EC2 instance {} based on AMI {}",
                    instance_id, AMI_ID);
        } catch (Ec2Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }

        return runInstancesResponse.instances();
    }

    public static void s3EstablishConnection() {
        s3 = S3Client.builder().region(region).build();
    }


    public static CreateBucketResponse s3CreateBucket(String bucketName) {

        // Create bucket
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .build();
        CreateBucketResponse createdBucket = s3.createBucket(createBucketRequest);
        logger.info("Bucket created");
        logger.info("Bucket Name: {}", bucketName);
        return createdBucket;
    }

    public static String s3GenerateBucketName(String name) {
        return name + '-' + System.currentTimeMillis();
    }

    public static void s3DeleteBucket(String bucketName) {
        s3DeleteBucketContent(bucketName);
        s3DeleteEmptyBucket(bucketName);
    }

    private static void s3DeleteBucketContent(String bucketName) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response listObjectsV2Response;
        do {
            listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build());
            }

            listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                    .continuationToken(listObjectsV2Response.nextContinuationToken())
                    .build();

        } while (listObjectsV2Response.isTruncated());

    }

    private static void s3DeleteEmptyBucket(String bucket) {
        // Delete empty bucket
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    public static void s3Upload(String bucketName, File toUpload) {
        logger.info("Beginning uploading file {}", toUpload.getName());
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(toUpload.getName()).build();
        s3.putObject(putObjectRequest, RequestBody.fromFile(toUpload));
        logger.info("Finished uploading file {}", toUpload.getName());
    }

    public static void s3UploadFiles(String bucketName, List<String> inputFiles) {
        logger.info("Beginning uploading input files: {}", inputFiles.toString());
        inputFiles.stream().forEach(file -> s3Upload(bucketName, getFileFromResources(file)));
        logger.info("Finished uploading input files: {}", inputFiles.toString());

    }


    public static void s3Download(String bucketName, String key, File downloadTo) {
        logger.info("Beginning downloading file {}", key);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        s3.getObject(getObjectRequest, downloadTo.toPath());
        logger.info("Finished download file {}", key);
    }

    public static void s3DownloadFiles(String bucketName, List<String> keys, String path) {
        logger.info("Beginning downloading input files: {}", keys.toString());
        keys.forEach(key -> {
            File newFile = new File(path + key);
            s3Download(bucketName, key, newFile);
        });
        logger.info("Finished downloading input files: {}", keys.toString());

    }

    public static File getFileFromResources(String fileName) {

        ClassLoader classLoader = LocalApplication.class.getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }


    public static void sqsEstablishConnection() {
        sqs = SqsClient.builder().region(region).build();
    }

    public static String sqsGetQueueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    public static String sqsCreateQueue(String queueName, Boolean isFifo) {
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        if (isFifo) {
            attributes.put(QueueAttributeName.FIFO_QUEUE, isFifo.toString());
            attributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, isFifo.toString());
        }
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).attributes(attributes)
                .build();
        sqs.createQueue(createQueueRequest);
        String queueUrl = sqsGetQueueUrl(queueName);
        logger.info("New queue created {}, {}", queueName, queueUrl);
        return queueUrl;
    }

    public static void sendMessageToSqs(String queueUrl, String message, Boolean isFifo) {
        SendMessageRequest sendMessageRequest;
        if (isFifo) {
            sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .messageGroupId("group1")
                    .build();
        } else {
            sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();
        }
        sqs.sendMessage(sendMessageRequest);
    }

    public static List<Message> receiveMessageFromSqs(String queueUrl, int timeout) {
        return receiveMessageFromSqs(queueUrl, timeout, 5);
    }

    public static List<Message> receiveMessageFromSqs(String queueUrl, int timeout, int maxNumberOfMessages) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(timeout)
                .build();
        return sqs.receiveMessage(receiveMessageRequest).messages();
    }

    public static void deleteMessageFromSqs(String queueUrl, Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
        logger.info("Message {} deleted from {}", message.body(), queueUrl);
    }


    private static String generateExecutionScript(String bucketName, String executableJar, List<String> args) {
        List<String> cmd = new ArrayList<>();
        cmd.add("#!/bin/bash");
        cmd.add("sudo su");
        cmd.add("amazon-linux-extras enable corretto8");
        cmd.add("yum clean metadata");
        cmd.add("yum install -y java-1.8.0-amazon-corretto");
        cmd.add("yum install -y git");
        cmd.add("yum install -y maven");
        cmd.add("git clone https://github.com/keila2k/dsps-assignment1.git");
        cmd.add("cd dsps-assignment1");
        cmd.add("while [ ! -d \"target\" ]");
        cmd.add("do");
        cmd.add("mvn package");
        cmd.add("done");
        cmd.add("cd target");
        cmd.add("sync; echo 3 > /proc/sys/vm/drop_caches");

        String makeJar = "java -Xmx2g -jar " + executableJar;

        // if there are args this is data script of worker
        if (args != null) {
            for (String arg : args) {
                makeJar += " " + arg;
            }
        }
        cmd.add(makeJar + " > outputLog.txt");
        cmd.add("");
        String initScript = StringUtils.join(cmd, "\n");
        logger.info("cmd {}", initScript);
        String str = Base64.getEncoder().encodeToString(initScript.getBytes());
        logger.info("base64 {}", str);
        return str;
    }

    public static TerminateInstancesResponse terminateEc2Instance(Instance instance) {
        logger.info("Beginning termination of instance: {}", instance.toString());
        TerminateInstancesRequest terminateInstancesRequest = TerminateInstancesRequest.builder()
                .instanceIds(instance.instanceId())
                .build();

        TerminateInstancesResponse terminateInstancesResponse = ec2.terminateInstances(terminateInstancesRequest);
        logger.info("Finished termination of instance: {}", instance.toString());
        return terminateInstancesResponse;
    }

    public static ResponseInputStream<GetObjectResponse> s3ReadFile(String bucketName, String inputFile) {
        logger.info("Beginning reading file {}", inputFile);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(inputFile).build();
        ResponseInputStream<GetObjectResponse> inputStream = s3.getObject(getObjectRequest);
        logger.info("Successful got file {}", inputFile);
        return inputStream;
    }

    public static String getAccessKeyId() {
        AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        return awsCredentialsProvider.resolveCredentials().accessKeyId();
    }
}
