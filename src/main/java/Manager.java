import com.google.gson.Gson;
import dto.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {
    private static final Logger logger = LoggerFactory.getLogger(AWSHandler.class);
    static String applicationQueueUrl;
    static String managerQueueUrl;
    static String doneTasksQueueUrl;
    static String workersQueueUrl;
    static String bucketName;
    static int workersFilesRatio;
    private static Gson gson = new Gson();
    static BufferedReader reader;

    private static Map<String, FileHandler> inputFileHandlersMap = new ConcurrentHashMap<>();
    private static Boolean isTerminate = false;
    private static List<Instance> workersList = new ArrayList<>();


    public static void main(String[] args) {
        configureLogger();
        AWSHandler.sqsEstablishConnection();
        AWSHandler.ec2EstablishConnection();
        AWSHandler.s3EstablishConnection();
        Options options = new Options();
        ExecutorService pool = Executors.newFixedThreadPool(3);


        parseProgramArgs(args, options);
        workersQueueUrl = AWSHandler.sqsCreateQueue("workersQ", false);
        doneTasksQueueUrl = AWSHandler.sqsCreateQueue("doneTasksQ", false);
        Runnable getInputFilesMessageRunnable = new Runnable() {
            @Override
            public void run() {
                getInputFilesMessage(); // 1
            }
        };
        Runnable handleInputFilesRunnable = new Runnable() {
            @Override
            public void run() {
                handleInputFiles(); // 2
            }
        };
        Runnable handleDoneTasksRunnable = new Runnable() {
            @Override
            public void run() {
                while (!isTerminate || !isFinishedDoneTasks()) handleDoneTasks(); // 3
                terminateWorkersIfNeeded(); // 4
            }
        };
        pool.execute(getInputFilesMessageRunnable);
        pool.execute(handleInputFilesRunnable);
        pool.execute(handleDoneTasksRunnable);
    }

    private static void terminateWorkersIfNeeded() {
        synchronized (isTerminate) {
            if (isTerminate) {
                logger.info("Terminate Manager");
                workersList.forEach(AWSHandler::terminateEc2Instance);
            }
        }
    }

    private static boolean isFinishedDoneTasks() {
        return inputFileHandlersMap.values().size() > 0 && inputFileHandlersMap.values().stream().allMatch(fileHandler -> {
            Boolean finishedSendingReview = fileHandler.getFinishedSendingReview();
            int numOfSentReviews = fileHandler.getNumOfSentReviews().get();
            int numOfHandledReviews = fileHandler.getNumOfHandledReviews().get();
            return finishedSendingReview && numOfSentReviews == numOfHandledReviews;
        });
    }


    private static void handleDoneTasks() {
        logger.info("Start handling done tasks");
        List<Message> messages = AWSHandler.receiveMessageFromSqs(doneTasksQueueUrl, 0);
        messages.forEach(message -> {
            MessageDto messageDto = gson.fromJson(message.body(), MessageDto.class);
            Task doneTask = gson.fromJson(messageDto.getData(), Task.class);
            String inputFile = gson.fromJson(doneTask.getFilename(), String.class);
            FileHandler fileHandler = inputFileHandlersMap.get(inputFile);
            BufferedWriter writer = fileHandler.getOutputBuffer();
            incrementHandledReviews(inputFile);
            try {
                writer.write(doneTask.getData());
                writer.newLine();
                AWSHandler.deleteMessageFromSqs(doneTasksQueueUrl, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Boolean finishedSendingReview = fileHandler.getFinishedSendingReview();
            AtomicInteger numOfSentReviews = fileHandler.getNumOfSentReviews();
            AtomicInteger numOfHandledReviews = fileHandler.getNumOfHandledReviews();
            if (finishedSendingReview && (numOfHandledReviews.get() == numOfSentReviews.get())) {
                try {
                    fileHandler.getOutputBuffer().close();
                    AWSHandler.s3Upload(bucketName, new File(fileHandler.getOutputFile()));
                    AWSHandler.sendMessageToSqs(applicationQueueUrl, gson.toJson(new MessageDto(MESSAGE_TYPE.DONE, fileHandler.getOutputFile())), true);
                    logger.info("uploaded output: {}", fileHandler.getOutputFile());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void incrementHandledReviews(String inputFile) {
        inputFileHandlersMap.get(inputFile).incrementHandledReviews();
    }

    private static void handleInputFiles() {
        while (true) {
            for (String inputFile : inputFileHandlersMap.keySet()) {
                int workerId = 0;
                int counter = 0;
                if (inputFileHandlersMap.get(inputFile).getFinishedSendingReview()) {
                    continue;
                }
                ResponseInputStream<GetObjectResponse> inputStream = AWSHandler.s3ReadFile(bucketName, inputFile);
                try {
                    reader = new BufferedReader(new InputStreamReader(inputStream));
                    String line = reader.readLine();

                    if (counter == 0) createWorkerInstance(workerId);
                    while (line != null) {
                        ProductReview productReview = gson.fromJson(line, ProductReview.class);
                        List<Review> reviews = productReview.getReviews();
                        for (Review review : reviews) {
                            String task = gson.toJson(new Task(inputFile, gson.toJson(review, Review.class)), Task.class);
                            AWSHandler.sendMessageToSqs(workersQueueUrl, gson.toJson(new MessageDto(MESSAGE_TYPE.TASK, task)), false);
                            incrementSentReviews(inputFile);
                            counter++;
                            if (counter == workersFilesRatio) {
                                counter = 0;
                                workerId++;
                                createWorkerInstance(workerId);
                            }
                        }
                        line = reader.readLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                inputFileHandlersMap.get(inputFile).setFinishedSendingReview(true);
            }
        }
    }

    private static void createWorkerInstance(int workerId) {
        List<String> args = new ArrayList<>();
        args.add("-workersQ " + workersQueueUrl);
        args.add("-doneTasksQ " + doneTasksQueueUrl);
        List<Instance> instances = AWSHandler.ec2CreateInstance(String.format("worker%d", workerId), 1, "Worker.jar", bucketName, args, InstanceType.T2_LARGE);
        boolean isWorkerExists = workersList.stream().anyMatch(worker -> worker.instanceId().equals(instances.get(0).instanceId()));
        if (!isWorkerExists)
            workersList.addAll(instances);
    }

    private static void incrementSentReviews(String inputFile) {
        inputFileHandlersMap.get(inputFile).incrementSentReviews();
    }

    private static void getInputFilesMessage() {
        while (true) {
            Message inputFilesMessage;
            logger.info("waiting for input files message at {}", managerQueueUrl);
            do {
                List<Message> messages = AWSHandler.receiveMessageFromSqs(managerQueueUrl, 1);
                inputFilesMessage = messages.stream().filter(message -> {
                    String messageBodyString = message.body();
                    MessageDto messageDto = gson.fromJson(messageBodyString, MessageDto.class);
                    MESSAGE_TYPE messageDtoType = messageDto.getType();
                    return messageDtoType.equals(MESSAGE_TYPE.INPUT) || messageDtoType.equals(MESSAGE_TYPE.INPUT_T);
                }).findAny().orElse(null);
            } while (inputFilesMessage == null);
            logger.info("found input files message at {}", managerQueueUrl);
            MessageDto messageDto = gson.fromJson(inputFilesMessage.body(), MessageDto.class);
//        Dto.MessageDto comes with braces [], take them off and split all inputFiles
            HashMap<String, String> inputOutputMap = gson.fromJson(messageDto.getData(), HashMap.class);
            inputOutputMap.keySet().forEach(inputFile -> {
                String outputFile = inputOutputMap.get(inputFile);
                inputFileHandlersMap.put(inputFile, new FileHandler(inputFile, outputFile));
            });
            synchronized (isTerminate) {
                isTerminate = isTerminate || messageDto.getType() == MESSAGE_TYPE.INPUT_T;
            }
            logger.info("input files are {}", inputOutputMap.keySet().toString());
            logger.info("output files are {}", inputOutputMap.values().toString());
            logger.info("terminates on finish");

            AWSHandler.deleteMessageFromSqs(managerQueueUrl, inputFilesMessage);
        }
    }

    private static void parseProgramArgs(String[] args, Options options) {
        Option appQ = new Option("appQ", true, "application q url");
        appQ.setRequired(true);
        options.addOption(appQ);

        Option managerQ = new Option("managerQ", true, "managerQ  url");
        managerQ.setRequired(true);
        options.addOption(managerQ);

        Option bucketNameOption = new Option("bucket", true, " bucketName");
        bucketNameOption.setRequired(true);
        options.addOption(bucketNameOption);

        Option workersFilesRatioOption = new Option("n", true, "Workers files ratio");
        workersFilesRatioOption.setRequired(true);
        options.addOption(workersFilesRatioOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        applicationQueueUrl = cmd.getOptionValue("appQ");
        managerQueueUrl = cmd.getOptionValue("managerQ");
        bucketName = cmd.getOptionValue("bucket");
        workersFilesRatio = NumberUtils.toInt(cmd.getOptionValue("n"));

        logger.info("appQUrl {}", applicationQueueUrl);
        logger.info("managerQUrl {}", managerQueueUrl);
        logger.info("bucketName {}", bucketName);
        logger.info("workersFilesRatio {}", workersFilesRatio);
    }

    private static void configureLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
}
