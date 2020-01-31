import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {
    private static String bucketName = "ori-and-shay-dsps-201-ass2-output";
    private static String keyName;
    private static String serviceRole;
    private static String jobFlowRole;
    private static String triGramJarPath = "s3n://ori-and-shay-dsps-201-ass2-bucket/TriGram.jar";


    public static void main(String[] args) {

        BasicConfigurator.configure();
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(triGramJarPath)
                .withMainClass("MainClass")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        "s3n://" + bucketName + "/3Gram-output");

        StepConfig stepConfig = new StepConfig()
                .withName("Calculate_3Gram_predictions")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(String.valueOf(InstanceType.M1_LARGE))
                .withSlaveInstanceType(String.valueOf(InstanceType.M1_LARGE))
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(keyName)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("3Gram hebrew")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole(serviceRole)
                .withJobFlowRole(jobFlowRole)
                .withLogUri("s3n://" + bucketName + "/3Grams-log/")
                .withReleaseLabel("emr-4.2.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}
