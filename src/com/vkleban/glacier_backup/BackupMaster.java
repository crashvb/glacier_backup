package com.vkleban.glacier_backup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.vkleban.argument_parser.ArgumentException;
import com.vkleban.argument_parser.ArgumentParser;
import com.vkleban.glacier_backup.config.Config;

public class BackupMaster {

    private final Config c_;

    private AmazonGlacier          amazonGlacier_;
    private AmazonSQS              amazonSQS_;
    private AmazonSNS              amazonSNS_;
    private ArchiveTransferManager archiveTransferManager_;

    public BackupMaster() {
        c_ = Config.get();

        BasicAWSCredentials credentials = new BasicAWSCredentials(c_.access_key, c_.secret_key);
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        amazonGlacier_ = AmazonGlacierClientBuilder
                .standard()
                .withRegion(c_.region)
                .withCredentials(credentialsProvider)
                .build();
        amazonSQS_ = AmazonSQSClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(c_.region)
                .build();
        amazonSNS_ = AmazonSNSClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(c_.region)
                .build();
        ArchiveTransferManagerBuilder builder = new ArchiveTransferManagerBuilder();
        archiveTransferManager_ = builder
                .withGlacierClient(amazonGlacier_)
                .withSqsClient(amazonSQS_)
                .withSnsClient(amazonSNS_)
                .build();
    }

    public void upload(String name) throws AmazonClientException, FileNotFoundException {
        String archiveId = archiveTransferManager_.upload(c_.vault, name, new File(name)).getArchiveId();
        System.out.println("archive ID: " + archiveId);
    }
    
    public void download(String name) {
        //TODO
    }
    
    private String initiateListJob() {
        InitiateJobRequest initJobRequest = new InitiateJobRequest()
                .withVaultName(c_.vault)
                .withJobParameters(
                        new JobParameters()
                        .withType("inventory-retrieval")
                        .withSNSTopic(c_.sns_topic_arn)
                        );
        InitiateJobResult initJobResult = amazonGlacier_.initiateJob(initJobRequest);
        return initJobResult.getJobId();
    }
    
    private void waitForJobCompletion(String jobId) {
        for (DescribeJobRequest describeJobRequest= new DescribeJobRequest()
                .withVaultName(c_.vault)
                .withJobId(jobId);
             !amazonGlacier_.describeJob(describeJobRequest).isCompleted();)
        {
            System.out.println("Job \""
                    + jobId
                    + "\" hasn't been completed yet. Trying again in "
                    + c_.polling_milliseconds
                    + " ms");
            try {
                Thread.sleep(c_.polling_milliseconds);
            } catch (InterruptedException e) {}
        }
    }
    
    private void downloadJobOutput(String jobId) throws IOException
    {      
        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
            .withVaultName(c_.vault)
            .withJobId(jobId);
        GetJobOutputResult getJobOutputResult = amazonGlacier_.getJobOutput(getJobOutputRequest);
          
        String inputLine;
        try (BufferedReader in=
                new BufferedReader(
                    new InputStreamReader(
                        getJobOutputResult.getBody(),
                        StandardCharsets.UTF_8)))
        {
            while ((inputLine = in.readLine()) != null) {
                System.out.println(inputLine);
            }
        }
        System.out.println("Retrieved inventory successfully");
    }
    
    public void list() throws IOException {    
        String jobId = initiateListJob();
        System.out.println("List job successfully initiated");
        waitForJobCompletion(jobId);
        System.out.println("Job \"" + jobId + "\" has been completed. Downloading...");
        downloadJobOutput(jobId);
    }

    private static String usage() {
        return "Usage:\n"
                + "java -jar glacier_backup.jar {h|c:{u:|l|d:}}\n"
                + "-h   usage\n"
                + "-c   configuration file\n"
                + "-u   upload\n"
                + "-l   list vault contents\n"
                + "-d   download\n";
    }

    public static void main(String[] args) throws AmazonServiceException, AmazonClientException {
        try {
            ArgumentParser optParser = new ArgumentParser("{h|c:{u:|l|d:}}");
            Map<String, String> opts = optParser.parseArguments(args);
            if (opts.containsKey("h")) {
                System.out.println(usage());
                return;
            }
            Config.init(Paths.get(opts.get("c")));
            BackupMaster bm= new BackupMaster();
            String arg;
            if ((arg = opts.get("u")) != null) {
                bm.upload(arg);
            } else if ((arg = opts.get("d")) != null) {
                bm.download(arg);
            } else if ((arg = opts.get("l")) != null) {
                bm.list();
            }
        } catch (ArgumentException e) {
            System.err.println(e);
            System.err.println(usage());
        } catch (InitException e) {
            System.err.println("Failed initializing:\n" + e);
        } catch (IOException e) {
            System.err.println("Failed file operation:\n" + e);
        }
    }
}