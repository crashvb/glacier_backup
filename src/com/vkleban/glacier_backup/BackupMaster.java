package com.vkleban.glacier_backup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
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

    /**
     * Upload archive
     * 
     * @param name - file path relative to the configured root dir
     * @throws AmazonClientException
     * @throws FileNotFoundException
     */
    public void upload(String name) throws AmazonClientException, FileNotFoundException {
//        List<String> archiveIds= new ArrayList<>();
        String archiveId = archiveTransferManager_
                               .upload(c_.vault,
                                       name,
                                       Paths.get(c_.root_dir, name).toFile())
                               .getArchiveId();
        System.out.println("archive ID: " + archiveId);
    }
    
    /**
     * Download particular archive by its ID into a file with a given name in a root dir
     * 
     * @param archiveID - Amazon Glacier archive ID (list operation will give you the IDs)
     * @param name      - File name to be given to the downloaded archive
     */
    public void download(String archiveID, String name) {
        archiveTransferManager_.download(c_.vault, archiveID, Paths.get(c_.root_dir, name).toFile());
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
    
    private String downloadJsonJobOutput(String jobId) throws IOException
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
            StringBuilder rawJson= new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                rawJson.append(inputLine);
            }
            return rawJson.toString();
        }
    }
    
    /**
     * Convert a JSON string to pretty print version
     * @param jsonString
     * @return beautified (properly indented for human reading) JSON string
     */
    public static String beautifyJson(String jsonString) 
    {
        JsonParser parser = new JsonParser();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(parser.parse(jsonString).getAsJsonObject());
    }
    
    /**
     * @return vault listing
     * @throws IOException
     */
    public String getListing() throws IOException {
        String jobId = initiateListJob();
        System.out.println("List job successfully initiated");
        waitForJobCompletion(jobId);
        System.out.println("Job \"" + jobId + "\" has been completed. Downloading...");
        return downloadJsonJobOutput(jobId);
    }
    
    /**
     * Print listing of the vault
     * @throws IOException
     */
    public void list() throws IOException {    
        System.out.println("Listing contents:\n" + beautifyJson(getListing()));
    }
    
    public void test() {
        List<String> archiveIDs = Arrays.asList("3tGq-0QGNvq3TtW1x9euJ8yejsRQQs2vB37hgPn_Ers-5j0IbWexQ78PxpaXpcCuni3Y1DXPgh-U22VQfyULcxlyqBv1DvpwVmOiPDfqxqZ82WVFkGck4_iOdvYfSJnevdH0J04gmQ",
                                    "UXfteUG7-GhINkQ0SFva1Rw5tpL40mHJztBauyfCR-zVXEjCVC0fjuNaI1lfi-Pva3EFebSrFxzmFYsEYigoPIV76bF4HeszKSgW_rTOSs-0f4byXAR8BCG9jIhqaG2DtMSuqGDKew",
                                    "mGW5ossJiOI8lQUFqXeaejxnT0PUsd_0WDO96ySNm_sVQ_GpjcYbVXShY-RP-Y-ENuxV8iz9jH00n7ay0OKDr6VlhP97ExrwY8DcO_UWqwkIXAhqVJZV9z12dyfnJi6a5jtjJ5Gm3Q");
        Set<String> jobs= new HashSet<>();
        for (String archiveID: archiveIDs) {
            JobParameters jobParameters = new JobParameters()
                    .withType("archive-retrieval")
                    .withArchiveId(archiveID)
                    .withTier(c_.retrieval_tier)
                    .withSNSTopic(c_.sns_topic_arn);
                
                InitiateJobRequest request = new InitiateJobRequest()
                    .withVaultName(c_.vault)
                    .withJobParameters(jobParameters);
                
                String jobID= amazonGlacier_.initiateJob(request).getJobId();
                jobs.add(jobID);
                System.out.println("Job " + jobID + " for archive " + archiveID);
        }
        try (StatusMonitor jobMonitor = new StatusMonitor(amazonSQS_, amazonSNS_)) {
            while (jobs.size() != 0) {
                String job= jobMonitor.waitForJobToComplete(jobs);
                System.out.println("Downloading job " + job);
                archiveTransferManager_
                    .downloadJobOutput(
                            null,
                            c_.vault,
                            job,
                            Paths.get(c_.root_dir, job.substring(0, 4)).toFile());
            }
        }
        System.exit(0);
    }

    private static String usage() {
        return "Usage:\n"
                + "java -jar glacier_backup.jar {h|c:{u:|l|d:n:}}\n"
                + "-h   usage\n"
                + "-c   configuration file\n"
                + "-u   upload\n"
                + "-l   list vault contents\n"
                + "-d   download specific archive by its ID. This usually requires listing your vault contents first\n"
                + "-n   file name\n"
                + "-e   extract bunch of files by ";
    }

    public static void main(String[] args) throws AmazonServiceException, AmazonClientException {
        try {
            ArgumentParser optParser = new ArgumentParser("{h|c:{u:|l|d:n:}}");
            Map<String, String> opts = optParser.parseArguments(args);
            if (opts.containsKey("h")) {
                System.out.println(usage());
                return;
            }
            Config.init(Paths.get(opts.get("c")));
            BackupMaster bm= new BackupMaster();
            bm.test();
            String arg;
            if ((arg = opts.get("u")) != null) {
                bm.upload(arg);
            } else if ((arg = opts.get("d")) != null) {
                bm.download(arg, opts.get("n"));
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