package com.vkleban.glacier_backup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vkleban.argument_parser.ArgumentException;
import com.vkleban.argument_parser.ArgumentParser;
import com.vkleban.glacier_backup.config.Config;

public class BackupMaster {

    private static Logger log= Logger.getLogger(BackupMaster.class.getName());
    
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
     * @throws ArgumentException 
     */
    public void upload() throws AmazonClientException, FileNotFoundException, ArgumentException {
        ArrayList<String> files= new ArrayList<>();
        try (Scanner sc= new Scanner(System.in)) {
            while (sc.hasNextLine())
                files.add(sc.nextLine());
        }
        log.info("Uploading " + files.size() + " files");
        for (String file : files) {
            log.info("Uploading \"" + file + "\"");
            String archiveId = archiveTransferManager_
                    .upload(c_.vault,
                            file,
                            Paths.get(c_.root_dir, file).toFile())
                    .getArchiveId();
            log.info("Upload successful. Archive ID: " + archiveId);
        }
        log.info("WARNING: Amazon Glacier updates your inventory once per day.\n"
                + "This means you won't see these uploads in the vault for up to a 24 hours");
    }
    
    /**
     * Initiate inventory (listing contents) job
     * 
     * @return inventory job ID
     */
    private String initiateListJob() {
        InitiateJobRequest initJobRequest = new InitiateJobRequest()
                .withVaultName(c_.vault)
                .withJobParameters(
                        new JobParameters()
                        .withType("inventory-retrieval")
                        .withSNSTopic(c_.sns_topic_arn)
                        );
        log.finer("Initiating list job \"" + initJobRequest + "\"");
        InitiateJobResult initJobResult = amazonGlacier_.initiateJob(initJobRequest);
        return initJobResult.getJobId();
    }
    
    /**
     * Wait on completion of a single job. For multiple jobs please use StatusMonitor
     * 
     * @param jobId - job to wait for
     */
    private void waitForJobCompletion(String jobId) {
        for (DescribeJobRequest describeJobRequest= new DescribeJobRequest()
                .withVaultName(c_.vault)
                .withJobId(jobId);
             !amazonGlacier_.describeJob(describeJobRequest).isCompleted();)
        {
            log.finest("Job \""
                    + jobId
                    + "\" hasn't been completed yet. Trying again in "
                    + c_.polling_milliseconds
                    + " ms");
            try {
                Thread.sleep(c_.polling_milliseconds);
            } catch (InterruptedException e) {}
        }
    }
    
    /**
     * Download job output, which is in JSON format
     * 
     * @param jobId - job result to download
     * @return job result
     * @throws IOException when reading job result fails
     */
    private String downloadJsonJobOutput(String jobId) throws IOException
    {      
        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
            .withVaultName(c_.vault)
            .withJobId(jobId);
        log.finer("Initiating job request \"" + getJobOutputRequest + "\"");
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
        log.finest("JSON before beautification \"" + jsonString + "\"");
        JsonParser parser = new JsonParser();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(parser.parse(jsonString).getAsJsonObject());
    }
    
    /**
     * Get listing of the vault
     * 
     * @return vault listing
     * @throws IOException
     */
    public String getListing() throws IOException {
        String jobId = initiateListJob();
        log.info("List job successfully initiated. About 4 hours is required for completion");
        waitForJobCompletion(jobId);
        log.fine("Job \"" + jobId + "\" has been completed. Downloading inventory...");
        return downloadJsonJobOutput(jobId);
    }
    
    /**
     * Print listing of the vault
     * @throws IOException
     */
    public void list() throws IOException {    
        log.info("Listing contents:\n" + beautifyJson(getListing()));
    }
    
    /**
     * Make sure there is a directory for this file to be written to
     * 
     * @param file - the file to prepare for
     * @throws IOException when we fail to ensure such directory exists for any reason
     */
    private void ensureParentDirectory(File file) throws IOException {
        File parentFile= file.getParentFile();
        if (parentFile == null)
            return;
        Files.createDirectories(parentFile.toPath());
    }
    
    /**
     * Download list of files by given archive IDs into given file names
     * 
     * @param archiveNameMap - map of archive IDs to file names
     * @throws IOException when file operation errors happen
     */
    public void downloadList(Map<String, String> archiveNameMap) throws IOException {
        Map<String, String> jobArchiveMap= new HashMap<>();
        log.info("Creating file download jobs");
        for (String archiveID: archiveNameMap.keySet()) {
            JobParameters jobParameters = new JobParameters()
                    .withType("archive-retrieval")
                    .withArchiveId(archiveID)
                    .withTier(c_.retrieval_tier)
                    .withSNSTopic(c_.sns_topic_arn);
                
                InitiateJobRequest request = new InitiateJobRequest()
                    .withVaultName(c_.vault)
                    .withJobParameters(jobParameters);
                
                String jobID= amazonGlacier_.initiateJob(request).getJobId();
                jobArchiveMap.put(jobID, archiveID);
                log.fine("Job " + jobID + " for archive " + archiveID);
        }
        log.info("Awaiting download jobs completion");
        try (StatusMonitor jobMonitor = new StatusMonitor(amazonSQS_, amazonSNS_)) {
            Set<String> jobs= jobArchiveMap.keySet();
            while (jobs.size() != 0) {
                String job= jobMonitor.waitForJobToComplete(jobs);
                String archiveId= jobArchiveMap.remove(job);
                String fileName= archiveNameMap.get(archiveId);
                log.info("Downloading file \""
                       + fileName
                       + "\" with archive ID \""
                       + archiveId
                       + "\" using job \""
                       + job
                       + "\"");
                File downloadedFile= Paths.get(c_.root_dir, fileName).toFile();
                ensureParentDirectory(downloadedFile);
                archiveTransferManager_
                    .downloadJobOutput(
                            null,
                            c_.vault,
                            job,
                            downloadedFile);
            }
        }
    }
    
    /**
     * Download archives by Java glob. This operation first uses inventory to get listing, which takes about 4 hours.
     * Then the download times will depend on configured tier
     * 
     * @param glob - Java glob
     * @throws IOException
     */
    public void downloadByGlob(String glob) throws IOException {
        log.info("Attempting to download files by glob \"" + glob + "\"");
        PathMatcher matcher= FileSystems.getDefault()
                .getPathMatcher("glob:" + glob);
        HashMap<String, String> filteredList= new HashMap<>();
        String inventory= getListing();
        log.finer("Received inventory:\n" + beautifyJson(inventory));
        for (Map.Entry<String, String> entry : parseInventoryJSONToArchiveFileMap(inventory).entrySet()) {
            if (matcher.matches(Paths.get(entry.getValue()))) {
                log.info("File \"" + entry.getValue() + "\" matched pattern \"" + glob + "\"");
                filteredList.put(entry.getKey(), entry.getValue());
            }
        }
        downloadList(filteredList);
    }
    
    /**
     * Download particular archive by its ID into a file with a given name in a root dir
     * 
     * @param archiveID - Amazon Glacier archive ID (list operation will give you the IDs)
     * @param name      - File name to be given to the downloaded archive
     * @throws IOException when failed reading given inventory
     */
    public void downloadByListing(Path inventory) throws IOException {
        log.info("Attempting to download files from given inventory \"" + inventory + "\"");
        downloadList(parseInventoryJSONToArchiveFileMap(
                new String(Files.readAllBytes(inventory),
                StandardCharsets.UTF_8)));
    }
    
    /**
     * Remove list of files by given archive IDs into given file names
     * 
     * @param archiveNameMap - map of archive IDs to file names
     */
    public void removeList(Map<String, String> archiveNameMap) {
        for (Map.Entry<String, String> archive : archiveNameMap.entrySet())
        {
            log.info("Removing archive \"" + archive.getValue() + "\" with archive ID \"" + archive.getKey() + "\"");
            DeleteArchiveRequest request = new DeleteArchiveRequest()
                    .withVaultName(c_.vault)
                    .withArchiveId(archive.getKey());

            amazonGlacier_.deleteArchive(request);
        }
    }
    
    /**
     * Remove archives by Java glob. This operation first uses inventory to get listing, which takes about 4 hours.
     * 
     * @param glob - Java glob
     * @throws IOException
     */
    public void removeByGlob(String glob) throws IOException {
        log.warning("Attempting to remove files by glob \"" + glob + "\". Is this what you really want?\n"
                  + "I suggest deletion of files by the inventory instead to stay on the safer side.\n"
                  + "You still have few hours to change your mind");
        PathMatcher matcher= FileSystems.getDefault()
                .getPathMatcher("glob:" + glob);
        HashMap<String, String> filteredList= new HashMap<>();
        for (Map.Entry<String, String> entry : parseInventoryJSONToArchiveFileMap(getListing()).entrySet()) {
            if (matcher.matches(Paths.get(entry.getValue()))) {
                log.info("File \"" + entry.getValue() + "\" matched pattern \"" + glob + "\"");
                filteredList.put(entry.getKey(), entry.getValue());
            }
        }
        removeList(filteredList);
    }
    
    /**
     * Download particular archive by its ID into a file with a given name in a root dir
     * 
     * @param archiveID - Amazon Glacier archive ID (list operation will give you the IDs)
     * @param name      - File name to be given to the downloaded archive
     * @throws IOException when failed reading given inventory
     */
    public void removeByListing(Path inventory) throws IOException {
        log.info("Attempting to remove files by given inventory \"" + inventory + "\"");
        removeList(parseInventoryJSONToArchiveFileMap(
                new String(Files.readAllBytes(inventory),
                StandardCharsets.UTF_8)));
    }
    
    private static void initLogger() throws InitException {
        try {
            // Close root logger handlers
            for (Handler h: Logger.getLogger("").getHandlers())
                h.close();
            // Close the rest of the handlers
            LogManager.getLogManager().reset();
            Logger myLogger= Logger.getLogger("com.vkleban");
            Config c= Config.get();
            Level logLevel= Level.parse(c.log_level);
            myLogger.setLevel(Level.parse(String.valueOf(Math.min(logLevel.intValue(), Level.INFO.intValue()))));
            Handler fh= new FileHandler(c.log_name, c.log_size, c.log_files);
            fh.setFormatter(new LogFormatter());
            fh.setLevel(logLevel);
            myLogger.addHandler(fh);
            Handler ch= new ConsoleHandler() {
                @Override
                protected synchronized void setOutputStream(OutputStream out) throws SecurityException {
                    super.setOutputStream(System.out);
                }
            };
            ch.setFormatter(new ConsoleFormatter());
            ch.setLevel(Level.INFO);
            myLogger.addHandler(ch);
        } catch (Exception e) {
            throw new InitException("Failure during initialization of logger", e);
        }
    }
    
    /**
     * Given an inventory output, perform search of archives by Java glob described in this article:
     * https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob
     * 
     * @param inventory - inventory output
     * @return Map of archive IDs to file names for which ArchiveDescription matches glob
     */
    public Map<String, String> parseInventoryJSONToArchiveFileMap(String inventory) {

        JsonParser parser = new JsonParser();
        JsonObject parsedListing= parser.parse(inventory).getAsJsonObject();
        JsonArray archiveList= parsedListing.getAsJsonArray("ArchiveList");
        Map<String, String> archiveIds= new HashMap<>();
        for (JsonElement fileEntry : archiveList) {
            JsonObject object= fileEntry.getAsJsonObject();
            String fileName= object.get("ArchiveDescription").getAsString();
            archiveIds.put(object.get("ArchiveId").getAsString(), fileName);
        }
        return archiveIds;
    }

    private static String usage() {
        return "Usage:\n"
                + "java -jar glacier_backup.jar {h|c:{u|l|d{g:|n:}|r{g:|n:}}}\n"
                + "where:\n"
                + "-h   usage\n"
                + "-c   configuration file\n"
                + "Upload:     <file listing> | java -jar glacier_backup.jar -c <config file> -u\n"
                + "List files: java -jar glacier_backup.jar -l\n"
                + "Download files by glob:\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -g <Java style file glob>\n"
                + "Download files by inventory:\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -n <File with inventory style JSON>\n"
                + "Remove files by glob:\n"
                + "            java -jar glacier_backup.jar -c <config file> -r -g <Java style file glob>\n"
                + "Remove files by inventory:\n"
                + "            java -jar glacier_backup.jar -c <config file> -r -n <File with inventory style JSON>\n";
    }
    
    public static String getTestListing() {
        return "{\n" + 
                "  \"VaultARN\": \"arn:aws:glacier:us-east-2:446183465487:vaults/vkleban\",\n" + 
                "  \"InventoryDate\": \"2018-07-05T12:27:52Z\",\n" + 
                "  \"ArchiveList\": [\n" + 
                "    {\n" + 
                "      \"ArchiveId\": \"3tGq-0QGNvq3TtW1x9euJ8yejsRQQs2vB37hgPn_Ers-5j0IbWexQ78PxpaXpcCuni3Y1DXPgh-U22VQfyULcxlyqBv1DvpwVmOiPDfqxqZ82WVFkGck4_iOdvYfSJnevdH0J04gmQ\",\n" + 
                "      \"ArchiveDescription\": \"kaka/byaka\",\n" + 
                "      \"CreationDate\": \"2018-07-03T05:49:21Z\",\n" + 
                "      \"Size\": 2097152,\n" + 
                "      \"SHA256TreeHash\": \"8810b1412025d931407f4debd89f5a11fd62db7ca9101040c0de336e3a453e23\"\n" + 
                "    },\n" + 
                "    {\n" + 
                "      \"ArchiveId\": \"UXfteUG7-GhINkQ0SFva1Rw5tpL40mHJztBauyfCR-zVXEjCVC0fjuNaI1lfi-Pva3EFebSrFxzmFYsEYigoPIV76bF4HeszKSgW_rTOSs-0f4byXAR8BCG9jIhqaG2DtMSuqGDKew\",\n" + 
                "      \"ArchiveDescription\": \"Tax 2012 documents\",\n" + 
                "      \"CreationDate\": \"2018-07-03T05:51:27Z\",\n" + 
                "      \"Size\": 2097152,\n" + 
                "      \"SHA256TreeHash\": \"8810b1412025d931407f4debd89f5a11fd62db7ca9101040c0de336e3a453e23\"\n" + 
                "    },\n" + 
                "    {\n" + 
                "      \"ArchiveId\": \"mGW5ossJiOI8lQUFqXeaejxnT0PUsd_0WDO96ySNm_sVQ_GpjcYbVXShY-RP-Y-ENuxV8iz9jH00n7ay0OKDr6VlhP97ExrwY8DcO_UWqwkIXAhqVJZV9z12dyfnJi6a5jtjJ5Gm3Q\",\n" + 
                "      \"ArchiveDescription\": \"Tax 2012 documents\",\n" + 
                "      \"CreationDate\": \"2018-07-05T01:57:06Z\",\n" + 
                "      \"Size\": 2097152,\n" + 
                "      \"SHA256TreeHash\": \"8810b1412025d931407f4debd89f5a11fd62db7ca9101040c0de336e3a453e23\"\n" + 
                "    }\n" + 
                "  ]\n" + 
                "}";
    }
    
    public static String testSQSData() {
        return "{\n" + 
                "  \"Type\": \"Notification\",\n" + 
                "  \"MessageId\": \"8489b5a5-fb36-5f7f-9080-e65adb441408\",\n" + 
                "  \"TopicArn\": \"arn:aws:sns:us-east-2:446183465487:retrieval\",\n" + 
                "  \"Message\": \"{\\\"Action\\\":\\\"ArchiveRetrieval\\\",\\\"ArchiveId\\\":\\\"mGW5ossJiOI8lQUFqXeaejxnT0PUsd_0WDO96ySNm_sVQ_GpjcYbVXShY-RP-Y-ENuxV8iz9jH00n7ay0OKDr6VlhP97ExrwY8DcO_UWqwkIXAhqVJZV9z12dyfnJi6a5jtjJ5Gm3Q\\\",\\\"ArchiveSHA256TreeHash\\\":\\\"8810b1412025d931407f4debd89f5a11fd62db7ca9101040c0de336e3a453e23\\\",\\\"ArchiveSizeInBytes\\\":2097152,\\\"Completed\\\":true,\\\"CompletionDate\\\":\\\"2018-08-28T10:04:33.978Z\\\",\\\"CreationDate\\\":\\\"2018-08-28T06:20:32.385Z\\\",\\\"InventoryRetrievalParameters\\\":null,\\\"InventorySizeInBytes\\\":null,\\\"JobDescription\\\":null,\\\"JobId\\\":\\\"fdjyolMiFRh5e3GiRuji6jqbbQMIqEKDTb_sv2B3Vu8p8Mzw8iIQAuYANkVgNNusduvYWJWWBaUl3f-3pc-B9MImhCJe\\\",\\\"RetrievalByteRange\\\":\\\"0-2097151\\\",\\\"SHA256TreeHash\\\":\\\"8810b1412025d931407f4debd89f5a11fd62db7ca9101040c0de336e3a453e23\\\",\\\"SNSTopic\\\":\\\"arn:aws:sns:us-east-2:446183465487:retrieval\\\",\\\"StatusCode\\\":\\\"Succeeded\\\",\\\"StatusMessage\\\":\\\"Succeeded\\\",\\\"Tier\\\":\\\"Standard\\\",\\\"VaultARN\\\":\\\"arn:aws:glacier:us-east-2:446183465487:vaults/vkleban\\\"}\",\n" + 
                "  \"Timestamp\": \"2018-08-28T10:04:34.054Z\",\n" + 
                "  \"SignatureVersion\": \"1\",\n" + 
                "  \"Signature\": \"RRfuT4NvfwOzQKuFXmciNrpYTVpgVnSHNI+vMMIbmiVAVAnRuK9mv75mnaGZ5HNlzAQMjvo5MVNwXQ1JJi+fjobSconffrlUs5lm44DWvBVOKbS4IL8PUIIlSIX7TS9xK4txtzhFxSJnsuYkM0+NrM3WrwA2OT1tlOmZmFk1rR1vtREZTSSZ44ZVxFzuScPR4HAeKn9QoENd1mtO+ijUyyt+y2DCjO1izO3cS6ZzsyRd68S1JvCWbwjwTrJ3QXkT7u/FPHDzmz0NRabGCLHm17rYNjLZuXxtSNQdQCiQj55qxtuolo1lXkmxBEnWyCefCUZefONk3Y38cl7RZ73WPg\\u003d\\u003d\",\n" + 
                "  \"SigningCertURL\": \"https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem\",\n" + 
                "  \"UnsubscribeURL\": \"https://sns.us-east-2.amazonaws.com/?Action\\u003dUnsubscribe\\u0026SubscriptionArn\\u003darn:aws:sns:us-east-2:446183465487:retrieval:640d8165-3947-452a-88d6-79ebbcc78292\"\n" + 
                "}";
    }

    public static void testSQSGsonParsing() {
        JsonParser parser_= new JsonParser();
        JsonObject parsedMessageBody= parser_.parse(testSQSData()).getAsJsonObject();
        String jsonMessage = parsedMessageBody.get("Message").getAsString().replace("\\\"", "\"");
        JsonObject parsedBody = parser_.parse(jsonMessage).getAsJsonObject();
        String messageJobId = parsedBody.get("JobId").getAsString();
        String messageStatus = parsedBody.get("StatusMessage").getAsString();
        System.out.println("Job ID \"" + messageJobId + "\"");
        System.out.println("Status \"" + messageStatus + "\"");
        System.exit(0);
    }
    
    public static void main(String[] args) throws AmazonServiceException, AmazonClientException {
        try {
            ArgumentParser optParser = new ArgumentParser("{h|c:{u|l|d{g:|n:}|r{g:|n:}}}");
            Map<String, String> opts = optParser.parseArguments(args);
            if (opts.containsKey("h")) {
                System.out.println(usage());
                return;
            }
            Config.init(Paths.get(opts.get("c")));
            initLogger();
            BackupMaster bm= new BackupMaster();
//            testSQSGsonParsing();
            if (opts.containsKey("u")) {
                bm.upload();
            } else if (opts.containsKey("d")) {
                String parameter;
                if ((parameter = opts.get("g")) != null) {
                    bm.downloadByGlob(parameter);
                } else {
                    bm.downloadByListing(Paths.get(opts.get("n")));
                }
            } else if (opts.containsKey("r")) {
                String parameter;
                if ((parameter = opts.get("g")) != null) {
                    bm.removeByGlob(parameter);
                } else {
                    bm.removeByListing(Paths.get(opts.get("n")));
                }
            } else if (opts.containsKey("l")) {
                bm.list();
            }
        } catch (ArgumentException e) {
            System.err.println(e);
            System.err.println(usage());
        } catch (InitException e) {
            System.err.println("Failed initializing:\n" + e);
        } catch (SdkBaseException e) {
            log.log(Level.SEVERE,"Failed Glacier operation:\n", e);
        } catch (IOException e) {
            log.log(Level.SEVERE,"Failed file operation:\n", e);
        } catch (Exception e) {
            log.log(Level.SEVERE, "General error: ", e);
        }
    }
}