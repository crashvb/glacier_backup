package com.vkleban.glacier_backup;

import java.io.BufferedReader;
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
     * Download list of files by given archive IDs into given file names
     * 
     * @param archiveNameMap - map of archive IDs to file names
     */
    public void downloadList(Map<String, String> archiveNameMap) {
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
                archiveTransferManager_
                    .downloadJobOutput(
                            null,
                            c_.vault,
                            job,
                            Paths.get(c_.root_dir, fileName).toFile());
            }
        }
    }
    
    /**
     * Download archives by Java glob. This operation first uses inventory to get listing, which takes about 4 hours.
     * Then the downloads will depend on configured 
     * 
     * @param glob - Java glob
     * @throws IOException
     */
    public void downloadByGlob(String glob) throws IOException {
        log.info("Attempting to download files by glob \"" + glob + "\"");
        PathMatcher matcher= FileSystems.getDefault()
                .getPathMatcher("glob:" + glob);
        HashMap<String, String> filteredList= new HashMap<>();
        for (Map.Entry<String, String> entry : parseInventoryJSONToArchiveFileMap(getListing()).entrySet()) {
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
                + "java -jar glacier_backup.jar {h|c:{u|l|d{g:|n:}}}\n"
                + "where:\n"
                + "-h   usage\n"
                + "-c   configuration file\n"
                + "Upload:     <file listing> | java -jar glacier_backup.jar -c <config file> -u\n"
                + "List files: java -jar glacier_backup.jar -l\n"
                + "Download files by glob:\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -g <Java style file glob>\n"
                + "Download files by inventory:\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -n <File with inventory style JSON>\n";
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

    
    public static void main(String[] args) throws AmazonServiceException, AmazonClientException {
        try {
            ArgumentParser optParser = new ArgumentParser("{h|c:{u|l|d{g:|n:}}}");
            Map<String, String> opts = optParser.parseArguments(args);
            if (opts.containsKey("h")) {
                System.out.println(usage());
                return;
            }
            Config.init(Paths.get(opts.get("c")));
            initLogger();
            BackupMaster bm= new BackupMaster();
//            bm.testGlobFinding();
            if (opts.containsKey("u")) {
                bm.upload();
            } else if (opts.containsKey("d")) {
                String parameter;
                if ((parameter = opts.get("g")) != null) {
                    bm.downloadByGlob(parameter);
                } else {
                    bm.downloadByListing(Paths.get(opts.get("n")));
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
            log.log(Level.SEVERE,"Failed Glacier operation", e);
        } catch (IOException e) {
            log.log(Level.SEVERE,"Failed file operation: ", e);
        } catch (Exception e) {
            log.log(Level.SEVERE, "General error: ", e);
        }
    }
}