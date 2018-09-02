package com.vkleban.glacier_backup;

import java.io.BufferedReader;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vkleban.argument_parser.ArgumentException;
import com.vkleban.argument_parser.ArgumentParser;
import com.vkleban.glacier_backup.config.Config;
import com.vkleban.glacier_backup.log.ConsoleFormatter;
import com.vkleban.glacier_backup.log.LogFormatter;
import com.vkleban.glacier_backup.slave.DownloadSlave;
import com.vkleban.glacier_backup.slave.SlaveDownloadJob;
import com.vkleban.glacier_backup.slave.SlaveException;

public class BackupMaster extends GlacierClient {

    private static Logger log= Logger.getLogger(BackupMaster.class.getName());

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
        List<Archive> filteredList= new ArrayList<>();
        String inventory= getListing();
        log.finer("Received inventory:\n" + beautifyJson(inventory));
        for (Archive entry : parseInventoryJSONToArchiveFileMap(inventory)) {
            if (matcher.matches(Paths.get(entry.getFile()))) {
                log.info("File \"" + entry.getFile() + "\" matched pattern \"" + glob + "\"");
                filteredList.add(entry);
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
     * Remove list of files by given archive list
     * 
     * @param archiveNameMap - map of archive IDs to file names
     */
    public void removeList(List<Archive> archiveNameMap) {
        for (Archive archive : archiveNameMap)
        {
            log.info("Removing archive \"" + archive.getFile() + "\" with archive ID \"" + archive.getArchive() + "\"");
            DeleteArchiveRequest request = new DeleteArchiveRequest()
                    .withVaultName(c_.vault)
                    .withArchiveId(archive.getArchive());
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
        List<Archive> filteredList= new ArrayList<>();
        for (Archive entry : parseInventoryJSONToArchiveFileMap(getListing())) {
            if (matcher.matches(Paths.get(entry.getFile()))) {
                log.info("File \"" + entry.getFile() + "\" matched pattern \"" + glob + "\"");
                filteredList.add(entry);
            }
        }
        removeList(filteredList);
    }
    
    /**
     * Remove archives by inventory provided as a file
     * 
     * @param inventory - path to inventory containing file describing archives to delete
     * @throws IOException when failed reading given inventory
     */
    public void removeByListing(Path inventory) throws IOException {
        log.info("Attempting to remove files by given inventory \"" + inventory + "\"");
        removeList(parseInventoryJSONToArchiveFileMap(
                new String(Files.readAllBytes(inventory),
                StandardCharsets.UTF_8)));
    }
    
    /**
     * Reset and re-initialize java.util.logging based logger
     * 
     * @throws InitException
     */
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
     * @return List of archives for which ArchiveDescription matches glob
     */
    public List<Archive> parseInventoryJSONToArchiveFileMap(String inventory) {

        JsonParser parser = new JsonParser();
        JsonObject parsedListing= parser.parse(inventory).getAsJsonObject();
        JsonArray archiveList= parsedListing.getAsJsonArray("ArchiveList");
        List<Archive> archiveIds= new ArrayList<>();
        for (JsonElement fileEntry : archiveList) {
            JsonObject object= fileEntry.getAsJsonObject();
            String fileName= object.get("ArchiveDescription").getAsString();
            archiveIds.add(new Archive(object.get("ArchiveId").getAsString(), fileName));
        }
        return archiveIds;
    }
    
    /**
     * Given set of archives, initiate their downloads
     * 
     * @param archiveIDs - list of archive IDs to download
     * @return map of job IDs to archives
     */
    private Map<String, Archive> initiateDownloadJobs(List<Archive> archives) {
        Map<String, Archive> jobArchiveMap= new HashMap<>();
        log.info("Creating file download jobs");
        for (Archive archiveID: archives) {
            JobParameters jobParameters = new JobParameters()
                    .withType("archive-retrieval")
                    .withArchiveId(archiveID.getArchive())
                    .withTier(c_.retrieval_tier)
                    .withSNSTopic(c_.sns_topic_arn);
                
                InitiateJobRequest request = new InitiateJobRequest()
                    .withVaultName(c_.vault)
                    .withJobParameters(jobParameters);
                
                String jobID= amazonGlacier_.initiateJob(request).getJobId();
                jobArchiveMap.put(jobID, archiveID);
                log.fine("Job " + jobID + " for archive " + archiveID);
        }
        return jobArchiveMap;
    }

    
    /**
     * Download list of files by given archive IDs into given file names
     * 
     * @param archives - list of archives
     * @throws IOException when file operation errors happen
     */
    public void downloadList(List<Archive> archives) throws IOException {
        Map<String, Archive> jobArchiveMap= initiateDownloadJobs(archives);
        log.fine("Starting download slaves");
        // NOTE! BlockingQueue size must be at least the size of the number of jobs. Otherwise deadlock is possible
        ArrayBlockingQueue<SlaveDownloadJob> downloadJobs= new ArrayBlockingQueue<>(archives.size());
        ArrayBlockingQueue<SlaveException> slaveExceptions= new ArrayBlockingQueue<>(archives.size());
        Set<Thread> workers= new HashSet<>(c_.file_transfer_slaves);
        // Map of failed archive downloads to their file names
        ArrayList<Archive> failedArchiveDownloads= new ArrayList<>();
        for (int i= 0; i < c_.file_transfer_slaves; i++) {
            Thread worker= new Thread(new DownloadSlave(downloadJobs, slaveExceptions));
            worker.start();
            workers.add(worker);
        }
        log.info("Awaiting download jobs completion");
        try (StatusMonitor jobMonitor = new StatusMonitor(amazonSQS_, amazonSNS_)) {
            Set<String> jobsToComplete= new HashSet<>();
            jobsToComplete.addAll(jobArchiveMap.keySet());
            while (jobsToComplete.size() != 0) {
                StatusMonitor.JobResult jobResult= jobMonitor.waitForJobToComplete(jobsToComplete);
                Archive archive= jobArchiveMap.get(jobResult.getJob());
                if (!jobResult.succeeded()) {
                    log.severe("Download job of file \""
                            + archive.getFile()
                            + "\" with archive ID \""
                            + archive.getArchive()
                            + "\" using job \""
                            + jobResult.getJob()
                            + "\" has failed on Glacier.\n"
                            + "Failure to download single file won't stop the download cycle.\n"
                            + "This is best effort download");
                    failedArchiveDownloads.add(archive);
                    continue;
                }
                log.info("Scheduling download of file \""
                        + archive.getFile()
                        + "\" with archive ID \""
                        + jobResult.getJob()
                        + "\" using job \""
                        + jobResult.getJob()
                        + "\"");
                downloadJobs.add(new SlaveDownloadJob(jobResult.getJob(), archive, false));
            }
            log.fine("End of jobs. Announcing shutdown to slave threads");
        } finally {
            downloadJobs.add(new SlaveDownloadJob(null, null, true));
            while (workers.size() > 0) {
                try {
                    SlaveException slaveTerm= slaveExceptions.take();
                    if (slaveTerm.getProblem() instanceof InterruptedException) {
                        Thread slave= slaveTerm.getSlave();
                        workers.remove(slave);
                        log.fine("Joining thread \"" + slave + "\"");
                        slave.join();                        
                    } else {
                        log.fine("Adding \"" + slaveTerm.getJobId() + "\" to the failed downloads list");
                        failedArchiveDownloads.add(jobArchiveMap.get(slaveTerm.getJobId()));                        
                    }
                } catch (InterruptedException e) {}
            }
            if (failedArchiveDownloads.size() == 0) {
                log.info("Downloads have completed successfully");
            } else {
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                JsonObject archiveList = new JsonObject();
                archiveList.add("ArchiveList", gson.toJsonTree(failedArchiveDownloads));
                log.severe("Downloads have completed with errors. "
                         + "The following list of archives failed to download:\n"
                         + gson.toJson(archiveList));
            }
        }
    }

    private static String usage() {
        return "Usage:\n"
                + "java -jar glacier_backup.jar {h|c:{u|l|d{g:|n:}|r{g:|n:}}}\n"
                + "where:\n"
                + "-h   usage\n"
                + "-c   configuration file\n"
                + "Upload:     <file listing> | java -jar glacier_backup.jar -c <config file> -u\n"
                + "List files: java -jar glacier_backup.jar -l\n"
                + "Download files by glob (best effort):\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -g <Java style file glob>\n"
                + "Download files by inventory (best effort):\n"
                + "            java -jar glacier_backup.jar -c <config file> -d -n <File with inventory style JSON>\n"
                + "Remove files by glob:\n"
                + "            java -jar glacier_backup.jar -c <config file> -r -g <Java style file glob>\n"
                + "Remove files by inventory:\n"
                + "            java -jar glacier_backup.jar -c <config file> -r -n <File with inventory style JSON>\n";
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
//            testSerialization();
            if (opts.containsKey("u")) {
//                bm.upload(); TODO
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