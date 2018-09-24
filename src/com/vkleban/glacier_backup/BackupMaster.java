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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.glacier.TreeHashGenerator;
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
import com.vkleban.glacier_backup.slave.SlaveRequest;
import com.vkleban.glacier_backup.slave.SlaveResponse;
import com.vkleban.glacier_backup.slave.UploadSlave;

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
            String output= rawJson.toString();
            log.finest("JSON job output \"" + output + "\"");
            return output;
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
        Set<Archive> filteredList= new LinkedHashSet<>();
        String inventory= getListing();
        log.finer("Received inventory:\n" + beautifyJson(inventory));
        for (Archive entry : parseInventoryJSONToArchiveFileMap(inventory)) {
            if (matcher.matches(Paths.get(entry.getFileName()))) {
                log.info("File \"" + entry.getFileName() + "\" matched pattern \"" + glob + "\"");
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
    public void removeList(Set<Archive> archiveNameMap) {
        for (Archive archive : archiveNameMap)
        {
            log.info("Removing archive \"" + archive.getFileName() + "\" with archive ID \"" + archive.getArchiveId() + "\"");
            DeleteArchiveRequest request = new DeleteArchiveRequest()
                    .withVaultName(c_.vault)
                    .withArchiveId(archive.getArchiveId());
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
        Set<Archive> filteredList= new LinkedHashSet<>();
        for (Archive entry : parseInventoryJSONToArchiveFileMap(getListing())) {
            if (matcher.matches(Paths.get(entry.getFileName()))) {
                log.info("File \"" + entry.getFileName() + "\" matched pattern \"" + glob + "\"");
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
     * Given a collection of archives, construct an emulation of Glacier's inventory JSON
     * 
     * @param archives - collection of archives
     * @return Emulation of Glacier inventory JSON
     */
    public String ArchivesToInventoryJSON(Collection<Archive> archives) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject archiveList = new JsonObject();
        archiveList.add("ArchiveList", gson.toJsonTree(archives));
        return gson.toJson(archiveList);
    }
    
    /**
     * Given a Glacier inventory JSON (generated by Amazon or this code), return set of Archives
     * 
     * @param inventory - Glacier inventory JSON
     * @return Set of archives for which ArchiveDescription matches glob
     */
    public Set<Archive> parseInventoryJSONToArchiveFileMap(String inventory) {

        JsonParser parser = new JsonParser();
        JsonObject parsedListing= parser.parse(inventory).getAsJsonObject();
        JsonArray archiveList= parsedListing.getAsJsonArray("ArchiveList");
        Set<Archive> archiveIds= new LinkedHashSet<>();
        for (JsonElement fileEntry : archiveList) {
            JsonObject object= fileEntry.getAsJsonObject();
            archiveIds.add(
                new Archive(
                    object.get("ArchiveId").getAsString(),
                    object.get("ArchiveDescription").getAsString(),
                    object.get("SHA256TreeHash").getAsString()));
        }
        return archiveIds;
    }
    
    /**
     * Given set of archives, initiate their downloads
     * 
     * @param archiveIDs - list of archive IDs to download
     * @return map of job IDs to archives
     */
    private Map<String, Archive> initiateDownloadJobs(Set<Archive> archives) {
        Map<String, Archive> jobArchiveMap= new HashMap<>();
        log.info("Creating file download jobs");
        for (Archive archiveID: archives) {
            JobParameters jobParameters = new JobParameters()
                    .withType("archive-retrieval")
                    .withArchiveId(archiveID.getArchiveId())
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
    public void downloadList(Set<Archive> archives) throws IOException {
        Map<String, Archive> jobArchiveMap= initiateDownloadJobs(archives);
        log.fine("Starting download slaves");
        // NOTE! downloadJobs size must be at least the size of the number of jobs. Otherwise deadlock is possible
        ArrayBlockingQueue<SlaveRequest<DownloadJob>> downloadJobs= new ArrayBlockingQueue<>(archives.size() + 1);
        // NOTE! slaveReplies size must be at least the size of the number of jobs + number of threads
        // Otherwise deadlock is possible
        ArrayBlockingQueue<SlaveResponse<DownloadJob>> slaveReplies=
            new ArrayBlockingQueue<>(archives.size()
                                   + c_.file_transfer_slaves);
        Set<Thread> workers= new HashSet<>(c_.file_transfer_slaves);
        for (int i= 0; i < c_.file_transfer_slaves; i++) {
            Thread worker= new Thread(new DownloadSlave(downloadJobs, slaveReplies), "DownloadSlave-" + i);
            worker.start();
            workers.add(worker);
        }
        log.info("Awaiting download jobs completion");
        try (StatusMonitor jobMonitor = new StatusMonitor(amazonSQS_, amazonSNS_)) {
            Set<String> jobsToComplete= jobArchiveMap.keySet();
            while (jobsToComplete.size() != 0) {
                StatusMonitor.JobResult jobResult= jobMonitor.waitForJobToComplete(jobsToComplete);
                Archive archive= jobArchiveMap.remove(jobResult.getJob());
                if (!jobResult.succeeded()) {
                    log.severe(
                        "Download job of file \""
                        + archive.getFileName()
                        + "\" with archive ID \""
                        + archive.getArchiveId()
                        + "\" using job \""
                        + jobResult.getJob()
                        + "\" has failed on Glacier.\n"
                        + "Failure to download single file won't stop the download cycle.\n"
                        + "This is best effort download");
                    continue;
                }
                log.info("Scheduling download of file \""
                        + archive.getFileName()
                        + "\" with archive ID \""
                        + jobResult.getJob()
                        + "\" using job \""
                        + jobResult.getJob()
                        + "\"");
                downloadJobs.add(new SlaveRequest<DownloadJob>(new DownloadJob(jobResult.getJob(), archive) , false));
            }
            log.fine("End of jobs. Announcing shutdown to slave threads");
        } finally {
            downloadJobs.add(new SlaveRequest<DownloadJob>(null, true));
            while (workers.size() > 0) {
                try {
                    SlaveResponse<DownloadJob> slaveResponse= slaveReplies.take();
                    if (slaveResponse.isStopped()) {
                        Thread slave= slaveResponse.getSlave();
                        log.fine("Joining thread \"" + slave + "\"");
                        workers.remove(slave);
                        slave.join();                        
                    } else if (slaveResponse.getException() == null) {
                        Archive downloadedArchive= slaveResponse.getResponse().getArchive();
                        log.fine("Registering archive with ID \""
                               + downloadedArchive.getArchiveId()
                               + "\" as downloaded");
                        archives.remove(downloadedArchive);
                    } else {
                        String error= "Download slave thread \"" + slaveResponse.getSlave().getName()
                                + "\" has reported a problem:\n"
                                + slaveResponse.getException() + "\n";
                        DownloadJob job= slaveResponse.getResponse();
                        if (job == null) {
                            error+= "Job unknown. Possible job loss";
                        }
                        else {
                            error+= "Job \"" + job.getJobId() + "\" has failed";
                        }
                        log.severe(error);
                    }
                } catch (InterruptedException e) {}
            }
            if (archives.size() == 0) {
                log.info("Downloads have completed successfully");
            } else {
                log.severe("Downloads have completed with errors. "
                         + "The following list of archives failed to download:\n"
                         + ArchivesToInventoryJSON(archives));
            }
        }
    }
    
    /**
     * Upload files provided in standard input, while filtering them against existing inventory,
     * given as a local inventory file.
     * The existing inventory is updated as soon as upload is finished
     * 
     * @param inventoryFileName - inventory file 
     * @throws AmazonClientException when Amazon Glacier operation fails
     * @throws IOException when disk operation fails
     */
    public void uploadIncremental(String inventoryFileName)
        throws AmazonClientException, IOException
    {
        Set<Archive> existingArchives;
        // Load inventory and check the corresponding file is writable
        Path inventoryPath= Paths.get(inventoryFileName);
        String inventoryError= "Unable to ensure I can update \"" + inventoryPath + "\" file";
        File inventoryFile= inventoryPath.toFile();
        if (!inventoryFile.isFile()) {
            log.warning("Given inventory file does not exist. Will create new one");
            existingArchives= new LinkedHashSet<>();
            try {
                Files.createFile(inventoryPath);
                Files.delete(inventoryPath);
            } catch (Exception e) {
                throw new IOException(inventoryError, e);
            }
        } else {
            if (!inventoryFile.canWrite())
                throw new IOException(inventoryError);
            existingArchives= parseInventoryJSONToArchiveFileMap(
                new String(Files.readAllBytes(inventoryPath),
                    StandardCharsets.UTF_8));
        }
        // Load file paths and filter them through the inventory contents
        Set<Path> filter= existingArchives
            .stream()
            .map(a -> Paths.get(a.getFileName()).normalize())
            .collect(Collectors.toSet());
        Set<String> files= new LinkedHashSet<>();
        try (Scanner sc= new Scanner(System.in)) {
            while (sc.hasNextLine()) {
                Path filePath= Paths.get(sc.nextLine()).normalize();
                // NOTE! Path equality is tested without normalization!
                // Tested on Linux
                if (filter.contains(filePath)) {
                    log.info("Skipping existing in inventory \"" + filePath + "\"");
                } else {
                    files.add(filePath.toString());
                }
            }
        }
        // Upload files
        List<Archive> uploaded= upload(new LinkedHashSet<>(files));
        // Update the inventory with freshly uploaded files
        if (uploaded.size() == 0)
            return;
        existingArchives.addAll(uploaded);
        Path tempFile= inventoryPath.resolveSibling(
            inventoryFileName +
            DateTimeFormatter.ofPattern("'.'yyyyMMdd'T'HHmmss'.'SSS").format(LocalDateTime.now()));
        log.fine("Creating temp file \"" + tempFile + "\"");
        Files.write(
            tempFile,
            ArchivesToInventoryJSON(existingArchives).getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE_NEW);
        log.info("Updating inventory \"" + inventoryPath + "\"");
        Files.move(tempFile, inventoryPath, StandardCopyOption.ATOMIC_MOVE);
    }
    
    /**
     * Upload files given in standard input and new line separated
     * 
     * @throws AmazonClientException
     * @throws FileNotFoundException
     */
    public void uploadUnfiltered() throws AmazonClientException, FileNotFoundException {
        Set<String> files= new LinkedHashSet<>();
        try (Scanner sc= new Scanner(System.in)) {
            while (sc.hasNextLine())
                files.add(Paths.get(sc.nextLine()).normalize().toString());
        }
        upload(files);
    }
    
    /**
     * Upload files given by set of relative (to root_dir) paths given as strings
     * 
     * @param files - set of relative paths given as strings. NOTE! The set is modified here
     * @return the list of successfully uploaded files
     * 
     * @throws AmazonClientException when Amazon Glacier operation fails
     * @throws FileNotFoundException when file to be uploaded is not found
     */
    public List<Archive> upload(Set<String> files)
        throws AmazonClientException, FileNotFoundException
    {
        log.fine("Starting upload slaves");
        // NOTE! downloadJobs size must be at least the size of the number of jobs. Otherwise deadlock is possible
        ArrayBlockingQueue<SlaveRequest<String>> uploadJobs= new ArrayBlockingQueue<>(files.size() + 1);
        // NOTE! slaveReplies size must be at least the size of the number of jobs + number of threads
        // Otherwise deadlock is possible
        ArrayBlockingQueue<SlaveResponse<Archive>> slaveReplies=
            new ArrayBlockingQueue<>(files.size()
                                   + c_.file_transfer_slaves);
        Set<Thread> workers= new HashSet<>(c_.file_transfer_slaves);
        for (int i= 0; i < c_.file_transfer_slaves; i++) {
            Thread worker= new Thread(new UploadSlave(uploadJobs, slaveReplies), "UploadSlave-" + i);
            worker.start();
            workers.add(worker);
        }
        log.info("Uploading " + files.size() + " files");
        for (String file : files) {
            log.fine("Scheduling upload of file \""
                    + file
                    + "\"");
            uploadJobs.add(new SlaveRequest<String>(file, false));
        }
        uploadJobs.add(new SlaveRequest<String>(null, true));
        List<Archive> uploaded= new ArrayList<>();
        while (workers.size() > 0) {
            try {
                SlaveResponse<Archive> slaveResponse= slaveReplies.take();
                if (slaveResponse.isStopped()) {
                    Thread slave= slaveResponse.getSlave();
                    log.fine("Joining thread \"" + slave + "\"");
                    workers.remove(slave);
                    slave.join();                        
                } else if (slaveResponse.getException() == null) {
                    Archive uploadedArchive= slaveResponse.getResponse();
                    log.fine("Registering archive with ID \""
                           + uploadedArchive.getArchiveId()
                           + "\" as uploaded");
                    uploaded.add(uploadedArchive);
                    files.remove(uploadedArchive.getFileName());
                } else {
                    String file= slaveResponse.getResponse() == null
                            ? null
                            : slaveResponse.getResponse().getFileName(); 
                    log.severe("Upload slave thread \"" + slaveResponse.getSlave().getName()
                            + "\" while uploading "
                            + (file == null ? "UNKNOWN FILE" : "\"" + file + "\"")
                            + " has reported a problem:\n"
                            + slaveResponse.getException() + "\n");
                }
            } catch (InterruptedException e) {}
        }
        log.info(
            "Inventory of successfully uploaded files:\n"
            + ArchivesToInventoryJSON(uploaded));
        if (files.size() == 0) {
            log.info("All uploads have completed successfully");
        } else {
            StringBuilder error= new StringBuilder("Uploads have completed with errors. "
                     + "The following list of files failed to upload:\n");
            for (String failedFile : files) {
                error.append(failedFile).append("\n");
            }
            log.severe(error.toString());
        }
        log.warning("WARNING: Amazon Glacier updates your inventory once per day.\n"
                  + "This means you won't see these uploads in the vault for up to a 24 hours");
        return uploaded;
    }
    
    /**
     * Verify checksums of the files referenced by given inventory
     * 
     * @param inventory - inventory file to verify
     * @throws IOException when file operations fail
     */
    public void verifyLocal(String inventory) throws IOException {
        Set<Archive> failedArchives= new LinkedHashSet<>();
        for (Archive testArchive:
                parseInventoryJSONToArchiveFileMap(
                    new String(Files.readAllBytes(Paths.get(inventory)),
                    StandardCharsets.UTF_8)))
        {
            String fileName= testArchive.getFileName();
            try {
                String actualChecksum = TreeHashGenerator.calculateTreeHash(Paths.get(c_.root_dir, fileName).toFile());
                if (actualChecksum.equals(testArchive.getTreeHash())) {
                    log.info("\"" + fileName + "\" is OK");
                } else {
                    failedArchives.add(testArchive);
                    log.severe(
                        "FAILED checksum test of \"" +
                        fileName +
                        "\". Expected checksum: " +
                        testArchive.getTreeHash() +
                        ". Actual checksum: " +
                        actualChecksum);
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "FAILED calculating checksum of \"" + fileName + "\"", e);
            }
        }
        if (failedArchives.isEmpty()) {
            log.info("The inventory \"" + inventory + "\" is healthy!");
        } else {
            log.severe("ERROR! The inventory \"" + inventory + "\" is corrupted.\n"
                + "The following list of archives failed integrity check:\n"
                + ArchivesToInventoryJSON(failedArchives));
        }
    }
    
    /**
     * Verify status of the local inventory in Glacier
     * 
     * @param inventory - inventory file to verify
     * @throws IOException when file operations fail
     */
    public void verifyRemote(String inventory) throws IOException {
        
        Set<Archive> failedArchives= new LinkedHashSet<>();
        Set<Archive> localArchives= parseInventoryJSONToArchiveFileMap(
            new String(Files.readAllBytes(Paths.get(inventory)),
            StandardCharsets.UTF_8));
        Set<Archive> glacierArchives= parseInventoryJSONToArchiveFileMap(getListing());
        Map<String, List<Archive>> glacierNameToArchiveMap= glacierArchives
            .stream()
            .collect(Collectors.groupingBy(Archive::getFileName));
        for (Archive testArchive: localArchives)
        {
            if (glacierArchives.contains(testArchive)) {
                log.info("\"" + testArchive.getFileName() + "\" is OK");
                continue;
            }
            failedArchives.add(testArchive);
            StringBuilder error= new StringBuilder("Failed to find any match for archive:\n").append(testArchive);
            List<Archive> matchingArchives= glacierNameToArchiveMap.get(testArchive.getFileName());
            if (matchingArchives == null) {
                error.append("\nGlacier does not have any candidate with the same file name");
            } else {
                for (Archive match: matchingArchives)
                    error.append("\nGlacier candidate:\n").append(match);
            }
            log.severe(error.toString());
        }
        if (failedArchives.isEmpty()) {
            log.info("The inventory \"" + inventory + "\" is healthy!");
        } else {
            log.severe("ERROR! The inventory \"" + inventory + "\" is corrupted.\n"
                + "The following list of archives could not be found in Glacier:\n"
                + ArchivesToInventoryJSON(failedArchives));
        }
    }

    private static String usage() {
        return
            "Usage:\n"
            + "java -jar glacier_backup.jar {h|c:{u[i:]|vi:{l|r}|l|d{g:|i:}|r{g:|i:}}}\n"
            + "where:\n"
            + "-h   usage\n"
            + "-c   configuration file\n"
            + "-i   file with Amazon Glacier inventory style JSON\n\n"
            + "Upload. If inventory is given, upload only what's not yet there, updating the inventory afterwards:\n"
            + "    <file listing relative to configured root_dir> | java -jar glacier_backup.jar -c <config file> -u [ -i <inventory> ]\n"
            + "Verify given inventory against local files (-l) or Glacier (-r):\n"
            + "    java -jar glacier_backup.jar -c <config file> -v -i <inventory> {-l|-r} \n"
            + "List files (get current inventory):\n"
            + "    java -jar glacier_backup.jar -l\n"
            + "Download files by glob (best effort):\n"
            + "    java -jar glacier_backup.jar -c <config file> -d -g <Java style file glob>\n"
            + "Download files by inventory (best effort):\n"
            + "    java -jar glacier_backup.jar -c <config file> -d -i <inventory>\n"
            + "Remove files by glob:\n"
            + "    java -jar glacier_backup.jar -c <config file> -r -g <Java style file glob>\n"
            + "Remove files by inventory:\n"
            + "    java -jar glacier_backup.jar -c <config file> -r -i <inventory>\n";
    }
    
    public static void main(String[] args) throws AmazonServiceException, AmazonClientException {
        try {
            ArgumentParser optParser = new ArgumentParser("{h|c:{u[i:]|vi:{l|r}|l|d{g:|i:}|r{g:|i:}}}");
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
                String parameter = opts.get("i");
                if (parameter != null)
                    bm.uploadIncremental(parameter);
                else
                    bm.uploadUnfiltered();
            } else if (opts.containsKey("v")) {
                String inventory= opts.get("i");
                if (opts.containsKey("l"))
                    bm.verifyLocal(inventory);
                else if (opts.containsKey("r"))
                    bm.verifyRemote(inventory);
            } else if (opts.containsKey("d")) {
                String parameter = opts.get("g");
                if (parameter != null) {
                    bm.downloadByGlob(parameter);
                } else {
                    bm.downloadByListing(Paths.get(opts.get("i")));
                }
            } else if (opts.containsKey("r")) {
                String parameter = opts.get("g");
                if (parameter != null) {
                    bm.removeByGlob(parameter);
                } else {
                    bm.removeByListing(Paths.get(opts.get("i")));
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