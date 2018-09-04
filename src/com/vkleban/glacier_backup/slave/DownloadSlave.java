package com.vkleban.glacier_backup.slave;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.vkleban.glacier_backup.Archive;

public class DownloadSlave extends TransferSlave {

    private static Logger log = Logger.getLogger(UploadSlave.class.getName());

    private final BlockingQueue<SlaveDownloadJob> downloads_;
    private final BlockingQueue<SlaveReport> reports_;

    public DownloadSlave(BlockingQueue<SlaveDownloadJob> downloads, BlockingQueue<SlaveReport> reports) {
        downloads_ = downloads;
        reports_= reports;
    }

    /**
     * Make sure there is a directory for this file to be written to
     * 
     * @param file
     *            - the file to prepare for
     * @throws IOException
     *             when we fail to ensure such directory exists for any reason
     */
    private void ensureParentDirectory(File file) throws IOException {
        File parentFile = file.getParentFile();
        if (parentFile == null)
            return;
        log.fine("Creating directory \"" + parentFile + "\"");
        Files.createDirectories(parentFile.toPath());
    }

    @Override
    public void run() {
        SlaveDownloadJob job= null;
        log.finer("Starting download slave thread \"" + Thread.currentThread() + "\"");
        try {
            while (true) {
                job= null; // next line can fail. This is to avoid misreporting failed job
                job= downloads_.take();
                if (job.isStopped())
                {
                    log.finer("Received thread stop request");
                    downloads_.add(job);
                    reports_.add(
                        new SlaveReport(
                            job == null ? null : job.getJob(),
                            new InterruptedException()));
                    return;
                }
                try {
                    File downloadedFile = Paths.get(c_.root_dir, job.getArchive().getFile()).toFile();
                    ensureParentDirectory(downloadedFile);
                    log.info("Downloading file \"" + downloadedFile + "\"");
                    archiveTransferManager_.downloadJobOutput(null, c_.vault, job.getJob(), downloadedFile);
                    log.info("Download of \"" + downloadedFile + "\" completed successfully");
                } catch (Exception e) {
                    Archive archive= job.getArchive();
                    log.severe("Download job of file \""
                            + archive.getFile()
                            + "\" with archive ID \""
                            + archive.getArchive()
                            + "\" using job \""
                            + job.getJob()
                            + "\" has failed on Glacier.\n"
                            + "Failure to download single file won't stop the download cycle.\n"
                            + "This is best effort download");
                    reports_.add(new SlaveReport(job.getJob(), e));
                }
            }
        } catch (Exception e) {
            reports_.add(
                new SlaveReport(
                    job == null ? null : job.getJob(),
                    new Exception("Unexpected exception", e)));            
        } finally {
            log.finer("Shutting down download slave thread \"" + Thread.currentThread() + "\"");
        }
    }

}
