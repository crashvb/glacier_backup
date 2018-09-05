package com.vkleban.glacier_backup.slave;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.vkleban.glacier_backup.Archive;
import com.vkleban.glacier_backup.DownloadJob;

public class DownloadSlave extends TransferSlave {

    private static Logger log = Logger.getLogger(UploadSlave.class.getName());

    private final BlockingQueue<SlaveRequest<DownloadJob>> downloads_;
    private final BlockingQueue<SlaveResponse<DownloadJob>> reports_;

    public DownloadSlave(BlockingQueue<SlaveRequest<DownloadJob>> downloads, BlockingQueue<SlaveResponse<DownloadJob>> reports) {
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
        SlaveRequest<DownloadJob> request= null;
        log.finer("Starting download slave thread \"" + Thread.currentThread() + "\"");
        try {
            while (true) {
                request= null; // next line can fail. This is to avoid misreporting failed job
                request= downloads_.take();
                DownloadJob job= request.getRequest();
                if (request.isStopped())
                {
                    log.finer("Received thread stop request");
                    // passing the request on to the next thread
                    downloads_.add(request);
                    reports_.add(
                        new SlaveResponse<DownloadJob>(job, null, true));
                    return;
                }
                try {
                    File downloadedFile = Paths.get(c_.root_dir, job.getArchive().getFileName()).toFile();
                    ensureParentDirectory(downloadedFile);
                    log.info("Downloading file \"" + downloadedFile + "\"");
                    archiveTransferManager_.downloadJobOutput(null, c_.vault, job.getJobId(), downloadedFile);
                    log.info("Download of \"" + downloadedFile + "\" completed successfully");
                    reports_.add(new SlaveResponse<DownloadJob>(job, null, false));
                } catch (Exception e) {
                    Archive archive= job.getArchive();
                    log.severe("Download job of file \""
                            + archive.getFileName()
                            + "\" with archive ID \""
                            + archive.getArchiveId()
                            + "\" using job \""
                            + job.getJobId()
                            + "\" has failed on Glacier.\n"
                            + "Failure to download single file won't stop the download cycle.\n"
                            + "This is best effort download");
                    reports_.add(new SlaveResponse<DownloadJob>(job, e, false));
                }
            }
        } catch (Exception e) {
            reports_.add(
                new SlaveResponse<DownloadJob>(
                    request == null ? null : request.getRequest(),
                    new Exception("Unexpected exception", e), true));            
        } finally {
            log.finer("Shutting down download slave thread \"" + Thread.currentThread() + "\"");
        }
    }

}
