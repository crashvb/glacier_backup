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
    private final BlockingQueue<SlaveException> exceptions_;

    public DownloadSlave(BlockingQueue<SlaveDownloadJob> downloads, BlockingQueue<SlaveException> terminations) {
        downloads_ = downloads;
        exceptions_= terminations;
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
                    downloads_.add(job);
                    throw new InterruptedException();
                }
                File downloadedFile = Paths.get(c_.root_dir, job.getArchive().getFile()).toFile();
                try {
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
                    exceptions_.add(new SlaveException(Thread.currentThread(), job.getJob(), e));
                }
            }
        } catch (InterruptedException e) {
            exceptions_.add(new SlaveException(Thread.currentThread(), job == null ? null : job.getJob(), e));
        } finally {
            log.finer("Shutting down download slave thread \"" + Thread.currentThread() + "\"");
        }
    }

}
