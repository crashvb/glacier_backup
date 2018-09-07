package com.vkleban.glacier_backup.slave;

import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.vkleban.glacier_backup.Archive;

public class UploadSlave extends TransferSlave {
    
    private static Logger log= Logger.getLogger(UploadSlave.class.getName());
    
    private final BlockingQueue<SlaveRequest<String>> uploads_;
    private final BlockingQueue<SlaveResponse<Archive>> reports_;
    
    public UploadSlave(BlockingQueue<SlaveRequest<String>> uploads, BlockingQueue<SlaveResponse<Archive>> reports) {
        uploads_ = uploads;
        reports_= reports;
    }

    @Override
    public void run() {
        SlaveRequest<String> request= null;
        log.finer("Starting upload slave thread \"" + Thread.currentThread() + "\"");
        try {
            while (true) {
                request= null; // next line can fail. This is to avoid misreporting failed job
                request= uploads_.take();
                String fileName= request.getRequest();
                if (request.isStopped())
                {
                    log.finer("Received thread stop request");
                    // passing the request on to the next thread
                    uploads_.add(request);
                    reports_.add(
                        new SlaveResponse<Archive>(null, null, true));
                    return;
                }
                try {
                    log.info("Uploading \"" + fileName + "\"");
                    String archiveId = archiveTransferManager_
                            .upload(c_.vault,
                                    fileName,
                                    Paths.get(c_.root_dir, fileName).toFile())
                            .getArchiveId();
                    log.info("Upload successful. Archive ID: " + archiveId);
                    reports_.add(new SlaveResponse<Archive>(new Archive(archiveId, fileName), null, false));
                } catch (Exception e) {
                    Archive archive= new Archive(null, fileName);
                    log.severe("Upload job of file \""
                            + archive.getFileName()
                            + "\" has failed on Glacier.\n"
                            + "Failure to upload single file won't stop the upload cycle.\n"
                            + "This is best effort upload");
                    reports_.add(new SlaveResponse<Archive>(archive, e, false));
                }
            }
        } catch (Exception e) {
            reports_.add(
                new SlaveResponse<Archive>(
                    request == null ? null : new Archive(null, request.getRequest()),
                    new Exception("Unexpected exception", e), true));            
        } finally {
            log.finer("Shutting down upload slave thread \"" + Thread.currentThread() + "\"");
        }
    }
    
}
