package com.vkleban.glacier_backup;

/**
 * Piece of work to be passed to download slaves
 * 
 * @author vova
 *
 */
public class DownloadJob {
    private String job_;
    private Archive archive_;
    
    /**
     * @param job - download job ID
     * @param file - file path to download the archive to
     */
    public DownloadJob(String job, Archive archive) {
        job_= job;
        archive_= archive;
    }
    
    public String getJobId() {
        return job_;
    }

    public Archive getArchive() {
        return archive_;
    }

}
