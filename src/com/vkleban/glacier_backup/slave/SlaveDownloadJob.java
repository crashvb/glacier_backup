package com.vkleban.glacier_backup.slave;

import com.vkleban.glacier_backup.Archive;

/**
 * Piece of work to be passed to download slaves
 * 
 * @author vova
 *
 */
public class SlaveDownloadJob {
    private String job_;
    private Archive archive_;
    private boolean stop_;
    
    /**
     * @param job - download job ID
     * @param file - file path to download the archive to
     * @param stop - do we have to stop downloading
     */
    public SlaveDownloadJob(String job, Archive archive, boolean stop) {
        job_= job;
        archive_= archive;
        stop_= stop;
    }
    
    public String getJob() {
        return job_;
    }

    public Archive getArchive() {
        return archive_;
    }
    
    public boolean isStopped() {
        return stop_;
    }

}
