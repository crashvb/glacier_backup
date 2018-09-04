package com.vkleban.glacier_backup.slave;

public class SlaveReport {
    
    private Thread slave_;
    private String jobId_;
    private Exception problem_;
    
    public SlaveReport(String jobId, Exception problem) {
        slave_= Thread.currentThread();
        problem_= problem;
        jobId_= jobId;
    }
    
    public Thread getSlave() {
        return slave_;
    }

    public Exception getProblem() {
        return problem_;
    }

    public String getJobId() {
        return jobId_;
    }
    
}
