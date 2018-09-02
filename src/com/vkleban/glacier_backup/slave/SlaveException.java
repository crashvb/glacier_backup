package com.vkleban.glacier_backup.slave;

public class SlaveException {
    
    private Thread slave_;
    private String jobId_;
    private Exception problem_;
    
    public SlaveException(Thread slave, String jobId, Exception problem) {
        slave_= slave;
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
