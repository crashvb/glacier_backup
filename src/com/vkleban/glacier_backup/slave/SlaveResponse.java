package com.vkleban.glacier_backup.slave;

public class SlaveResponse<Response> {

    Thread slave_;
    Response response_;
    Exception exception_;
    boolean isStopped_;
    
    public SlaveResponse(Response response, Exception exception, boolean isStopped) {
        slave_= Thread.currentThread();
        response_= response;
        exception_= exception;
        isStopped_= isStopped;
    }
    
    public Thread getSlave() {
        return slave_;
    }

    public Response getResponse() {
        return response_;
    }

    public Exception getException() {
        return exception_;
    }

    public boolean isStopped() {
        return isStopped_;
    }
    
}
