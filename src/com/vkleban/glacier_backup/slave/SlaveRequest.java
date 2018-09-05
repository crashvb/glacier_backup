package com.vkleban.glacier_backup.slave;

public class SlaveRequest<Request> {
    
    private Request request_;
    private boolean stop_;
    
    public SlaveRequest(Request request, boolean stop) {
        request_= request;
        stop_= stop;
    }
    
    public Request getRequest() {
        return request_;
    }
    
    public boolean isStopped() {
        return stop_;
    }
    
}
