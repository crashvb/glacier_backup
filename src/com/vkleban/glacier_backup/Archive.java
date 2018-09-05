package com.vkleban.glacier_backup;

import com.google.gson.annotations.SerializedName;

public class Archive {
    
    @SerializedName("ArchiveId")
    private String archive_;
    @SerializedName("ArchiveDescription")
    private String file_;
    
    public Archive(String archive, String file) {
        archive_= archive;
        file_= file;
    }

    public String getArchiveId() {
        return archive_;
    }

    public String getFileName() {
        return file_;
    }
    
    @Override
    public int hashCode() {
        return archive_.hashCode();
    }
    
    @Override
    public boolean equals(Object another) {
        return another instanceof Archive && ((Archive) another).archive_.equals(archive_); 
    }

}
