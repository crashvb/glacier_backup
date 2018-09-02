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

    public String getArchive() {
        return archive_;
    }

    public String getFile() {
        return file_;
    }

}
