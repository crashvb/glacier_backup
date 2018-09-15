package com.vkleban.glacier_backup;

import com.google.gson.annotations.SerializedName;

public class Archive {
    
    @SerializedName("ArchiveId")
    private String archive_;
    @SerializedName("ArchiveDescription")
    private String file_;
    @SerializedName("SHA256TreeHash")
    private String treeHash_;
    
    public Archive(String archiveId, String fileName, String treeHash) {
        archive_= archiveId;
        file_= fileName;
        treeHash_= treeHash;
    }

    public String getArchiveId() {
        return archive_;
    }

    public String getFileName() {
        return file_;
    }
    
    public String getTreeHash() {
        return treeHash_;
    }
    
    @Override
    public int hashCode() {
        return archive_.hashCode();
    }
    
    @Override
    public boolean equals(Object another) {
        if (!(another instanceof Archive))
            return false;
        Archive anotherArchive= (Archive) another;
        return anotherArchive.archive_.equals(archive_) && anotherArchive.treeHash_.equals(treeHash_); 
    }

}
