package com.vkleban.glacier_backup;

import com.google.gson.annotations.SerializedName;

public class Archive {
    
    @SerializedName("ArchiveId")
    private String archiveID_;
    @SerializedName("ArchiveDescription")
    private String fileName_;
    @SerializedName("SHA256TreeHash")
    private String treeHash_;
    
    public Archive(String archiveId, String fileName, String treeHash) {
        archiveID_= archiveId;
        fileName_= fileName;
        treeHash_= treeHash;
    }

    public String getArchiveId() {
        return archiveID_;
    }

    public String getFileName() {
        return fileName_;
    }
    
    public String getTreeHash() {
        return treeHash_;
    }
    
    @Override
    public int hashCode() {
        return archiveID_.hashCode();
    }
    
    @Override
    public boolean equals(Object another) {
        if (!(another instanceof Archive))
            return false;
        Archive anotherArchive= (Archive) another;
        return anotherArchive.archiveID_.equals(archiveID_) && anotherArchive.treeHash_.equals(treeHash_); 
    }

    @Override
    public String toString() {
        return "File name \"" + fileName_ + "\", archive ID \"" + archiveID_ + "\", tree Hash \"" + treeHash_ + "\"";
    }
}
