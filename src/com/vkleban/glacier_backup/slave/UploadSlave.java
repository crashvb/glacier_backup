package com.vkleban.glacier_backup.slave;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.vkleban.argument_parser.ArgumentException;

public class UploadSlave extends TransferSlave {
    
    private static Logger log= Logger.getLogger(UploadSlave.class.getName());
    
    public UploadSlave() {
    } 

    /**
     * Upload archive
     * 
     * @param name - file path relative to the configured root dir
     * @throws AmazonClientException
     * @throws FileNotFoundException
     * @throws ArgumentException 
     */
    public void upload() throws AmazonClientException, FileNotFoundException, ArgumentException {
        ArrayList<String> files= new ArrayList<>();
        try (Scanner sc= new Scanner(System.in)) {
            while (sc.hasNextLine())
                files.add(sc.nextLine());
        }
        log.info("Uploading " + files.size() + " files");
        for (String file : files) {
            log.info("Uploading \"" + file + "\"");
            String archiveId = archiveTransferManager_
                    .upload(c_.vault,
                            file,
                            Paths.get(c_.root_dir, file).toFile())
                    .getArchiveId();
            log.info("Upload successful. Archive ID: " + archiveId);
        }
        log.info("WARNING: Amazon Glacier updates your inventory once per day.\n"
                + "This means you won't see these uploads in the vault for up to a 24 hours");
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        
    }
    
}
