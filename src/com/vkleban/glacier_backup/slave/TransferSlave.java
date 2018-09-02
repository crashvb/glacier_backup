package com.vkleban.glacier_backup.slave;

import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.vkleban.glacier_backup.GlacierClient;

public abstract class TransferSlave extends GlacierClient implements Runnable {

    protected ArchiveTransferManager archiveTransferManager_;
    
    protected TransferSlave() {
        ArchiveTransferManagerBuilder builder = new ArchiveTransferManagerBuilder();
        archiveTransferManager_ = builder
                .withGlacierClient(amazonGlacier_)
                .withSqsClient(amazonSQS_)
                .withSnsClient(amazonSNS_)
                .build();
    }
}
