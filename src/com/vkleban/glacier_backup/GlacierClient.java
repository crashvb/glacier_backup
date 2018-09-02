package com.vkleban.glacier_backup;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.vkleban.glacier_backup.config.Config;

public abstract class GlacierClient {
    
    protected final Config c_;
    
    protected AmazonGlacier          amazonGlacier_;
    protected AmazonSQS              amazonSQS_;
    protected AmazonSNS              amazonSNS_;

    protected GlacierClient() {
        c_ = Config.get();

        BasicAWSCredentials credentials = new BasicAWSCredentials(c_.access_key, c_.secret_key);
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        amazonGlacier_ = AmazonGlacierClientBuilder
                .standard()
                .withRegion(c_.region)
                .withCredentials(credentialsProvider)
                .build();
        amazonSQS_ = AmazonSQSClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(c_.region)
                .build();
        amazonSNS_ = AmazonSNSClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(c_.region)
                .build();
    }

}
