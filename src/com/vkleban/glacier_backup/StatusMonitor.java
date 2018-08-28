package com.vkleban.glacier_backup;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.services.glacier.model.StatusCode;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.util.BinaryUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vkleban.glacier_backup.config.Config;

public class StatusMonitor implements AutoCloseable {
    
    private Logger log= Logger.getLogger(this.getClass().getName());
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private final Config c_= Config.get();
    
    private final String queueUrl;
    
    private final AmazonSQS amazonSQS_;
    
    private Iterator<Message> messageIterator_= new Iterator<Message>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Message next() {
            return null;
        }
    };

    /**
     * Create an SQS queue and attach it to configured SNS
     * 
     * @param amazonSQS - Amazon SQS object
     * @param amazonSNS - Amazon SNS object
     */
    public StatusMonitor(AmazonSQS amazonSQS, AmazonSNS amazonSNS) {
        amazonSQS_= amazonSQS;
        String randomSeed = UUID.randomUUID().toString();
        String queueName = "glacier-archive-transfer-" + randomSeed;
        log.fine("Generated SQS queue name \"" + queueName + "\"");

        queueUrl = amazonSQS.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
        String queueARN = amazonSQS
                .getQueueAttributes(
                        new GetQueueAttributesRequest(queueUrl)
                        .withAttributeNames("QueueArn"))
                        .getAttributes()
                        .get("QueueArn");
        
        log.finer("Received SQS queue name ARN \"" + queueARN + "\"");

        Policy sqsPolicy =
            new Policy().withStatements(
                    new Statement(Effect.Allow)
                    .withPrincipals(Principal.AllUsers)
                    .withActions(SQSActions.SendMessage)
                    .withResources(new Resource(queueARN))
                    .withConditions(ConditionFactory.newSourceArnCondition(c_.sns_topic_arn)));
        log.finest("SQS policy: " + BackupMaster.beautifyJson(sqsPolicy.toJson()));
        amazonSQS.setQueueAttributes(
                new SetQueueAttributesRequest(
                        queueUrl,
                        newAttributes("Policy", sqsPolicy.toJson())));

        amazonSNS.subscribe(new SubscribeRequest(c_.sns_topic_arn, "sqs", queueARN));
    }
    
    /**
     * Turn array of strings into HashMap. This ugliness came from Amazon Glacier source code
     * 
     * @param keyValuePairs - given array of strings
     * @return map of odd strings to subsequent even strings
     */
    private Map<String, String> newAttributes(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0)
            throw new IllegalArgumentException(
                    "Incorrect number of arguments passed.  Input must be specified as: key, value, key, value, ...");

        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            String key   = keyValuePairs[i];
            String value = keyValuePairs[i+1];
            map.put(key, value);
        }

        return map;
    }    
    
    /**
     * @return message from the SQS queue
     */
    private Message getMessage() {
        while (!messageIterator_.hasNext()) {
            messageIterator_ = amazonSQS_.receiveMessage(new ReceiveMessageRequest(queueUrl)).getMessages().iterator();
            if (!messageIterator_.hasNext())
                try {
                    Thread.sleep(c_.polling_milliseconds);
                } catch (InterruptedException e) {/* Ignore */ }
        }
        return messageIterator_.next();
    }
    
    /**
     * Given set of jobs, wait for any one of them to complete, then return the ID of the completed one
     * 
     * @param jobs
     * @return
     * @throws IllegalArgumentException
     */
    public String waitForJobToComplete(Set<String> jobs) throws IllegalArgumentException {
        if (jobs.size() == 0)
            throw new IllegalArgumentException("Cannot supply empty job set. Please fix your code");
        while (true) {
            Message message= getMessage();
            String messageBody = message.getBody();
            if (!messageBody.startsWith("{")) {
                messageBody = new String(BinaryUtils.fromBase64(messageBody));
            }
            log.finer("Received message from SQS:\n" + BackupMaster.beautifyJson(messageBody));

            try {
                JsonNode json = MAPPER.readTree(messageBody);

                String jsonMessage = json.get("Message").asText().replace("\\\"", "\"");

                json = MAPPER.readTree(jsonMessage);
                String messageJobId = json.get("JobId").asText();
                String messageStatus = json.get("StatusMessage").asText();

                log.fine("Received job \"" + messageJobId + "\" with status \"" + messageStatus + "\"");

                // Don't process this message if it wasn't the job we were looking for
                if (!jobs.contains(messageJobId)) continue;

                try {
                    if (StatusCode.Succeeded.toString().equals(messageStatus)) 
                    {
                        log.fine("Notifying requestor of job \"" + messageJobId + "\"");
                        return messageJobId;
                    }
                    if (StatusCode.Failed.toString().equals(messageStatus)) {
                        throw new AmazonClientException("Archive retrieval failed");
                    }
                } finally {
                    deleteMessage(message);
                }
            } catch (IOException e) {
                throw new AmazonClientException("Unable to parse status message: " + messageBody, e);
            }
        }
    }
    
    private void deleteMessage(Message message) {
        try {
            log.fine("Removing message \"" + message.getReceiptHandle() + "\" from SQS queue \"" + queueUrl + "\"");
            amazonSQS_.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
        } catch (Exception e) {}
    }

    @Override
    public void close() {
        log.fine("Removing SQS queue \"" + queueUrl + "\"");
        amazonSQS_.deleteQueue(new DeleteQueueRequest(queueUrl));
    }
    
}
