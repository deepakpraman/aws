package com.bigdreams.aws.ddb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExportTable implements RequestHandler {

    private AmazonDynamoDB dynamoDb;

    @Override
    public Object handleRequest(Object o, Context context) {
        downloadAllRecords();
        return "Success";
    }

    public static void main(String[] args) {
            downloadAllRecords();
    }
    public static void downloadAllRecords() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(Regions.EU_WEST_1)
                .build();

        List<Map<String, AttributeValue>>items =  new ArrayList<>();
        ScanRequest scanRequest = new ScanRequest()
                .withTableName("nudge_tasks")
                .withConsistentRead(false)
                .withLimit(100);
        do {
            ScanResult result  = client.scan(scanRequest);
            Map<String, AttributeValue> lastEvaluatedKey = result.getLastEvaluatedKey();
            for (Map<String, AttributeValue> item : result.getItems()) {
                items.add(item);
            }
            scanRequest.setExclusiveStartKey(lastEvaluatedKey);
        } while (scanRequest.getExclusiveStartKey() != null);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        byte[] jsonResult = null;
        try {
            jsonResult = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(items);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println("Json file generated successfully.");
        ObjectMetadata omd = new ObjectMetadata();
        omd.setContentLength(jsonResult.length);
        s3Client.
                putObject("my-ddb-bucket","ddb/backup/"+ LocalDateTime.now()+".json".toString(),
                        new ByteArrayInputStream(jsonResult),omd);
    }
}
