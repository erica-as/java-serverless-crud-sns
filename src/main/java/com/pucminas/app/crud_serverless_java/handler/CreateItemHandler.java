package com.pucminas.app.crud_serverless_java.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pucminas.app.crud_serverless_java.model.Item;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CreateItemHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final ObjectMapper mapper = new ObjectMapper();

    private final DynamoDbClient ddbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localstack:4566"))
            .build();
    private final SnsClient snsClient = SnsClient.builder()
            .endpointOverride(URI.create("http://localstack:4566"))
            .build();

    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE");
    private static final String SNS_TOPIC_ARN = System.getenv("SNS_TOPIC_ARN");

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setHeaders(Map.of("Content-Type", "application/json", "Access-Control-Allow-Origin", "*"));

        try {
            Item newItem = mapper.readValue(request.getBody(), Item.class);

            if (newItem.getTitle() == null || newItem.getTitle().trim().isEmpty()) {
                return response.withStatusCode(400).withBody("{\"message\": \"O campo 'title' é obrigatório para criação.\"}");
            }

            String itemId = UUID.randomUUID().toString();
            newItem.setId(itemId);

            Map<String, AttributeValue> itemMap = new HashMap<>();
            itemMap.put("id", AttributeValue.builder().s(newItem.getId()).build());
            itemMap.put("title", AttributeValue.builder().s(newItem.getTitle()).build());
            itemMap.put("description", AttributeValue.builder().s(newItem.getDescription() != null ? newItem.getDescription() : "").build());

            PutItemRequest putRequest = PutItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .item(itemMap)
                    .build();

            ddbClient.putItem(putRequest);

            String snsPayload = String.format(
                    "{\"itemId\":\"%s\", \"title\":\"%s\", \"action\":\"CREATED\"}",
                    newItem.getId(), newItem.getTitle()
            );

            PublishRequest publishRequest = PublishRequest.builder()
                    .topicArn(SNS_TOPIC_ARN)
                    .subject("Novo Item Criado")
                    .message(snsPayload)
                    .build();

            snsClient.publish(publishRequest);

            return response.withStatusCode(201).withBody(mapper.writeValueAsString(newItem));

        } catch (Exception e) {
            context.getLogger().log("Erro CREATE: " + e.getMessage());
            return response.withStatusCode(500).withBody("{\"message\": \"Erro interno do servidor: " + e.getMessage() + "\"}");
        }
    }
}