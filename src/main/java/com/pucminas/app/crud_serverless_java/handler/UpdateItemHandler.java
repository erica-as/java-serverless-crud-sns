package com.pucminas.app.crud_serverless_java.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pucminas.app.crud_serverless_java.model.Item;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.net.URI;
import java.util.Map;
import java.util.HashMap;

public class UpdateItemHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

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

        String itemId = request.getPathParameters() != null ? request.getPathParameters().get("id") : null;
        if (itemId == null) {
            return response.withStatusCode(400).withBody("{\"message\": \"ID do item é obrigatório na rota.\"}");
        }

        try {
            Item updatedItemData = mapper.readValue(request.getBody(), Item.class);

            if (updatedItemData.getTitle() == null || updatedItemData.getTitle().trim().isEmpty()) {
                return response.withStatusCode(400).withBody("{\"message\": \"O campo 'title' é obrigatório para atualização.\"}");
            }

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id", AttributeValue.builder().s(itemId).build());

            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            updates.put("title", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(updatedItemData.getTitle()).build())
                    .action(AttributeAction.PUT)
                    .build());
            updates.put("description", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(updatedItemData.getDescription() != null ? updatedItemData.getDescription() : "").build())
                    .action(AttributeAction.PUT)
                    .build());

            UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(key)
                    .attributeUpdates(updates)
                    .returnValues(ReturnValue.ALL_NEW)
                    .build();

            UpdateItemResponse updateResponse = ddbClient.updateItem(updateRequest);

            String snsPayload = String.format(
                    "{\"itemId\":\"%s\", \"title\":\"%s\", \"action\":\"UPDATED\"}",
                    itemId, updatedItemData.getTitle()
            );

            PublishRequest publishRequest = PublishRequest.builder()
                    .topicArn(SNS_TOPIC_ARN)
                    .subject("Item Atualizado")
                    .message(snsPayload)
                    .build();

            snsClient.publish(publishRequest);

            // 4. Resposta 200 OK
            Item finalItem = new Item();
            finalItem.setId(itemId);
            finalItem.setTitle(updateResponse.attributes().get("title").s());
            finalItem.setDescription(updateResponse.attributes().get("description").s());

            return response.withStatusCode(200).withBody(mapper.writeValueAsString(finalItem));

        } catch (Exception e) {
            context.getLogger().log("Erro UPDATE: " + e.getMessage());
            return response.withStatusCode(500).withBody("{\"message\": \"Erro interno do servidor: " + e.getMessage() + "\"}");
        }
    }
}
