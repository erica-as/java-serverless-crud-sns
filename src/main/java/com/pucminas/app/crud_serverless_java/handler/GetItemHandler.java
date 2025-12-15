package com.pucminas.app.crud_serverless_java.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pucminas.app.crud_serverless_java.model.Item;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetItemHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final DynamoDbClient ddbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localstack:4566"))
            .build();

    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE");

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setHeaders(Map.of("Content-Type", "application/json", "Access-Control-Allow-Origin", "*"));

        String itemId = request.getPathParameters() != null ? request.getPathParameters().get("id") : null;

        try {
            if (itemId != null) {
                GetItemRequest getRequest = GetItemRequest.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of("id", AttributeValue.builder().s(itemId).build()))
                        .build();

                GetItemResponse result = ddbClient.getItem(getRequest);

                if (result.item().isEmpty()) {
                    return response.withStatusCode(404).withBody("{\"message\": \"Item não encontrado.\"}");
                }

                Item item = mapDynamoItemToModel(result.item());
                return response.withStatusCode(200).withBody(mapper.writeValueAsString(item));

            } else {
                ScanRequest scanRequest = ScanRequest.builder().tableName(TABLE_NAME).build();
                ScanResponse result = ddbClient.scan(scanRequest);

                List<Item> items = result.items().stream()
                        .map(this::mapDynamoItemToModel)
                        .collect(Collectors.toList());

                return response.withStatusCode(200).withBody(mapper.writeValueAsString(items));
            }
        } catch (Exception e) {
            context.getLogger().log("Erro GET: " + e.getMessage());
            return response.withStatusCode(500).withBody("{\"message\": \"Erro interno: " + e.getMessage() + "\"}");
        }
    }

    private Item mapDynamoItemToModel(Map<String, AttributeValue> itemMap) {
        Item item = new Item();
        item.setId(itemMap.get("id").s());
        if (itemMap.containsKey("title")) item.setTitle(itemMap.get("title").s());
        if (itemMap.containsKey("description")) item.setDescription(itemMap.get("description").s());
        return item;
    }
}
