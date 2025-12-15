package com.pucminas.app.crud_serverless_java.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.net.URI;
import java.util.Map;

public class DeleteItemHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient ddbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localstack:4566"))
            .build();

    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE");

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setHeaders(Map.of("Access-Control-Allow-Origin", "*"));

        String itemId = request.getPathParameters() != null ? request.getPathParameters().get("id") : null;

        if (itemId == null) {
            return response.withStatusCode(400).withBody("{\"message\": \"ID do item é obrigatório.\"}");
        }

        try {
            DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("id", AttributeValue.builder().s(itemId).build()))
                    .build();

            ddbClient.deleteItem(deleteRequest);
            context.getLogger().log("Item " + itemId + " deletado com sucesso.");

            return response.withStatusCode(204);

        } catch (DynamoDbException e) {
            context.getLogger().log("Erro DynamoDB: " + e.getMessage());
            return response.withStatusCode(500).withBody("{\"message\": \"Erro ao deletar item: " + e.getMessage() + "\"}");
        }
    }
}
