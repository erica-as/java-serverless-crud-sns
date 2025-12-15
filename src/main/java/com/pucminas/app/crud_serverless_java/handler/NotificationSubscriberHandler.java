package com.pucminas.app.crud_serverless_java.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class NotificationSubscriberHandler implements RequestHandler<SNSEvent, Void> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Void handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("--- INÍCIO DO PROCESSAMENTO DA NOTIFICAÇÃO SNS ---");

        for (SNSEvent.SNSRecord record : event.getRecords()) {
            String message = record.getSNS().getMessage();
            String subject = record.getSNS().getSubject();

            context.getLogger().log("Assunto da Notificação: " + subject);

            try {
                Map<String, Object> payload = mapper.readValue(message, Map.class);

                context.getLogger().log(">>> Evento Recebido: " + payload.get("action"));
                context.getLogger().log(">>> Item ID: " + payload.get("itemId"));
                context.getLogger().log(">>> Título: " + payload.get("title"));

            } catch (Exception e) {
                context.getLogger().log("Corpo da Mensagem (Raw): " + message);
            }
        }

        context.getLogger().log("--- FIM DO PROCESSAMENTO ---");
        return null;
    }
}