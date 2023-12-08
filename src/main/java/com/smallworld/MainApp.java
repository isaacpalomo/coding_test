package com.smallworld;

import java.io.IOException;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MainApp {

    public static void main(String[] args) {
        TransactionDataFetcher dataFetcher = new TransactionDataFetcher();

        exportResultToJson("results/getTotalTransactionAmount.json", dataFetcher.getTotalTransactionAmount());
        exportResultToJson("results/getTotalTransactionAmountSentBy.json", dataFetcher.getTotalTransactionAmountSentBy("Tom Shelby"));
        exportResultToJson("results/getMaxTransactionAmount.json", dataFetcher.getMaxTransactionAmount());
        exportResultToJson("results/countUniqueClients.json", dataFetcher.countUniqueClients());
        exportResultToJson("results/hasOpenComplianceIssues.json", dataFetcher.hasOpenComplianceIssues("Billy Kimber"));
        exportResultToJson("results/getTransactionsByBeneficiaryName.json", dataFetcher.getTransactionsByBeneficiaryName());
        exportResultToJson("results/getUnsolvedIssueIds.json", dataFetcher.getUnsolvedIssueIds());
        exportResultToJson("results/getAllSolvedIssueMessages.json", dataFetcher.getAllSolvedIssueMessages());
        exportResultToJson("results/getTop3TransactionsByAmount.json", dataFetcher.getTop3TransactionsByAmount());
        exportResultToJson("results/getTopSender.json", dataFetcher.getTopSender());
        
    }

    private static void exportResultToJson(String fileName, Object result) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(Paths.get(fileName).toFile(), result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
