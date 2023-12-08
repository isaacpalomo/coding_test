package com.smallworld;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TransactionDataFetcher {

    private List<Transaction> transactions;

    public TransactionDataFetcher() {
        // Initialize transactions by reading from the JSON file
        this.transactions = readTransactionsFromFile("transactions.json");
    }

    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount() {
        return transactions.stream()
            .filter(Transaction::isIssueSolved)
            .mapToDouble(Transaction::getAmount)
            .sum();
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderFullName) {
        return transactions.stream()
            .filter(transaction -> senderFullName.equals(transaction.getSenderFullName()))
            .filter(Transaction::isIssueSolved)
            .mapToDouble(Transaction::getAmount)
            .sum();
    }

    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount() {
        return transactions.stream()
            .filter(Transaction::isIssueSolved)
            .mapToDouble(Transaction::getAmount)
            .max()
            .orElse(0);
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() {
        Set<String> uniqueClients = new HashSet<>();

        transactions.stream()
                .filter(Transaction::isIssueSolved)
                .forEach(transaction -> {
                    uniqueClients.add(transaction.getSenderFullName());
                    uniqueClients.add(transaction.getBeneficiaryFullName());
                });

        return uniqueClients.size();
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) {
        Map<Integer, List<Transaction>> transactionsByMtn = transactions.stream()
                .collect(Collectors.groupingBy(Transaction::getMtn));

        List<Transaction> openComplianceIssues = transactionsByMtn.values().stream()
            .filter(group -> group.stream()
                    .noneMatch(transaction -> transaction.getIssueId() != null && transaction.isIssueSolved()))
            .flatMap(group -> group.stream()
                    .filter(transaction -> transaction.getIssueId() != null && transaction.getSenderFullName().equals(clientFullName)))
            .collect(Collectors.toList());

        return openComplianceIssues.size() > 0;
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, List<Transaction>> getTransactionsByBeneficiaryName() {
        return transactions.stream()
        .collect(Collectors.groupingBy(Transaction::getBeneficiaryFullName));
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() {
        Map<Integer, List<Transaction>> transactionsByMtn = transactions.stream()
                .collect(Collectors.groupingBy(Transaction::getMtn));

        return transactionsByMtn.values().stream()
                .filter(group -> group.stream()
                        .noneMatch(transaction -> transaction.getIssueId() != null && transaction.isIssueSolved()))
                .flatMap(group -> group.stream()
                        .filter(transaction -> transaction.getIssueId() != null)
                        .map(Transaction::getIssueId))
                .collect(Collectors.toSet());
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() {
        return transactions.stream()
                .filter(Transaction::isIssueSolved)
                .map(Transaction::getIssueMessage)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Returns the 3 transactions with highest amount sorted by amount descending
     */
    public List<Object> getTop3TransactionsByAmount() {
        return transactions.stream()
            .filter(Transaction::isIssueSolved)
            .sorted(Comparator.comparingDouble(Transaction::getAmount).reversed())
            .limit(3)
            .collect(Collectors.toList());
    }

    /**
     * Returns the sender with the most total sent amount
     */
    public String getTopSender() {
        Map<String, Double> senderTotalAmountMap = transactions.stream()
            .filter(Transaction::isIssueSolved)
            .collect(Collectors.groupingBy(Transaction::getSenderFullName,
                    Collectors.summingDouble(Transaction::getAmount)));

        Optional<Map.Entry<String, Double>> maxEntry = senderTotalAmountMap.entrySet().stream()
                .max(Map.Entry.comparingByValue());

        return maxEntry.map(Map.Entry::getKey).orElse(null);
    }

    private List<Transaction> readTransactionsFromFile(String filePath) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(Paths.get(filePath).toFile(), objectMapper.getTypeFactory().constructCollectionType(List.class, Transaction.class));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
