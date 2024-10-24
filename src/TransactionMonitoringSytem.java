import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TransactionMonitoringSytem {

    /* Build a transaction monitoring subsystem
    Input to the subsystem will be transaction list csv file (max 10k rows) with following fields - user ID, timestamp, merchant name, amount
    Propose 5 fraud related transaction monitoring rules + a short explanation on why
    Implement the subsystem and flag suspicious transactions as the output */

    private static final double LARGE_AMOUNT_THRESHOLD = 10000;
    private static final int LOCATION_INCONSISTENCY_INTERVAL = 1; //In hours
    private static final int HIGH_FREQUENCY_LIMIT = 5;  // Number of transactions
    private static final int HIGH_FREQUENCY_TIME_LIMIT = 10;  // In minutes

    static class Transaction {
        String userID;
        LocalDateTime timestamp;
        String merchantName;
        double amount;
        String useCase;

        Transaction(String userID, LocalDateTime timestamp, String merchantName, double amount) {
            this.userID = userID;
            this.timestamp = timestamp;
            this.merchantName = merchantName;
            this.amount = amount;
            this.useCase = null;
        }
        Transaction(String userID, LocalDateTime timestamp, String merchantName, double amount, String useCase) {
            this.userID = userID;
            this.timestamp = timestamp;
            this.merchantName = merchantName;
            this.amount = amount;
            this.useCase = useCase;
        }
        @Override
        public String toString() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            return "User: " + userID + ", Merchant: " + merchantName + ", Amount: " + amount + ", Time: " + timestamp.format(formatter) + ", Flagged for: " + useCase;
        }
    }
    public List<Transaction> flagTransactions(List<Transaction> transactions) {

        List<Transaction> flaggedTransactions = new ArrayList<>();
        // each transaction analyze
        for (Transaction transaction: transactions){
            //use case 1: large transactions
            if (transaction.amount > LARGE_AMOUNT_THRESHOLD) {
                flaggedTransactions.add(new Transaction(transaction.userID, transaction.timestamp, transaction.merchantName, transaction.amount, "Large Transaction"));
            }
            //use case 2: transaction between midnight and 5am
            if (isOddHour(transaction.timestamp)) {
                flaggedTransactions.add(new Transaction(transaction.userID, transaction.timestamp, transaction.merchantName, transaction.amount, "Odd Hour Transaction"));
            }
        }

        //pattern detection
        Map<String, List<Transaction>> transactionsByUser = new HashMap<>();
        for (Transaction transaction: transactions) {
            String userID = transaction.userID;
            if (!transactionsByUser.containsKey(userID)) {
                transactionsByUser.put(userID, new ArrayList<>());
            }
            transactionsByUser.get(userID).add(transaction);
        }

        Map<String, List<Transaction>> transactionsPerMerchant = new HashMap<>();
        for (Transaction transaction: transactions) {
            String merchantName = transaction.merchantName;
            if (!transactionsPerMerchant.containsKey(merchantName)) {
                transactionsPerMerchant.put(merchantName, new ArrayList<>());
            }
            transactionsPerMerchant.get(merchantName).add(transaction);
        }

        for (Map.Entry<String, List<Transaction>> entry: transactionsByUser.entrySet()) {
            List<Transaction> userTransactions = entry.getValue();
            userTransactions.sort(Comparator.comparing(transaction -> transaction.timestamp));
            //use case 3: more than 10 transactions within 5 hours
            flaggedTransactions.addAll(highFrequencyTransactions(userTransactions));

            //use case 4: different location within 1 hour
            flaggedTransactions.addAll(locationInconsistentTransactions(userTransactions));
        }

        for (Map.Entry<String, List<Transaction>> entry: transactionsPerMerchant.entrySet()) {
            List<Transaction> merchantTransactions = entry.getValue();
            //use case 5: unusually large transactions
            flaggedTransactions.addAll(largeTransactionsByMedian(merchantTransactions));
        }
        return flaggedTransactions;
    }
    private static List<Transaction> loadTransactions(String filePath) {
        List<Transaction> transactions = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            File file = new File(filePath);
            Scanner scanner = new Scanner(file);
            if (scanner.hasNextLine()) {
                scanner.nextLine();
            }
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] fields = line.split(",");

                String userID = fields[0];
                LocalDateTime timestamp = LocalDateTime.parse(fields[1], formatter);
                String merchantName = fields[2];
                double amount = Double.parseDouble(fields[3]);
                transactions.add(new Transaction(userID, timestamp, merchantName, amount));
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred while loading transactions.");
            e.printStackTrace();
        }

        return transactions;
    }
    private static List<Transaction> highFrequencyTransactions(List<Transaction> userTransactions) {
        List<Transaction> flaggedTransactions = new ArrayList<>();

        for (int i = 0; i < userTransactions.size()-HIGH_FREQUENCY_LIMIT; i++) {
            Transaction fistTransaction = userTransactions.get(i);
            Transaction lastTransaction = userTransactions.get(i + HIGH_FREQUENCY_LIMIT -1);
            LocalDateTime firstTransactionTimestamp = fistTransaction.timestamp;
            LocalDateTime lastTransactionTimestamp = lastTransaction.timestamp;
            Duration duration = Duration.between(firstTransactionTimestamp, lastTransactionTimestamp);
            if (duration.toMinutes() <= HIGH_FREQUENCY_TIME_LIMIT) {
                for (Transaction transaction : userTransactions.subList(i, i + HIGH_FREQUENCY_LIMIT)) {
                    flaggedTransactions.add(new Transaction(transaction.userID, transaction.timestamp, transaction.merchantName, transaction.amount, "High Frequency"));
                }
                i = i + HIGH_FREQUENCY_LIMIT;
            }
        }
        return flaggedTransactions;
    }
    private static List<Transaction> locationInconsistentTransactions(List<Transaction> userTransactions) {
        List<Transaction> flaggedTransactions = new ArrayList<>();

        for (int i = 0; i < userTransactions.size()-1; i++) {
            Transaction firstTransaction = userTransactions.get(i);
            Transaction secondTransaction = userTransactions.get(i+1);

            if (!firstTransaction.merchantName.equals(secondTransaction.merchantName)) {
                LocalDateTime firstTransactionTimestamp = firstTransaction.timestamp;
                LocalDateTime secondTransactionTimestamp = secondTransaction.timestamp;
                Duration duration = Duration.between(firstTransactionTimestamp, secondTransactionTimestamp);
                if (duration.toHours() <= LOCATION_INCONSISTENCY_INTERVAL) {
                    flaggedTransactions.add(new Transaction(firstTransaction.userID, firstTransaction.timestamp, firstTransaction.merchantName, firstTransaction.amount, "Location Inconsistency"));
                    flaggedTransactions.add(new Transaction(secondTransaction.userID, secondTransaction.timestamp, secondTransaction.merchantName, secondTransaction.amount, "Location Inconsistency"));
                }
            }
        }
        return flaggedTransactions;
    }
    private static double getMedian(List<Double> amounts) {
        Collections.sort(amounts);

        int size = amounts.size();
        if (size % 2 == 0) {
            return (amounts.get(size / 2 - 1) + amounts.get(size / 2)) / 2.0;
        } else {
            return amounts.get(size / 2);
        }
    }
    private static List<Transaction> largeTransactionsByMedian(List<Transaction> merchantTransactions) {
        List<Transaction> flaggedTransactions = new ArrayList<>();
        List<Double> amounts = new ArrayList<>();
        for (Transaction transaction: merchantTransactions) {
            amounts.add(transaction.amount);
        }

        double median = getMedian(amounts);
        for (Transaction transaction: merchantTransactions) {
            if (transaction.amount >= median*10) {
                flaggedTransactions.add(new Transaction(transaction.userID, transaction.timestamp, transaction.merchantName, transaction.amount, "Unusually Large Transaction"));
            }
        }
        return flaggedTransactions;
    }
    private static boolean isOddHour(LocalDateTime timestamp){
        int hour = timestamp.getHour();
        return hour <= 5;
    }
    public static void main (String[] args) throws IOException {
        List<Transaction> transactions = new ArrayList<>();
        transactions = loadTransactions("src/transactions.csv");


        TransactionMonitoringSytem monitoringSytem = new TransactionMonitoringSytem();
        List<Transaction> flaggedTransactions = monitoringSytem.flagTransactions(transactions);
        flaggedTransactions.forEach(System.out::println);
    }
}