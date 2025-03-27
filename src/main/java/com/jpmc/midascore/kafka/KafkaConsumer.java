package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepo;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


import java.util.Optional;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepo transactionRepo;


    @KafkaListener(topics = "${general.kafka-topic}",groupId = "midas-core-groupId")
    public void consumer(ConsumerRecord<String, Transaction> record){

        Transaction transaction = record.value();
        Optional<UserRecord> senderOpId = Optional.ofNullable(userRepository.findById(transaction.getSenderId()));
        Optional<UserRecord> recipientOpId = Optional.ofNullable(userRepository.findById(transaction.getRecipientId()));

        if(senderOpId.isPresent() && recipientOpId.isPresent()){
            UserRecord sender = senderOpId.get();
            UserRecord recipient = recipientOpId.get();
            System.out.println("--------------------------------------------------");
            System.out.println("           📌 Transaction Details                ");
            System.out.println("--------------------------------------------------");
            System.out.println("🔹 Sender Information:");
            System.out.println("   🆔 ID       : " + sender.getId());
            System.out.println("   👤 Name     : " + sender.getName());
            System.out.println("   💰 Balance  : $" + String.format("%.2f", sender.getBalance()));
            System.out.println("--------------------------------------------------");
            System.out.println("🔹 Recipient Information:");
            System.out.println("   🆔 ID       : " + recipient.getId());
            System.out.println("   👤 Name     : " + recipient.getName());
            System.out.println("   💰 Balance  : $" + String.format("%.2f", recipient.getBalance()));
            System.out.println("--------------------------------------------------");
            System.out.println("💵 Transaction Amount: $" + String.format("%.2f", transaction.getAmount()));
            System.out.println("--------------------------------------------------");

            if(sender.getBalance()>=transaction.getAmount()){
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());
                userRepository.save(sender);
                userRepository.save(recipient);
                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRepo.save(transactionRecord);
                System.out.println("==========================================");
                System.out.println(" ✅ TRANSACTION SUCCESSFUL                ");
                System.out.println("------------------------------------------");
                System.out.println(" 💸 Amount Transferred: $" + transaction.getAmount());
                System.out.println(" 👤 Sender: " + sender.getName() + " (ID: " + sender.getId() + ")");
                System.out.println(" 💰 Sender's New Balance: $" + sender.getBalance());
                System.out.println("------------------------------------------");
                System.out.println(" 👤 Recipient: " + recipient.getName() + " (ID: " + recipient.getId() + ")");
                System.out.println(" 💰 Recipient's New Balance: $" + recipient.getBalance());
                System.out.println("==========================================");

            }else{
                System.out.println("==========================================");
                System.out.println(" ❌  TRANSACTION FAILED                   ");
                System.out.println("------------------------------------------");
                System.out.println(" 🚨 Reason: Insufficient Balance          ");
                System.out.println("==========================================");
            }
        }else{
            System.out.println("==========================================");
            System.out.println(" ❌  TRANSACTION FAILED                   ");
            System.out.println("------------------------------------------");
            System.out.println(" 🚨 Reason: Invalid Sender or Recipient ID");
            System.out.println("==========================================");

        }

//        System.out.println("Kafka Consumer Received Message: " + transaction);

    }


}
