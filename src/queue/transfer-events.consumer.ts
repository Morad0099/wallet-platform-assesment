import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { ConsumeMessage } from 'amqplib';
import { Model } from 'mongoose';
import { LedgerService } from '../ledger/ledger.service';
import {
  Transaction,
  TransactionDocument,
  TransactionStatus,
  TransactionType,
} from '../transactions/schemas/transaction.schema';
import { Transfer, TransferDocument, TransferStatus } from '../wallets/schemas/transfer.schema';
import { Wallet, WalletDocument } from '../wallets/schemas/wallet.schema';
import { RabbitMQService } from './rabbitmq.service';

export interface TransferInitiatedEvent {
  transferId: string;
  fromWalletId: string;
  toWalletId: string;
  amount: number;
}

@Injectable()
export class TransferEventsConsumer implements OnModuleInit {
  private readonly logger = new Logger(TransferEventsConsumer.name);

  constructor(
    private readonly rabbitMQService: RabbitMQService,
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name)
    private readonly transactionModel: Model<TransactionDocument>,
    private readonly ledgerService: LedgerService,
  ) {}

  onModuleInit() {
    const channelWrapper = this.rabbitMQService.getChannelWrapper();
    const queue = this.rabbitMQService.getTransferQueue();

    channelWrapper.addSetup((channel) =>
      channel.consume(queue, (message) => this.handleMessage(message, channel)),
    );
  }

  private async handleMessage(message: ConsumeMessage | null, channel: any) {
    if (!message) {
      return;
    }

    try {
      const event: TransferInitiatedEvent = JSON.parse(message.content.toString());
      await this.completeTransfer(event);
      channel.ack(message);
    } catch (error) {
      this.logger.error(`Failed to process transfer event: ${(error as Error).message}`);
      
      // Requeue for retry instead of dropping
      channel.nack(message, false, true); // Requeue the message
    }
  }

 private async completeTransfer(event: TransferInitiatedEvent) {
  const transfer = await this.transferModel.findById(event.transferId);
  if (!transfer) {
    this.logger.warn(`Transfer ${event.transferId} not found, skipping`);
    return;
  }

  // Idempotency: If already completed, skip
  if (transfer.status === TransferStatus.COMPLETED) {
    this.logger.log(`Transfer ${event.transferId} already completed, skipping`);
    return;
  }

  if (transfer.status === TransferStatus.FAILED) {
    this.logger.log(`Transfer ${event.transferId} already failed, skipping`);
    return;
  }

  const toWallet = await this.walletModel.findById(event.toWalletId);
  if (!toWallet) {
    transfer.status = TransferStatus.FAILED;
    transfer.failureReason = `Destination wallet ${event.toWalletId} not found`;
    await transfer.save();
    throw new Error(`Destination wallet not found`);
  }

  // Use the connection from the model
  const session = await this.walletModel.db.startSession();
  
  try {
    await session.withTransaction(async () => {
      // Optimistic locking for receiver
      const updatedWallet = await this.walletModel.findOneAndUpdate(
        {
          _id: toWallet._id,
          version: toWallet.version,
        },
        {
          $inc: { balance: event.amount, version: 1 },
        },
        {
          new: true,
          session,
        }
      );

      if (!updatedWallet) {
        throw new Error('Concurrent modification detected on receiver wallet');
      }

      // Create credit transaction
      const [creditTransaction] = await this.transactionModel.create(
        [
          {
            walletId: toWallet._id,
            type: TransactionType.TRANSFER_IN,
            amount: event.amount,
            status: TransactionStatus.COMPLETED,
            balanceAfter: updatedWallet.balance,
            transferId: transfer._id,
            counterpartyWalletId: transfer.fromWalletId,
          },
        ],
        { session },
      );

      await this.ledgerService.recordCredit(
        toWallet._id,
        creditTransaction._id,
        event.amount,
        updatedWallet.balance,
        session,
      );

      // Mark transfer as COMPLETED
      transfer.status = TransferStatus.COMPLETED;
      await transfer.save({ session });
    });
  } catch (error) {
    transfer.status = TransferStatus.FAILED;
    transfer.failureReason = (error as Error).message;
    await transfer.save();
    throw error;
  } finally {
    await session.endSession();
  }

  this.logger.log(`Transfer ${transfer.id} completed for wallet ${toWallet.id}`);
}
}
