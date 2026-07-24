import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Transfer, TransferDocument, TransferStatus } from '../wallets/schemas/transfer.schema';
import { Wallet, WalletDocument } from '../wallets/schemas/wallet.schema';
import { Transaction, TransactionDocument, TransactionStatus, TransactionType } from '../transactions/schemas/transaction.schema';
import { LedgerService } from '../ledger/ledger.service';

@Injectable()
export class PendingTransferWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PendingTransferWorker.name);
  private timer: NodeJS.Timeout;

  constructor(
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private readonly transactionModel: Model<TransactionDocument>, // ✅ ADDED
    private readonly ledgerService: LedgerService,
    private readonly configService: ConfigService,
  ) {}

  onModuleInit() {
    const intervalMs = this.configService.getOrThrow<number>(
      'workers.pendingTransferSweepIntervalMs',
    );
    this.timer = setInterval(() => this.sweep(), intervalMs);
    this.logger.log('PendingTransferWorker initialized');
  }

  private async sweep() {
    const timeoutMs = this.configService.getOrThrow<number>('workers.pendingTransferTimeoutMs');
    const cutoff = new Date(Date.now() - timeoutMs);

    const stale = await this.transferModel
      .find({ 
        status: TransferStatus.PENDING, 
        createdAt: { $lt: cutoff } 
      })
      .exec();

    if (stale.length === 0) {
      return;
    }

    this.logger.warn(`Found ${stale.length} transfer(s) pending past the timeout window - attempting recovery`);

    for (const transfer of stale) {
      try {
        await this.recoverTransfer(transfer);
      } catch (error) {
        this.logger.error(`Failed to recover transfer ${transfer.id}: ${(error as Error).message}`);
      }
    }
  }

  private async recoverTransfer(transfer: TransferDocument) {
    this.logger.log(`Attempting to recover transfer ${transfer.id}`);

    // Check if transfer already has a credit transaction
    const creditTransaction = await this.transactionModel.findOne({
      transferId: transfer._id,
      type: TransactionType.TRANSFER_IN,
    });

    if (creditTransaction) {
      // Transfer is already completed, just update status
      transfer.status = TransferStatus.COMPLETED;
      await transfer.save();
      this.logger.log(`Transfer ${transfer.id} marked as COMPLETED (credit exists)`);
      return;
    }

    // Check if receiver wallet exists
    const toWallet = await this.walletModel.findById(transfer.toWalletId);
    if (!toWallet) {
      transfer.status = TransferStatus.FAILED;
      transfer.failureReason = 'Destination wallet not found during recovery';
      await transfer.save();
      this.logger.log(`Transfer ${transfer.id} marked as FAILED: wallet not found`);
      return;
    }

    // Check if debit transaction exists
    const debitTransaction = await this.transactionModel.findOne({
      transferId: transfer._id,
      type: TransactionType.TRANSFER_OUT,
    });

    if (!debitTransaction) {
      // No debit transaction - the transfer was never properly initiated
      transfer.status = TransferStatus.FAILED;
      transfer.failureReason = 'No debit transaction found during recovery';
      await transfer.save();
      this.logger.log(`Transfer ${transfer.id} marked as FAILED: no debit transaction`);
      return;
    }

    // Debit exists but credit doesn't - complete the transfer
    try {
      const session = await this.walletModel.db.startSession();
      
      try {
        await session.withTransaction(async () => {
          // Update receiver balance
          const updatedToWallet = await this.walletModel.findOneAndUpdate(
            {
              _id: toWallet._id,
            },
            {
              $inc: { balance: transfer.amount },
            },
            {
              new: true,
              session,
            }
          );

          if (!updatedToWallet) {
            throw new Error('Failed to update receiver wallet');
          }

          // Create credit transaction
          const [newCreditTransaction] = await this.transactionModel.create(
            [
              {
                walletId: toWallet._id,
                type: TransactionType.TRANSFER_IN,
                amount: transfer.amount,
                status: TransactionStatus.COMPLETED,
                balanceAfter: updatedToWallet.balance,
                transferId: transfer._id,
                counterpartyWalletId: transfer.fromWalletId,
              },
            ],
            { session },
          );

          // Record ledger credit
          await this.ledgerService.recordCredit(
            toWallet._id,
            newCreditTransaction._id,
            transfer.amount,
            updatedToWallet.balance,
            session,
          );

          // Mark transfer as COMPLETED
          transfer.status = TransferStatus.COMPLETED;
          await transfer.save({ session });
        });
      } finally {
        await session.endSession();
      }

      this.logger.log(`Transfer ${transfer.id} successfully completed during recovery`);
    } catch (error) {
      // If we can't complete it, mark as FAILED to avoid infinite retries
      transfer.status = TransferStatus.FAILED;
      transfer.failureReason = `Recovery failed: ${(error as Error).message}`;
      await transfer.save();
      this.logger.error(`Transfer ${transfer.id} marked as FAILED: ${(error as Error).message}`);
    }
  }

  onModuleDestroy() {
    clearInterval(this.timer);
    this.logger.log('PendingTransferWorker destroyed');
  }
}