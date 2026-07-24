import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import { LedgerEntry, LedgerEntryDocument } from '../ledger/schemas/ledger-entry.schema';
import { LedgerService } from '../ledger/ledger.service';
import { OutboxService } from '../outbox/outbox.service';
import { RabbitMQService } from '../queue/rabbitmq.service';
import { RedisService } from '../redis/redis.service';
import { TransactionsService } from '../transactions/transactions.service';
import {
  Transaction,
  TransactionDocument,
  TransactionStatus,
  TransactionType,
} from '../transactions/schemas/transaction.schema';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { DepositDto } from './dto/deposit.dto';
import { TransferDto } from './dto/transfer.dto';
import { WithdrawDto } from './dto/withdraw.dto';
import { Transfer, TransferDocument, TransferStatus } from './schemas/transfer.schema';
import { Wallet, WalletDocument } from './schemas/wallet.schema';

@Injectable()
export class WalletsService {
  constructor(
    @InjectConnection() private readonly connection: Connection,
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    @InjectModel(Transaction.name) private readonly transactionModel: Model<TransactionDocument>,
    @InjectModel(LedgerEntry.name) private readonly ledgerEntryModel: Model<LedgerEntryDocument>,
    private readonly transactionsService: TransactionsService,
    private readonly ledgerService: LedgerService,
    private readonly outboxService: OutboxService,
    private readonly rabbitMQService: RabbitMQService,
    private readonly redisService: RedisService,
  ) {}

  async createWallet(dto: CreateWalletDto) {
    const session = await this.connection.startSession();
    let wallet!: WalletDocument;

    try {
      await session.withTransaction(async () => {
        [wallet] = await this.walletModel.create(
          [
            {
              userId: dto.userId,
              ownerName: dto.ownerName,
              currency: dto.currency ?? 'GHS',
              balance: 0,
            },
          ],
          { session },
        );

        await this.outboxService.enqueue(
          'wallet.created',
          {
            walletId: wallet._id.toString(),
            userId: wallet.userId,
            currency: wallet.currency,
          },
          session,
        );
      });
    } finally {
      await session.endSession();
    }

    return wallet;
  }

  async getWallet(id: string) {
    const wallet = await this.walletModel.findById(id);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${id} not found`);
    }

    const cachedBalance = await this.redisService.getCachedBalance(id);
    if (cachedBalance !== null) {
      return { ...wallet.toObject(), balance: cachedBalance };
    }

    await this.redisService.setCachedBalance(id, wallet.balance);
    return wallet;
  }

async deposit(id: string, dto: DepositDto) {
  const wallet = await this.walletModel.findById(id);
  if (!wallet) {
    throw new NotFoundException(`Wallet ${id} not found`);
  }

  const session = await this.connection.startSession();
  let updatedWallet!: WalletDocument;

  try {
    await session.withTransaction(async () => {
      const result = await this.walletModel.findOneAndUpdate(
        {
          _id: wallet._id,
          version: wallet.version,
        },
        {
          $inc: { balance: dto.amount, version: 1 },
        },
        {
          new: true,
          session,
        }
      );

      if (!result) {
        throw new BadRequestException('Concurrent modification detected. Please retry.');
      }

      updatedWallet = result as WalletDocument;

      const transaction = await this.transactionsService.create({
        walletId: updatedWallet.id,
        type: TransactionType.DEPOSIT,
        amount: dto.amount,
        balanceAfter: updatedWallet.balance,
        reference: dto.reference,
      });

      await this.ledgerService.recordCredit(
        updatedWallet._id,
        transaction._id,
        dto.amount,
        updatedWallet.balance,
        session,
      );
    });
  } finally {
    await session.endSession();
  }

  return updatedWallet;
}

 async withdraw(id: string, dto: WithdrawDto) {
  const wallet = await this.walletModel.findById(id);
  if (!wallet) {
    throw new NotFoundException(`Wallet ${id} not found`);
  }

  if (wallet.balance < dto.amount) {
    throw new BadRequestException('Insufficient balance');
  }

  const session = await this.connection.startSession();
  let updatedWallet!: WalletDocument;

  try {
    await session.withTransaction(async () => {
      const result = await this.walletModel.findOneAndUpdate(
        {
          _id: wallet._id,
          balance: { $gte: dto.amount },
          version: wallet.version,
        },
        {
          $inc: { balance: -dto.amount, version: 1 },
        },
        {
          new: true,
          session,
        }
      );

      if (!result) {
        throw new BadRequestException('Insufficient balance or concurrent modification');
      }

      updatedWallet = result as WalletDocument;

      const transaction = await this.transactionsService.create({
        walletId: updatedWallet.id,
        type: TransactionType.WITHDRAWAL,
        amount: dto.amount,
        balanceAfter: updatedWallet.balance,
        reference: dto.reference,
      });

      await this.ledgerService.recordDebit(
        updatedWallet._id,
        transaction._id,
        dto.amount,
        updatedWallet.balance,
        session,
      );
    });
  } finally {
    await session.endSession();
  }

  return updatedWallet;
}

async transfer(dto: TransferDto) {
  if (dto.fromWalletId === dto.toWalletId) {
    throw new BadRequestException('Cannot transfer to the same wallet');
  }

  // Check if transfer already exists with this idempotency key
  if (dto.idempotencyKey) {
    const existingTransfer = await this.transferModel.findOne({
      idempotencyKey: dto.idempotencyKey,
    });
    
    if (existingTransfer) {
      return existingTransfer;
    }
  }

  // Fetch wallets with current version
  const [fromWallet, toWallet] = await Promise.all([
    this.walletModel.findById(dto.fromWalletId),
    this.walletModel.findById(dto.toWalletId),
  ]);

  if (!fromWallet || !toWallet) {
    throw new NotFoundException('Wallet not found');
  }

  if (fromWallet.balance < dto.amount) {
    throw new BadRequestException('Insufficient balance');
  }

  const session = await this.connection.startSession();
  let transfer!: TransferDocument;

  try {
    await session.withTransaction(async () => {
      // Optimistic locking: Update sender with version check
      const updatedFromWallet = await this.walletModel.findOneAndUpdate(
        { 
          _id: fromWallet._id, 
          balance: { $gte: dto.amount },
          version: fromWallet.version
        },
        { 
          $inc: { balance: -dto.amount, version: 1 }
        },
        { 
          new: true, 
          session 
        }
      );

      if (!updatedFromWallet) {
        throw new BadRequestException('Insufficient balance or concurrent modification');
      }

      // Optimistic locking: Update receiver with version check
      const updatedToWallet = await this.walletModel.findOneAndUpdate(
        { 
          _id: toWallet._id,
          version: toWallet.version
        },
        { 
          $inc: { balance: dto.amount, version: 1 }
        },
        { 
          new: true, 
          session 
        }
      );

      if (!updatedToWallet) {
        throw new Error('Failed to update receiver wallet');
      }

      // Create transfer record
      [transfer] = await this.transferModel.create(
        [
          {
            fromWalletId: fromWallet._id,
            toWalletId: toWallet._id,
            amount: dto.amount,
            status: TransferStatus.PENDING,
            idempotencyKey: dto.idempotencyKey,
          },
        ],
        { session },
      );

      // Create debit transaction for sender
      const [debitTransaction] = await this.transactionModel.create(
        [
          {
            walletId: fromWallet._id,
            type: TransactionType.TRANSFER_OUT,
            amount: dto.amount,
            status: TransactionStatus.COMPLETED,
            balanceAfter: updatedFromWallet.balance, // Use updated balance
            transferId: transfer._id,
            counterpartyWalletId: toWallet._id,
          },
        ],
        { session },
      );

      // Create credit transaction for receiver
      const [creditTransaction] = await this.transactionModel.create(
        [
          {
            walletId: toWallet._id,
            type: TransactionType.TRANSFER_IN,
            amount: dto.amount,
            status: TransactionStatus.COMPLETED,
            balanceAfter: updatedToWallet.balance, // Use updated balance
            transferId: transfer._id,
            counterpartyWalletId: fromWallet._id,
          },
        ],
        { session },
      );

      // Record ledger entries
      await this.ledgerService.recordDebit(
        fromWallet._id,
        debitTransaction._id,
        dto.amount,
        updatedFromWallet.balance,
        session,
      );

      await this.ledgerService.recordCredit(
        toWallet._id,
        creditTransaction._id,
        dto.amount,
        updatedToWallet.balance,
        session,
      );

      // Enqueue outbox event
      await this.outboxService.enqueue(
        'transfer.initiated',
        {
          transferId: transfer._id.toString(),
          fromWalletId: fromWallet._id.toString(),
          toWalletId: toWallet._id.toString(),
          amount: dto.amount,
        },
        session,
      );
    });
  } catch (error: any) {
    // Handle duplicate key error gracefully
    if (error.code === 11000 && dto.idempotencyKey) {
      const existing = await this.transferModel.findOne({
        idempotencyKey: dto.idempotencyKey,
      });
      if (existing) {
        return existing;
      }
    }
    throw error;
  } finally {
    await session.endSession();
  }

  return transfer;
}

  async getDashboard(id: string) {
    const wallet = await this.walletModel.findById(id);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${id} not found`);
    }

    const transactions = await this.transactionModel
      .find({ walletId: id })
      .sort({ createdAt: -1 })
      .exec();

    let totalDeposited = 0;
    let totalWithdrawn = 0;
    const recentActivity: Array<{
      transaction: TransactionDocument;
      entries: LedgerEntryDocument[];
    }> = [];

    for (const txn of transactions) {
      const entries = await this.ledgerEntryModel.find({ transactionId: txn._id }).exec();

      if (txn.type === TransactionType.DEPOSIT || txn.type === TransactionType.TRANSFER_IN) {
        totalDeposited += txn.amount;
      } else {
        totalWithdrawn += txn.amount;
      }

      recentActivity.push({ transaction: txn, entries });
    }

    return {
      wallet,
      totalDeposited,
      totalWithdrawn,
      transactionCount: transactions.length,
      recentActivity: recentActivity.slice(0, 10),
    };
  }
}
