import { getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { LedgerService } from '../ledger/ledger.service';
import { Transaction, TransactionType } from '../transactions/schemas/transaction.schema';
import { Transfer, TransferStatus } from '../wallets/schemas/transfer.schema';
import { Wallet } from '../wallets/schemas/wallet.schema';
import { RabbitMQService } from './rabbitmq.service';
import { TransferEventsConsumer } from './transfer-events.consumer';

describe('TransferEventsConsumer', () => {
  let consumer: TransferEventsConsumer;
  let transferModel: any;
  let walletModel: any;
  let transactionModel: any;
  let ledgerService: any;

  const mockSession = {
    withTransaction: jest.fn(async (fn: () => Promise<unknown>) => fn()),
    endSession: jest.fn(),
  };

  beforeEach(async () => {
    transferModel = { 
      findById: jest.fn(),
      db: { startSession: jest.fn().mockResolvedValue(mockSession) },
    };
    
    walletModel = { 
      findById: jest.fn(),
      findOneAndUpdate: jest.fn(),
      db: { startSession: jest.fn().mockResolvedValue(mockSession) },
    };
    
    transactionModel = { create: jest.fn() };
    ledgerService = { recordCredit: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TransferEventsConsumer,
        {
          provide: RabbitMQService,
          useValue: { getChannelWrapper: jest.fn(), getTransferQueue: jest.fn() },
        },
        { provide: getModelToken(Transfer.name), useValue: transferModel },
        { provide: getModelToken(Wallet.name), useValue: walletModel },
        { provide: getModelToken(Transaction.name), useValue: transactionModel },
        { provide: LedgerService, useValue: ledgerService },
      ],
    }).compile();

    consumer = module.get(TransferEventsConsumer);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('credits the destination wallet and marks the transfer completed', async () => {
    const transfer = {
      _id: new Types.ObjectId(),
      id: 'transfer-1',
      save: jest.fn().mockResolvedValue(undefined),
      status: TransferStatus.PENDING,
      fromWalletId: 'wallet-1',
      failureReason: undefined,
    };
    
    const toWallet = { 
      _id: new Types.ObjectId(), 
      id: 'wallet-2', 
      balance: 100,
      version: 0,
    };
    
    const updatedToWallet = { 
      _id: toWallet._id, 
      id: 'wallet-2', 
      balance: 125,
      version: 1,
    };
    
    transferModel.findById.mockResolvedValue(transfer);
    walletModel.findById.mockResolvedValue(toWallet);
    walletModel.findOneAndUpdate.mockResolvedValue(updatedToWallet);
    
    const creditTransaction = { _id: new Types.ObjectId() };
    transactionModel.create.mockResolvedValue([creditTransaction]);

    await (consumer as any).completeTransfer({
      transferId: transfer._id.toString(),
      fromWalletId: 'wallet-1',
      toWalletId: toWallet._id.toString(),
      amount: 25,
    });

    expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
      {
        _id: toWallet._id,
        version: toWallet.version,
      },
      {
        $inc: { balance: 25, version: 1 },
      },
      expect.objectContaining({ new: true, session: expect.anything() }),
    );
    
    expect(transactionModel.create).toHaveBeenCalledWith(
      [
        expect.objectContaining({ 
          type: TransactionType.TRANSFER_IN, 
          amount: 25, 
          balanceAfter: 125 
        }),
      ],
      { session: mockSession },
    );
    
    expect(ledgerService.recordCredit).toHaveBeenCalledWith(
      toWallet._id,
      creditTransaction._id,
      25,
      125,
      mockSession,
    );
    
    expect(transfer.status).toBe(TransferStatus.COMPLETED);
    expect(transfer.save).toHaveBeenCalledWith({ session: mockSession });
  });

  it('skips processing when the transfer no longer exists', async () => {
    transferModel.findById.mockResolvedValue(null);

    await (consumer as any).completeTransfer({
      transferId: 'missing',
      fromWalletId: 'wallet-1',
      toWalletId: 'wallet-2',
      amount: 25,
    });

    expect(walletModel.findById).not.toHaveBeenCalled();
  });

  it('skips processing when transfer is already completed (idempotency)', async () => {
    const transfer = {
      _id: new Types.ObjectId(),
      id: 'transfer-1',
      status: TransferStatus.COMPLETED,
      save: jest.fn(), // Add save
      failureReason: undefined, // Add failureReason
    };
    
    transferModel.findById.mockResolvedValue(transfer);

    await (consumer as any).completeTransfer({
      transferId: transfer._id.toString(),
      fromWalletId: 'wallet-1',
      toWalletId: 'wallet-2',
      amount: 25,
    });

    expect(walletModel.findById).not.toHaveBeenCalled();
    expect(transfer.save).not.toHaveBeenCalled();
  });

  it('skips processing when transfer is already failed', async () => {
    const transfer = {
      _id: new Types.ObjectId(),
      id: 'transfer-1',
      status: TransferStatus.FAILED,
      save: jest.fn(), // Add save
      failureReason: 'Already failed', // Add failureReason
    };
    
    transferModel.findById.mockResolvedValue(transfer);

    await (consumer as any).completeTransfer({
      transferId: transfer._id.toString(),
      fromWalletId: 'wallet-1',
      toWalletId: 'wallet-2',
      amount: 25,
    });

    expect(walletModel.findById).not.toHaveBeenCalled();
    expect(transfer.save).not.toHaveBeenCalled();
  });

  it('marks transfer as FAILED when destination wallet is not found', async () => {
  const transfer = {
    _id: new Types.ObjectId(),
    id: 'transfer-1',
    save: jest.fn().mockResolvedValue(undefined),
    status: TransferStatus.PENDING,
    fromWalletId: 'wallet-1',
    failureReason: undefined,
  };
  
  transferModel.findById.mockResolvedValue(transfer);
  walletModel.findById.mockResolvedValue(null);

  await expect(
    (consumer as any).completeTransfer({
      transferId: transfer._id.toString(),
      fromWalletId: 'wallet-1',
      toWalletId: 'wallet-2',
      amount: 25,
    })
  ).rejects.toThrow('Destination wallet not found');

  expect(transfer.status).toBe(TransferStatus.FAILED);
  // Update to match the actual error message format
  expect(transfer.failureReason).toBe('Destination wallet wallet-2 not found');
  expect(transfer.save).toHaveBeenCalled();
});
});