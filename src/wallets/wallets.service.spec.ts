import { BadRequestException, NotFoundException } from '@nestjs/common';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { LedgerService } from '../ledger/ledger.service';
import { LedgerEntry } from '../ledger/schemas/ledger-entry.schema';
import { OutboxService } from '../outbox/outbox.service';
import { RabbitMQService } from '../queue/rabbitmq.service';
import { RedisService } from '../redis/redis.service';
import { Transaction, TransactionType } from '../transactions/schemas/transaction.schema';
import { TransactionsService } from '../transactions/transactions.service';
import { Transfer } from './schemas/transfer.schema';
import { Wallet } from './schemas/wallet.schema';
import { WalletsService } from './wallets.service';

describe('WalletsService', () => {
  let service: WalletsService;
  let walletModel: any;
  let transferModel: any;
  let transactionModel: any;
  let ledgerEntryModel: any;
  let transactionsService: any;
  let ledgerService: any;
  let outboxService: any;
  let rabbitMQService: any;
  let redisService: any;

  const mockSession = {
    withTransaction: jest.fn(async (fn: () => Promise<unknown>) => fn()),
    endSession: jest.fn(),
  };

  beforeEach(async () => {
    walletModel = {
      create: jest.fn(),
      findById: jest.fn(),
      findByIdAndUpdate: jest.fn(),
      findOneAndUpdate: jest.fn(),
    };
    transferModel = {
      create: jest.fn(),
      findOne: jest.fn(),
    };
    transactionModel = {
      create: jest.fn(),
      find: jest.fn(),
    };
    ledgerEntryModel = {
      find: jest.fn(),
    };
    transactionsService = { create: jest.fn() };
    ledgerService = { recordCredit: jest.fn(), recordDebit: jest.fn() };
    outboxService = { enqueue: jest.fn() };
    rabbitMQService = { publish: jest.fn() };
    redisService = {
      getCachedBalance: jest.fn(),
      setCachedBalance: jest.fn(),
      invalidateBalance: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WalletsService,
        {
          provide: getConnectionToken(),
          useValue: { startSession: jest.fn().mockResolvedValue(mockSession) },
        },
        { provide: getModelToken(Wallet.name), useValue: walletModel },
        { provide: getModelToken(Transfer.name), useValue: transferModel },
        { provide: getModelToken(Transaction.name), useValue: transactionModel },
        { provide: getModelToken(LedgerEntry.name), useValue: ledgerEntryModel },
        { provide: TransactionsService, useValue: transactionsService },
        { provide: LedgerService, useValue: ledgerService },
        { provide: OutboxService, useValue: outboxService },
        { provide: RabbitMQService, useValue: rabbitMQService },
        { provide: RedisService, useValue: redisService },
      ],
    }).compile();

    service = module.get(WalletsService);
  });

  afterEach(() => jest.clearAllMocks());

  describe('createWallet', () => {
    it('creates a wallet with a zero opening balance and enqueues a wallet.created event', async () => {
      const created = {
        _id: new Types.ObjectId(),
        userId: 'user-1',
        ownerName: 'Ama Owusu',
        balance: 0,
      };
      walletModel.create.mockResolvedValue([created]);

      const result = await service.createWallet({ userId: 'user-1', ownerName: 'Ama Owusu' });

      expect(walletModel.create).toHaveBeenCalledWith(
        [expect.objectContaining({ userId: 'user-1', balance: 0 })],
        expect.objectContaining({ session: mockSession }),
      );
      expect(outboxService.enqueue).toHaveBeenCalledWith(
        'wallet.created',
        expect.objectContaining({ walletId: created._id.toString() }),
        mockSession,
      );
      expect(result).toBe(created);
    });
  });

  describe('getWallet', () => {
    it('seeds the cache from Mongo on a cache miss', async () => {
      const wallet = {
        id: 'w1',
        _id: 'w1',
        balance: 250,
        toObject: () => ({ id: 'w1', balance: 250 }),
      };
      walletModel.findById.mockResolvedValue(wallet);
      redisService.getCachedBalance.mockResolvedValue(null);

      const result = await service.getWallet('w1');

      expect(redisService.setCachedBalance).toHaveBeenCalledWith('w1', 250);
      expect(result).toBe(wallet);
    });

    it('returns the cached balance instead of re-reading Mongo on a cache hit', async () => {
      const wallet = {
        id: 'w1',
        _id: 'w1',
        balance: 250,
        toObject: () => ({ id: 'w1', balance: 250 }),
      };
      walletModel.findById.mockResolvedValue(wallet);
      redisService.getCachedBalance.mockResolvedValue(99);

      const result = await service.getWallet('w1');

      expect(redisService.setCachedBalance).not.toHaveBeenCalled();
      expect(result).toEqual(expect.objectContaining({ balance: 99 }));
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      walletModel.findById.mockResolvedValue(null);

      await expect(service.getWallet('missing-id')).rejects.toThrow(NotFoundException);
    });
  });

  describe('deposit', () => {
  it('increments the balance atomically and records a ledger credit', async () => {
    const walletId = new Types.ObjectId().toString();
    const wallet = { _id: walletId, id: walletId, balance: 100, version: 0 };
    const updatedWallet = { _id: walletId, id: walletId, balance: 150, version: 1 };
    
    // Mock findById for initial fetch
    walletModel.findById.mockResolvedValue(wallet);
    
    // Mock findOneAndUpdate for optimistic update
    walletModel.findOneAndUpdate.mockResolvedValue(updatedWallet);
    
    const transaction = { _id: new Types.ObjectId() };
    transactionsService.create.mockResolvedValue(transaction);

    const result = await service.deposit(walletId, { amount: 50 });

    expect(walletModel.findById).toHaveBeenCalledWith(walletId);
    expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
      {
        _id: wallet._id,
        version: wallet.version,
      },
      {
        $inc: { balance: 50, version: 1 },
      },
      expect.objectContaining({ new: true, session: expect.anything() }),
    );
    expect(transactionsService.create).toHaveBeenCalledWith(
      expect.objectContaining({ type: TransactionType.DEPOSIT, amount: 50 }),
    );
    expect(ledgerService.recordCredit).toHaveBeenCalledWith(
      updatedWallet._id,
      transaction._id,
      50,
      150,
      expect.anything(),
    );
    expect(result).toBe(updatedWallet);
  });

  it('throws NotFoundException when the wallet does not exist', async () => {
    walletModel.findById.mockResolvedValue(null);

    await expect(service.deposit('missing-id', { amount: 10 })).rejects.toThrow(
      NotFoundException,
    );
  });
});

 describe('withdraw', () => {
  it('debits the wallet when the balance is sufficient', async () => {
    const walletId = new Types.ObjectId().toString();
    const wallet = { _id: walletId, id: walletId, balance: 100, version: 0 };
    const updatedWallet = { _id: walletId, id: walletId, balance: 60, version: 1 };
    
    // Mock findById for initial fetch
    walletModel.findById.mockResolvedValue(wallet);
    
    // Mock findOneAndUpdate for optimistic update
    walletModel.findOneAndUpdate.mockResolvedValue(updatedWallet);
    
    const transaction = { _id: new Types.ObjectId() };
    transactionsService.create.mockResolvedValue(transaction);

    const result = await service.withdraw(walletId, { amount: 40 });

    expect(walletModel.findById).toHaveBeenCalledWith(walletId);
    expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
      {
        _id: wallet._id,
        balance: { $gte: 40 },
        version: wallet.version,
      },
      {
        $inc: { balance: -40, version: 1 },
      },
      expect.objectContaining({ new: true, session: expect.anything() }),
    );
    expect(transactionsService.create).toHaveBeenCalledWith(
      expect.objectContaining({ type: TransactionType.WITHDRAWAL, amount: 40 }),
    );
    expect(ledgerService.recordDebit).toHaveBeenCalledWith(
      updatedWallet._id,
      transaction._id,
      40,
      60,
      expect.anything(),
    );
    expect(result).toBe(updatedWallet);
  });

  it('rejects a withdrawal larger than the current balance', async () => {
    const walletId = new Types.ObjectId().toString();
    const wallet = { _id: walletId, id: walletId, balance: 10, version: 0 };
    
    // Mock findById for initial fetch
    walletModel.findById.mockResolvedValue(wallet);

    await expect(service.withdraw(walletId, { amount: 40 })).rejects.toThrow(BadRequestException);
    
    // findOneAndUpdate should NOT be called
    expect(walletModel.findOneAndUpdate).not.toHaveBeenCalled();
  });

  it('throws NotFoundException when the wallet does not exist', async () => {
    walletModel.findById.mockResolvedValue(null);

    await expect(service.withdraw('missing-id', { amount: 10 })).rejects.toThrow(
      NotFoundException,
    );
  });
});

  describe('transfer', () => {
    const fromId = new Types.ObjectId();
    const toId = new Types.ObjectId();

    function mockWallets(fromBalance: number) {
      const fromWallet = { _id: fromId, balance: fromBalance, save: jest.fn() };
      const toWallet = { _id: toId, balance: 0 };
      walletModel.findById.mockImplementation((id: unknown) => {
        if (String(id) === String(fromId)) return Promise.resolve(fromWallet);
        if (String(id) === String(toId)) return Promise.resolve(toWallet);
        return Promise.resolve(null);
      });
      return { fromWallet, toWallet };
    }

    it('rejects transfers between the same wallet', async () => {
      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: fromId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(BadRequestException);
    });

    it('throws NotFoundException when either wallet is missing', async () => {
      walletModel.findById.mockResolvedValue(null);

      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: toId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(NotFoundException);
    });

    it('rejects a transfer larger than the sender balance', async () => {
      mockWallets(5);

      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: toId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(BadRequestException);
    });

   it('debits the sender, records a ledger entry, and enqueues a transfer.initiated event in the outbox', async () => {
  const { fromWallet } = mockWallets(100);
  const createdTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
  
  const updatedFromWallet = { _id: fromId, balance: 70, version: 1 };
  const updatedToWallet = { _id: toId, balance: 30, version: 1 };
  
  // Mock findOneAndUpdate
  walletModel.findOneAndUpdate
    .mockResolvedValueOnce(updatedFromWallet)
    .mockResolvedValueOnce(updatedToWallet);
  
  transferModel.findOne.mockResolvedValue(null);
  transferModel.create.mockResolvedValue([createdTransfer]);
  transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);

  const result = await service.transfer({
    fromWalletId: fromId.toString(),
    toWalletId: toId.toString(),
    amount: 30,
  });

  expect(walletModel.findOneAndUpdate).toHaveBeenCalledTimes(2);
  expect(ledgerService.recordDebit).toHaveBeenCalledWith(
    fromWallet._id,
    expect.anything(),
    30,
    70,
    mockSession,
  );
  expect(outboxService.enqueue).toHaveBeenCalledWith(
    'transfer.initiated',
    expect.objectContaining({ 
      transferId: createdTransfer._id.toString(), 
      amount: 30 
    }),
    mockSession,
  );
  expect(rabbitMQService.publish).not.toHaveBeenCalled();
  expect(result).toBe(createdTransfer);
});

   it('does not create a second transfer when retried with the same idempotency key', async () => {
  const { fromWallet } = mockWallets(100);
  const createdTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
  
  // Mock findOneAndUpdate for optimistic locking
  const updatedFromWallet = { _id: fromId, balance: 70, version: 1 };
  const updatedToWallet = { _id: toId, balance: 30, version: 1 };
  
  walletModel.findOneAndUpdate
    .mockResolvedValueOnce(updatedFromWallet)   // First call: sender
    .mockResolvedValueOnce(updatedToWallet)     // Second call: receiver
    .mockResolvedValueOnce(updatedFromWallet)   // Third call: sender (retry)
    .mockResolvedValueOnce(updatedToWallet);    // Fourth call: receiver (retry)
  
  transferModel.findOne
    .mockResolvedValueOnce(null)                // First call: no existing
    .mockResolvedValueOnce(createdTransfer);    // Second call: returns existing
  
  transferModel.create.mockResolvedValue([createdTransfer]);
  transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);

  const dto = {
    fromWalletId: fromId.toString(),
    toWalletId: toId.toString(),
    amount: 30,
    idempotencyKey: 'retry-key-1',
  };

  // First call - should create
  await service.transfer(dto);
  
  // Second call - should find existing and return it
  const result = await service.transfer(dto);

  // Verify create was called only once
  expect(transferModel.create).toHaveBeenCalledTimes(1);
  
  // Verify findOne was called twice
  expect(transferModel.findOne).toHaveBeenCalledTimes(2);
  
  // Verify the result is the existing transfer
  expect(result).toBe(createdTransfer);
});

    it('ends the Mongo session even when the transaction fails partway through', async () => {
  mockWallets(100);
  
  // Mock findOneAndUpdate to fail
  walletModel.findOneAndUpdate.mockRejectedValue(new Error('write conflict'));

  await expect(
    service.transfer({
      fromWalletId: fromId.toString(),
      toWalletId: toId.toString(),
      amount: 30,
    }),
  ).rejects.toThrow('write conflict');

  expect(mockSession.endSession).toHaveBeenCalled();
  expect(rabbitMQService.publish).not.toHaveBeenCalled();
});
  });
});
