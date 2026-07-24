import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Schema as MongooseSchema, Types } from 'mongoose';

export type TransferDocument = HydratedDocument<Transfer>;

export enum TransferStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Schema({ timestamps: true, collection: 'transfers' })
export class Transfer {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  fromWalletId: Types.ObjectId;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  toWalletId: Types.ObjectId;

  @Prop({ required: true })
  amount: number;

  @Prop({ type: String, enum: TransferStatus, default: TransferStatus.PENDING })
  status: TransferStatus;

  @Prop({
    unique: true,
    index: true,
    sparse: true, // Allows multiple null values, but unique for actual keys
  })
  idempotencyKey?: string;

  @Prop()
  failureReason?: string;

  createdAt?: Date;
  updatedAt?: Date;
}

export const TransferSchema = SchemaFactory.createForClass(Transfer);
