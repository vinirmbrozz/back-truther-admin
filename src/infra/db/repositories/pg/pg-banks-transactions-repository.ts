import { PoolClient } from "pg";
import { PostgresDatabase } from "../../pg/connection";
import { PaginatedResult, PaginationParams } from "@/shared/pagination";
import { BanksTransactionsRepository } from "@/domain/transactions/repositories/banks-transactions-repository";
import { PixOutTransaction } from "@/domain/transactions/model/pix-out-transaction";
import { PixInTransaction } from "@/domain/transactions/model/pix-in-transaction";
import {
  PixOutPaginationParams,
  PixInPaginationParams,
  BilletCashoutParams,
  BridgeParams,
  UserTransactionsParams,
  AtmParams,
} from "@/domain/transactions/model/pix-pagination-params";
import { BilletCashoutTransaction } from "@/domain/transactions/model/billet-cashout-transaction";
import { BridgeTransaction } from "@/domain/transactions/model/bridge-transaction";
import { UserTransaction } from "@/domain/transactions/model/user-transaction";
import { AtmTransaction } from "@/domain/transactions/model/atm-transaction";

export class PgBanksTransactionsRepository
  implements BanksTransactionsRepository
{
  private async getClientBanks(): Promise<PoolClient> {
    return PostgresDatabase.getClient("banks");
  }

  private async getClient(): Promise<PoolClient> {
    return PostgresDatabase.getClient();
  }

  private async getClientTruther(): Promise<PoolClient> {
    return PostgresDatabase.getClient("truther");
  }

  async findPixOutPaginated(
    params: PixOutPaginationParams
  ): Promise<PaginatedResult<PixOutTransaction>> {
    const {
      page,
      limit,
      sortBy = 'px."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      wallet,
      wallets,
      txid,
      end2end,
      pixKey,
      receiverDocument,
      receiverName,
      status_px,
      status_bk,
      min_amount,
      max_amount,
    } = params;

    const offset = (page - 1) * limit;

    const allowedSortBy = new Set<string>([
      "ob.id",
      "ob.txid",
      "px.end2end",
      'ob."sender"',
      'aw."name"',
      'aw."document"',
      'px."amount"',
      'px."status"',
      "ob.status",
      'px."createdAt"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'px."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";
    const orderByColumn =
      safeSortBy === 'px."amount"' ? `px."amount"` : safeSortBy;

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    const walletList = (wallets && wallets.length > 0 ? wallets : []).concat(
      wallet ? [wallet] : []
    );
    if (walletList.length > 0) {
      const startIndex = values.length + 1;
      const placeholders = walletList
        .map((_, i) => `$${startIndex + i}`)
        .join(", ");
      where.push(`ob."sender" IN (${placeholders})`);
      values.push(...walletList);
    }

    pushWhere("ob.txid =", txid);
    pushWhere("px.end2end =", end2end);
    pushWhere('px."pixKey" =', pixKey);
    pushWhere('px."receiverDocument" =', receiverDocument);

    if (receiverName) {
      values.push(`%${receiverName}%`);
      where.push(`px."receiverName" ILIKE $${values.length}`);
    }

    pushWhere('px."status" =', status_px);
    pushWhere("ob.status =", status_bk);
    if (min_amount !== undefined) {
      //Amount na pixCashout é numeric
      values.push(min_amount);
      where.push(`px."amount" >= $${values.length}`);
    }
    if (max_amount !== undefined) {
      //Amount na pixCashout é numeric
      values.push(max_amount);
      where.push(`px."amount" <= $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`px."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`px."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const select = `
      SELECT 
        ob.id,
        ob.txid,
        px.end2end,
        ob."sender" AS sender,
        aw."name" AS sender_name,
        aw."document" AS sender_document,
        px."amount" AS amount_brl,
        px."status" AS status_px,
        ob.status AS status_bk,
        px."createdAt"::text AS date_op,
        px."receiverDocument" as receiver_document,
        px."receiverName" as receiver_name,
        px."pixKey",
        t.symbol AS token_symbol
      FROM "orderBuy" AS ob
      LEFT JOIN "pixCashout" AS px ON px."orderId" = ob."id"
      LEFT JOIN "aclWallets" AS aw ON ob."sender" = aw.wallet
      LEFT JOIN "tokens" AS t ON t.id = ob."tokensId"
    `;

    const orderLimit = `
      ORDER BY ${orderByColumn} ${safeSortOrder}
      LIMIT $${values.length + 1}
      OFFSET $${values.length + 2}
    `;

    const query = `${select} ${whereClause} ${orderLimit}`;

    const countQuery = `
      SELECT COUNT(*)::int AS total
      FROM "orderBuy" AS ob
      LEFT JOIN "pixCashout" AS px ON px."orderId" = ob."id"
      LEFT JOIN "aclWallets" AS aw ON ob."sender" = aw.wallet
      ${whereClause}
    `;

    const client = await this.getClientBanks();
    try {
      const result = await client.query(query, [...values, limit, offset]);
      const count = await client.query(countQuery, values);
      return {
        data: result.rows as PixOutTransaction[],
        total: Number(count.rows[0]?.total ?? 0),
        page,
        limit,
      };
    } finally {
      client.release();
    }
  }

  async findPixOutAll(
    params: Omit<PixOutPaginationParams, 'page' | 'limit'>
  ): Promise<PixOutTransaction[]> {
    const {
      sortBy = 'px."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      wallet,
      wallets,
      txid,
      end2end,
      pixKey,
      receiverDocument,
      receiverName,
      status_px,
      status_bk,
      min_amount,
      max_amount,
    } = params;

    const allowedSortBy = new Set<string>([
      "ob.id",
      "ob.txid",
      "px.end2end",
      'ob."sender"',
      'aw."name"',
      'aw."document"',
      'px."amount"',
      'px."status"',
      "ob.status",
      'px."createdAt"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'px."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";
    const orderByColumn = safeSortBy === 'px."amount"' ? `px."amount"` : safeSortBy;

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    const walletList = (wallets && wallets.length > 0 ? wallets : []).concat(
      wallet ? [wallet] : []
    );
    if (walletList.length > 0) {
      const startIndex = values.length + 1;
      const placeholders = walletList
        .map((_, i) => `$${startIndex + i}`)
        .join(", ");
      where.push(`ob."sender" IN (${placeholders})`);
      values.push(...walletList);
    }

    pushWhere("ob.txid =", txid);
    pushWhere("px.end2end =", end2end);
    pushWhere('px."pixKey" =', pixKey);
    pushWhere('px."receiverDocument" =', receiverDocument);

    if (receiverName) {
      values.push(`%${receiverName}%`);
      where.push(`px."receiverName" ILIKE $${values.length}`);
    }

    pushWhere('px."status" =', status_px);
    pushWhere("ob.status =", status_bk);
    if (min_amount !== undefined) {
      values.push(min_amount);
      where.push(`px."amount" >= $${values.length}`);
    }
    if (max_amount !== undefined) {
      values.push(max_amount);
      where.push(`px."amount" <= $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`px."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`px."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const select = `
      SELECT 
        ob.id,
        ob.txid,
        px.end2end,
        ob."sender" AS sender,
        aw."name" AS sender_name,
        aw."document" AS sender_document,
        px."amount" AS amount_brl,
        px."status" AS status_px,
        ob.status AS status_bk,
        px."createdAt"::text AS date_op,
        px."receiverDocument" as receiver_document,
        px."receiverName" as receiver_name,
        px."pixKey",
        t.symbol AS token_symbol
      FROM "orderBuy" AS ob
      LEFT JOIN "pixCashout" AS px ON px."orderId" = ob."id"
      LEFT JOIN "aclWallets" AS aw ON ob."sender" = aw.wallet
      LEFT JOIN "tokens" AS t ON t.id = ob."tokensId"
    `;

    const query = `${select} ${whereClause} ORDER BY ${orderByColumn} ${safeSortOrder}`;

    const client = await this.getClientBanks();
    try {
      const result = await client.query(query, values);
      return result.rows as PixOutTransaction[];
    } finally {
      client.release();
    }
  }

  async findPixInPaginated(
    params: PixInPaginationParams
  ): Promise<PaginatedResult<PixInTransaction>> {
    const {
      page,
      limit,
      sortBy = 'px."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      wallet,
      wallets,
      txid,
      end2end,
      destinationKey,
      payerDocument,
      payerName,
      status_bank,
      status_blockchain,
      typeIn,
      min_amount,
      max_amount,
    } = params;

    const offset = (page - 1) * limit;

    const allowedSortBy = new Set<string>([
      "ob.id",
      "aw.id",
      "ob.txid",
      "ob.wallet",
      'aw."name"',
      'aw."document"',
      'px."destinationKey"',
      "px.end2end",
      'px."PayerName"',
      'px."payerDocument"',
      'px."amount"',
      'px."status"',
      "ob.status",
      'px."createdAt"',
      'ob."typeIn"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'px."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";
    const orderByColumn =
      safeSortBy === 'px."amount"'
        ? `REPLACE(px."amount", ',', '.')::numeric`
        : safeSortBy;

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    const walletList = (wallets && wallets.length > 0 ? wallets : []).concat(
      wallet ? [wallet] : []
    );
    if (walletList.length > 0) {
      const startIndex = values.length + 1;
      const placeholders = walletList
        .map((_, i) => `$${startIndex + i}`)
        .join(", ");
      where.push(`ob.wallet IN (${placeholders})`);
      values.push(...walletList);
    }

    pushWhere("ob.txid =", txid);
    pushWhere("px.end2end =", end2end);
    pushWhere('px."destinationKey" =', destinationKey);
    pushWhere('px."payerDocument" =', payerDocument);

    if (payerName) {
      values.push(`%${payerName}%`);
      where.push(`px."PayerName" ILIKE $${values.length}`);
    }

    pushWhere('px."status" =', status_bank);
    pushWhere("ob.status =", status_blockchain);
    pushWhere('ob."typeIn" =', typeIn);

    if (min_amount !== undefined) {
      //Amount na pixIn é varchar
      values.push(min_amount);
      where.push(
        `REPLACE(px."amount", ',', '.')::numeric >= $${values.length}`
      );
    }
    if (max_amount !== undefined) {
      //Amount na pixIn é varchar
      values.push(max_amount);
      where.push(
        `REPLACE(px."amount", ',', '.')::numeric <= $${values.length}`
      );
    }

    if (created_after) {
      values.push(created_after);
      where.push(`px."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`px."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const select = `
      SELECT 
        ob.id,
        aw.id AS wallet_id,
        ob.txid,
        ob.wallet AS receive_wallet,
        aw."name" AS receive_name,
        aw."document" AS receive_doc,
        px."destinationKey",
        px.end2end,
        px."PayerName" AS payer_name,
        px."payerDocument" as payer_document,
        px."amount",
        px."status" AS status_bank,
        ob.status AS status_blockchain,
        ob."msgError" AS msg_error_blockchain,
        px."msgError" AS msg_error_bank,
        px."createdAt"::text AS "createdAt",
        ob."typeIn",
        t.symbol AS token_symbol
      FROM "orderSell" AS ob
      LEFT JOIN "pixIn" AS px ON px.id = ob."pixInId"
      LEFT JOIN "aclWallets" AS aw ON aw.wallet = ob.wallet OR aw."btcWallet" = ob.wallet OR aw."liquidWallet" = ob.wallet
      LEFT JOIN "tokens" AS t ON t.id = ob."tokensId"
    `;

    const orderLimit = `
      ORDER BY ${orderByColumn} ${safeSortOrder}
      LIMIT $${values.length + 1}
      OFFSET $${values.length + 2}
    `;

    const query = `${select} ${whereClause} ${orderLimit}`;

    const countQuery = `
      SELECT COUNT(*)::int AS total
      FROM "orderSell" AS ob
      LEFT JOIN "pixIn" AS px ON px.id = ob."pixInId"
      LEFT JOIN "aclWallets" AS aw ON aw.wallet = ob.wallet OR aw."btcWallet" = ob.wallet OR aw."liquidWallet" = ob.wallet
      ${whereClause}
    `;

    const client = await this.getClientBanks();
    try {
      const result = await client.query(query, [...values, limit, offset]);
      const count = await client.query(countQuery, values);
      return {
        data: result.rows as PixInTransaction[],
        total: Number(count.rows[0]?.total ?? 0),
        page,
        limit,
      };
    } finally {
      client.release();
    }
  }

  async findBilletCashoutPaginated(
    params: BilletCashoutParams
  ): Promise<PaginatedResult<BilletCashoutTransaction>> {
    const {
      page,
      limit,
      created_after,
      created_before,
      status,
      receiverName,
      receiverDocument,
      min_amount,
      max_amount,
      banksId,
      orderId,
    } = params;

    const client = await this.getClientBanks();
    const offset = (page - 1) * limit;

    const filters: string[] = [];
    const values: any[] = [];

    const addFilter = (condition: string, value: any) => {
      if (value !== undefined && value !== null && value !== "") {
        values.push(value);
        filters.push(condition.replace(/\$idx/g, `$${values.length}`));
      }
    };

    addFilter(
      `LOWER(t."receiverName") LIKE LOWER($idx)`,
      receiverName ? `%${receiverName}%` : undefined
    );
    addFilter(`t."receiverDocument" = $idx`, receiverDocument);
    addFilter(`UPPER(t."status"::text) = UPPER($idx)`, status);
    addFilter(`t."createdAt" >= $idx`, created_after);
    addFilter(`t."createdAt" <= $idx`, created_before);
    addFilter(`t."amount" >= $idx`, min_amount);
    addFilter(`t."amount" <= $idx`, max_amount);
    addFilter(`t."banksId" = $idx`, banksId);
    addFilter(`t."orderId" = $idx`, orderId);

    const whereClause = filters.length ? `WHERE ${filters.join(" AND ")}` : "";

    values.push(limit);
    const limitIndex = values.length;
    values.push(offset);
    const offsetIndex = values.length;

    const query = `
      SELECT 
        t."id", 
        t."uuid", 
        t."identifier", 
        t."movimentCode", 
        t."transactionCode",
        t."transactionIdentifier", 
        t."aditionalInfor", 
        t."receiverName",
        t."receiverDocument", 
        t."brcode", 
        t."msgError", 
        t."tryAgain",
        t."status", 
        t."countTimer", 
        t."refundMovimentCode", 
        t."createdAt",
        t."updateAt", 
        t."banksId", 
        t."orderId", 
        t."feeSymbol", 
        t."price",
        t."fee", 
        t."amount", 
        t."typeBoleto", 
        t."module",
        COUNT(*) OVER() AS total_count
      FROM "billetCashout" t
      LEFT JOIN "orderBuy" ob ON t."orderId" = ob.id
      LEFT JOIN "aclWallets" aw ON ob.sender = aw.wallet
      ${whereClause}
      ORDER BY t."createdAt" DESC
      LIMIT $${limitIndex} OFFSET $${offsetIndex};
    `;

    try {
      const result = await client.query(query, values);
      const total = result.rows.length ? Number(result.rows[0].total_count) : 0;

      return {
        data: result.rows as BilletCashoutTransaction[],
        total,
        page,
        limit,
      };
    } finally {
      client.release();
    }
  }

  async findBridgePaginated(
    params: BridgeParams
  ): Promise<PaginatedResult<BridgeTransaction>> {
    const {
      page,
      limit,
      user_id,
      wallet_id,
      value,
      status,
      sortBy = "created_at",
      sortOrder = "DESC",
      created_after,
      created_before,
    } = params;

    const client = await this.getClientTruther();
    const offset = (page - 1) * limit;

    const allowedSortBy = ["id", "created_at", "value", "status"];
    const safeSortBy = allowedSortBy.includes(sortBy) ? sortBy : "created_at";
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    const where: string[] = [];
    const values: any[] = [];

    if (user_id) {
      values.push(user_id);
      where.push(`t.user_id = $${values.length}`);
    }

    if (wallet_id) {
      values.push(wallet_id);
      where.push(`t.wallet_id = $${values.length}`);
    }

    if (value) {
      values.push(value);
      where.push(`t.value = $${values.length}`);
    }

    if (status) {
      values.push(status);
      where.push(`t.status = $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`t.created_at >= $${values.length}`);
    }

    if (created_before) {
      values.push(created_before);
      where.push(`t.created_at <= $${values.length}`);
    }

    const bridgeCondition = `
      t.id IN (
        SELECT parent_transaction_id
        FROM public.transactions
        WHERE parent_transaction_id IS NOT NULL
        UNION
        SELECT id
        FROM public.transactions
        WHERE parent_transaction_id IS NOT NULL
      )
    `;

    const whereClause =
      where.length > 0
        ? `WHERE ${bridgeCondition} AND ${where.join(" AND ")}`
        : `WHERE ${bridgeCondition}`;

    try {
      const countQuery = `
        SELECT COUNT(*)::int AS total
        FROM public.transactions t
        ${whereClause};
      `;

      const countResult = await client.query(countQuery, values);
      const total = Number(countResult.rows[0].total);

      const dataQuery = `
        SELECT
          t.id,
          t.user_id,
          t.wallet_id,
          t.from_address,
          t.to_address,
          t.value,
          t.tx_hash,
          t.status,
          t.created_at,
          t.flow,
          t.type,
          t.symbol,
          t.retry_count,
          t.protocol_destination
        FROM public.transactions t
        ${whereClause}
        ORDER BY t.${safeSortBy} ${safeSortOrder}
        LIMIT $${values.length + 1}
        OFFSET $${values.length + 2};
      `;

      const result = await client.query(dataQuery, [...values, limit, offset]);

      return {
        data: result.rows as BridgeTransaction[],
        total,
        page,
        limit,
      };
    } finally {
      client.release();
    }
  }

  async findPixInAll(
    params: Omit<PixInPaginationParams, 'page' | 'limit'>
  ): Promise<PixInTransaction[]> {
    const {
      sortBy = 'px."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      wallet,
      wallets,
      txid,
      end2end,
      destinationKey,
      payerDocument,
      payerName,
      status_bank,
      status_blockchain,
      typeIn,
      min_amount,
      max_amount,
    } = params;

    const allowedSortBy = new Set<string>([
      "ob.id",
      "aw.id",
      "ob.txid",
      "ob.wallet",
      'aw."name"',
      'aw."document"',
      'px."destinationKey"',
      "px.end2end",
      'px."PayerName"',
      'px."payerDocument"',
      'px."amount"',
      'px."status"',
      "ob.status",
      'px."createdAt"',
      'ob."typeIn"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'px."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";
    const orderByColumn =
      safeSortBy === 'px."amount"'
        ? `REPLACE(px."amount", ',', '.')::numeric`
        : safeSortBy;

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    const walletList = (wallets && wallets.length > 0 ? wallets : []).concat(
      wallet ? [wallet] : []
    );
    if (walletList.length > 0) {
      const startIndex = values.length + 1;
      const placeholders = walletList
        .map((_, i) => `$${startIndex + i}`)
        .join(", ");
      where.push(`ob.wallet IN (${placeholders})`);
      values.push(...walletList);
    }

    pushWhere("ob.txid =", txid);
    pushWhere("px.end2end =", end2end);
    pushWhere('px."destinationKey" =', destinationKey);
    pushWhere('px."payerDocument" =', payerDocument);

    if (payerName) {
      values.push(`%${payerName}%`);
      where.push(`px."PayerName" ILIKE $${values.length}`);
    }

    pushWhere('px."status" =', status_bank);
    pushWhere("ob.status =", status_blockchain);
    pushWhere('ob."typeIn" =', typeIn);

    if (min_amount !== undefined) {
      values.push(min_amount);
      where.push(
        `REPLACE(px."amount", ',', '.')::numeric >= $${values.length}`
      );
    }
    if (max_amount !== undefined) {
      values.push(max_amount);
      where.push(
        `REPLACE(px."amount", ',', '.')::numeric <= $${values.length}`
      );
    }

    if (created_after) {
      values.push(created_after);
      where.push(`px."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`px."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const select = `
      SELECT 
        ob.id,
        aw.id AS wallet_id,
        ob.txid,
        ob.wallet AS receive_wallet,
        aw."name" AS receive_name,
        aw."document" AS receive_doc,
        px."destinationKey",
        px.end2end,
        px."PayerName" AS payer_name,
        px."payerDocument" as payer_document,
        px."amount",
        px."status" AS status_bank,
        ob.status AS status_blockchain,
        ob."msgError" AS msg_error_blockchain,
        px."msgError" AS msg_error_bank,
        px."createdAt"::text AS "createdAt",
        ob."typeIn",
        t.symbol AS token_symbol
      FROM "orderSell" AS ob
      LEFT JOIN "pixIn" AS px ON px.id = ob."pixInId"
      LEFT JOIN "aclWallets" AS aw ON aw.wallet = ob.wallet OR aw."btcWallet" = ob.wallet OR aw."liquidWallet" = ob.wallet
      LEFT JOIN "tokens" AS t ON t.id = ob."tokensId"
    `;

    const query = `${select} ${whereClause} ORDER BY ${orderByColumn} ${safeSortOrder}`;

    const client = await this.getClientBanks();
    try {
      const result = await client.query(query, values);
      return result.rows as PixInTransaction[];
    } finally {
      client.release();
    }
  }

  async findAtmPaginated(
    params: AtmParams
  ): Promise<PaginatedResult<AtmTransaction>> {
    const {
      page,
      limit,
      sortBy = 'atm."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      txid,
      sender,
      receiverDocument,
      receiverName,
      status_bk,
      status_px,
      min_amount,
      max_amount,
    } = params;

    const offset = (page - 1) * limit;

    const allowedSortBy = new Set<string>([
      "atm.id",
      "ob.txid",
      'ob."sender"',
      'atm."receiverName"',
      'atm."receiverDocument"',
      'atm."amount"',
      'atm."status"',
      "ob.status",
      'atm."createdAt"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'atm."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    pushWhere("ob.txid =", txid);
    pushWhere('ob."sender" =', sender);
    pushWhere('atm."receiverDocument" =', receiverDocument);

    if (receiverName) {
      values.push(`%${receiverName}%`);
      where.push(`atm."receiverName" ILIKE $${values.length}`);
    }

    pushWhere('atm."status" =', status_px);
    pushWhere("ob.status =", status_bk);

    if (min_amount !== undefined) {
      values.push(min_amount);
      where.push(`atm."amount" >= $${values.length}`);
    }
    if (max_amount !== undefined) {
      values.push(max_amount);
      where.push(`atm."amount" <= $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`atm."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`atm."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

   
    const select = `
      SELECT
        atm.id,
        ob.txid,
        ob."refundTxid",
        ob."block",
        ob."sender",
        ob."receiver",
        ob."amount" AS amount_crypto,
        ob.status AS status_bk,
        atm."receiverName",
        atm."receiverDocument",
        atm."amount" AS amount_brl,
        atm."status" AS status_px,
        atm."createdAt"::text AS createdAt
      FROM "atmCashout" AS atm
      LEFT JOIN "orderBuy" AS ob ON ob."id" = atm."orderId"
    `;


    const query = `
    ${select}
    ${whereClause}
    ORDER BY ${safeSortBy} ${safeSortOrder}
    LIMIT $${values.length + 1}
    OFFSET $${values.length + 2}
  `;

    const countQuery = `
    SELECT COUNT(*)::int AS total
    FROM "atmCashout" AS atm
    LEFT JOIN "orderBuy" AS ob ON ob."id" = atm."orderId"
    ${whereClause}
  `;

    const client = await this.getClientBanks();

    try {
      const result = await client.query(query, [...values, limit, offset]);
      const count = await client.query(countQuery, values);

      return {
        data: result.rows as AtmTransaction[],
        total: Number(count.rows[0]?.total ?? 0),
        page,
        limit,
      };
    } finally {
      client.release();
    }
  }

  async findBilletCashoutAll(
    params: Omit<BilletCashoutParams, 'page' | 'limit'>
  ): Promise<BilletCashoutTransaction[]> {
    const {
      created_after,
      created_before,
      status,
      receiverName,
      receiverDocument,
      min_amount,
      max_amount,
      banksId,
      orderId,
    } = params as BilletCashoutParams;

    const client = await this.getClientBanks();

    const filters: string[] = [];
    const values: any[] = [];

    const addFilter = (condition: string, value: any) => {
      if (value !== undefined && value !== null && value !== "") {
        values.push(value);
        filters.push(condition.replace(/\$idx/g, `$${values.length}`));
      }
    };

    addFilter(
      `LOWER(t."receiverName") LIKE LOWER($idx)`,
      receiverName ? `%${receiverName}%` : undefined
    );
    addFilter(`t."receiverDocument" = $idx`, receiverDocument);
    addFilter(`UPPER(t."status"::text) = UPPER($idx)`, status);
    addFilter(`t."createdAt" >= $idx`, created_after);
    addFilter(`t."createdAt" <= $idx`, created_before);
    addFilter(`t."amount" >= $idx`, min_amount);
    addFilter(`t."amount" <= $idx`, max_amount);
    addFilter(`t."banksId" = $idx`, banksId);
    addFilter(`t."orderId" = $idx`, orderId);

    const whereClause = filters.length ? `WHERE ${filters.join(" AND ")}` : "";

    const query = `
      SELECT 
        t."id", 
        t."uuid", 
        t."identifier", 
        t."movimentCode", 
        t."transactionCode",
        t."transactionIdentifier", 
        t."aditionalInfor", 
        t."receiverName",
        t."receiverDocument", 
        t."brcode", 
        t."msgError", 
        t."tryAgain",
        t."status", 
        t."countTimer", 
        t."refundMovimentCode", 
        t."createdAt",
        t."updateAt", 
        t."banksId", 
        t."orderId", 
        t."feeSymbol", 
        t."price",
        t."fee", 
        t."amount", 
        t."typeBoleto", 
        t."module"
      FROM "billetCashout" t
      LEFT JOIN "orderBuy" ob ON t."orderId" = ob.id
      LEFT JOIN "aclWallets" aw ON ob.sender = aw.wallet
      ${whereClause}
      ORDER BY t."createdAt" DESC
    `;

    try {
      const result = await client.query(query, values);
      return result.rows as BilletCashoutTransaction[];
    } finally {
      client.release();
    }
  }

  async findBridgeAll(
    params: Omit<BridgeParams, 'page' | 'limit'>
  ): Promise<BridgeTransaction[]> {
    const {
      user_id,
      wallet_id,
      value,
      status,
      sortBy = "created_at",
      sortOrder = "DESC",
      created_after,
      created_before,
    } = params as BridgeParams;

    const client = await this.getClientTruther();

    const allowedSortBy = ["id", "created_at", "value", "status"];
    const safeSortBy = allowedSortBy.includes(sortBy) ? sortBy : "created_at";
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    const where: string[] = [];
    const values: any[] = [];

    if (user_id) {
      values.push(user_id);
      where.push(`t.user_id = $${values.length}`);
    }

    if (wallet_id) {
      values.push(wallet_id);
      where.push(`t.wallet_id = $${values.length}`);
    }

    if (value) {
      values.push(value);
      where.push(`t.value = $${values.length}`);
    }

    if (status) {
      values.push(status);
      where.push(`t.status = $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`t.created_at >= $${values.length}`);
    }

    if (created_before) {
      values.push(created_before);
      where.push(`t.created_at <= $${values.length}`);
    }

    const bridgeCondition = `
      t.id IN (
        SELECT parent_transaction_id
        FROM public.transactions
        WHERE parent_transaction_id IS NOT NULL
        UNION
        SELECT id
        FROM public.transactions
        WHERE parent_transaction_id IS NOT NULL
      )
    `;

    const whereClause =
      where.length > 0
        ? `WHERE ${bridgeCondition} AND ${where.join(" AND ")}`
        : `WHERE ${bridgeCondition}`;

    const dataQuery = `
      SELECT
        t.id,
        t.user_id,
        t.wallet_id,
        t.from_address,
        t.to_address,
        t.value,
        t.tx_hash,
        t.status,
        t.created_at,
        t.flow,
        t.type,
        t.symbol,
        t.retry_count,
        t.protocol_destination
      FROM public.transactions t
      ${whereClause}
      ORDER BY t.${safeSortBy} ${safeSortOrder}
    `;

    try {
      const result = await client.query(dataQuery, values);
      return result.rows as BridgeTransaction[];
    } finally {
      client.release();
    }
  }

  async findAtmAll(
    params: Omit<AtmParams, 'page' | 'limit'>
  ): Promise<AtmTransaction[]> {
    const {
      sortBy = 'atm."createdAt"',
      sortOrder = "DESC",
      created_after,
      created_before,
      txid,
      sender,
      receiverDocument,
      receiverName,
      status_bk,
      status_px,
      min_amount,
      max_amount,
    } = params as AtmParams;

    const allowedSortBy = new Set<string>([
      "atm.id",
      "ob.txid",
      'ob."sender"',
      'atm."receiverName"',
      'atm."receiverDocument"',
      'atm."amount"',
      'atm."status"',
      "ob.status",
      'atm."createdAt"',
    ]);

    const safeSortBy = allowedSortBy.has(sortBy) ? sortBy : 'atm."createdAt"';
    const safeSortOrder = sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    const where: string[] = [];
    const values: unknown[] = [];

    const pushWhere = (clause: string, value?: unknown) => {
      if (value === undefined || value === null || value === "") return;
      values.push(value);
      where.push(`${clause} $${values.length}`);
    };

    pushWhere("ob.txid =", txid);
    pushWhere('ob."sender" =', sender);
    pushWhere('atm."receiverDocument" =', receiverDocument);

    if (receiverName) {
      values.push(`%${receiverName}%`);
      where.push(`atm."receiverName" ILIKE $${values.length}`);
    }

    pushWhere('atm."status" =', status_px);
    pushWhere("ob.status =", status_bk);

    if (min_amount !== undefined) {
      values.push(min_amount);
      where.push(`atm."amount" >= $${values.length}`);
    }
    if (max_amount !== undefined) {
      values.push(max_amount);
      where.push(`atm."amount" <= $${values.length}`);
    }

    if (created_after) {
      values.push(created_after);
      where.push(`atm."createdAt" >= $${values.length}`);
    }
    if (created_before) {
      values.push(created_before);
      where.push(`atm."createdAt" <= $${values.length}`);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const select = `
      SELECT
        atm.id,
        ob.txid,
        ob."refundTxid",
        ob."block",
        ob."sender",
        ob."receiver",
        ob."amount" AS amount_crypto,
        ob.status AS status_bk,
        atm."receiverName",
        atm."receiverDocument",
        atm."amount" AS amount_brl,
        atm."status" AS status_px,
        atm."createdAt"::text AS createdAt
      FROM "atmCashout" AS atm
      LEFT JOIN "orderBuy" AS ob ON ob."id" = atm."orderId"
    `;

    const query = `
      ${select}
      ${whereClause}
      ORDER BY ${safeSortBy} ${safeSortOrder}
    `;

    const client = await this.getClientBanks();

    try {
      const result = await client.query(query, values);
      return result.rows as AtmTransaction[];
    } finally {
      client.release();
    }
  }

  async findAllUserTransactionsByDocument(
    document: string,
    params?: UserTransactionsParams
  ): Promise<{
    data: UserTransaction[];
    total: number;
    page: number;
    limit: number;
  }> {
    const clientBanks = await this.getClientBanks();
    const clientAdmin = await this.getClient();

    const page = params?.page && params.page > 0 ? params.page : 1;
    const limit = params?.limit && params.limit > 0 ? params.limit : 20;
    const offset = (page - 1) * limit;
    const sortBy = params?.sortBy ?? "created_at";
    const sortOrder =
      params?.sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    try {
      const { rows: walletsRows } = await clientBanks.query<{
        wallet: string;
        liquidWallet: string;
        btcWallet: string;
      }>(
        `
      SELECT wallet, "liquidWallet", "btcWallet"
      FROM public."aclWallets"
      WHERE document = $1
      `,
        [document]
      );

      if (!walletsRows.length) {
        return { data: [], total: 0, page, limit };
      }

      const baseWallets = [
        walletsRows[0].wallet,
        walletsRows[0].liquidWallet,
        walletsRows[0].btcWallet,
      ].filter(Boolean);

      const [orderBuyRows, orderSellRows, smartlinkRows] = await Promise.all([
        clientBanks.query<{ sender: string }>(
          `SELECT DISTINCT sender FROM public."orderBuy" WHERE sender = ANY($1)`,
          [baseWallets]
        ),
        clientBanks.query<{ wallet: string }>(
          `SELECT DISTINCT wallet FROM public."orderSell" WHERE wallet = ANY($1)`,
          [baseWallets]
        ),
        clientBanks.query<{ wallet: string }>(
          `
          SELECT DISTINCT sl.wallet
          FROM public.smartlink sl
          JOIN public."orderSell" os ON os.wallet = sl.wallet
          WHERE sl.wallet = ANY($1)
          `,
          [baseWallets]
        ),
      ]);

      const allWallets = new Set<string>([
        ...baseWallets,
        ...orderBuyRows.rows.map((r) => r.sender),
        ...orderSellRows.rows.map((r) => r.wallet),
        ...smartlinkRows.rows.map((r) => r.wallet),
      ]);

      const allWalletsArray = Array.from(allWallets);

      if (allWalletsArray.length === 0) {
        return { data: [], total: 0, page, limit };
      }
  const normalizedWallets = allWalletsArray.map((w) =>
        w.toLowerCase().replace("0x", "")
      );

      const filters: string[] = [];
      const values: any[] = [normalizedWallets];
      let paramIndex = 2;

      if (params?.status) {
        filters.push(`LOWER(t.status) = LOWER($${paramIndex++})`);
        values.push(params.status);
      }

      if (params?.created_after) {
        filters.push(`t.created_at >= $${paramIndex++}`);
        values.push(params.created_after);
      }

      if (params?.created_before) {
        filters.push(`t.created_at <= $${paramIndex++}`);
        values.push(params.created_before);
      }

      if (params?.value) {
        filters.push(`t.value::numeric >= $${paramIndex++}`);
        values.push(params.value);
      }

      if (params?.hash) {
        filters.push(`t.tx_hash ILIKE $${paramIndex++}`);
        values.push(`%${params.hash}%`);
      }

      const queryBase = `
      FROM public.transactions t
      WHERE 
        (
          REPLACE(LOWER(t.from_address), '0x', '') = ANY($1)
          OR 
          REPLACE(LOWER(t.to_address), '0x', '') = ANY($1)
        )
        AND LOWER(t.type) IN ('blockchain', 'bridge')
        ${filters.length ? `AND ${filters.join(" AND ")}` : ""}
    `;


     const { rows: countRows } = await clientAdmin.query<{ total: number }>(
      `SELECT COUNT(*)::int AS total ${queryBase}`,
      values
    );

    const total = countRows[0]?.total ?? 0;

    values.push(limit, offset);
      const { rows: data } = await clientAdmin.query<UserTransaction>(
        `
      SELECT 
        t.id,
        t.uuid,
        t.token_id,
        t.user_id,
        t.from_address,
        t.to_address,
        t.value,
        t.fee_value,
        t.status,
        t.type,
        t.tx_hash,
        t.symbol,
        t.flow,
        t.created_at,
        t.updated_at
      ${queryBase}
      ORDER BY t.${sortBy} ${sortOrder}
      LIMIT $${paramIndex++} OFFSET $${paramIndex}
      `,
        values
      );

      return { data, total, page, limit };
    } finally {
      clientBanks.release();
      clientAdmin.release();
    }
  }

  async findAllUserTransactionsByDocumentAll(
    document: string,
    params?: Omit<UserTransactionsParams, 'page' | 'limit'>
  ): Promise<UserTransaction[]> {
    const clientBanks = await this.getClientBanks();
    const clientAdmin = await this.getClient();

    const sortBy = params?.sortBy ?? "created_at";
    const sortOrder = params?.sortOrder?.toUpperCase() === "ASC" ? "ASC" : "DESC";

    try {
      const { rows: walletsRows } = await clientBanks.query<{
        wallet: string;
        liquidWallet: string;
        btcWallet: string;
      }>(
        `
      SELECT wallet, "liquidWallet", "btcWallet"
      FROM public."aclWallets"
      WHERE document = $1
      `,
        [document]
      );

      if (!walletsRows.length) {
        return [];
      }

      const baseWallets = [
        walletsRows[0].wallet,
        walletsRows[0].liquidWallet,
        walletsRows[0].btcWallet,
      ].filter(Boolean);

      const [orderBuyRows, orderSellRows, smartlinkRows] = await Promise.all([
        clientBanks.query<{ sender: string }>(
          `SELECT DISTINCT sender FROM public."orderBuy" WHERE sender = ANY($1)`,
          [baseWallets]
        ),
        clientBanks.query<{ wallet: string }>(
          `SELECT DISTINCT wallet FROM public."orderSell" WHERE wallet = ANY($1)`,
          [baseWallets]
        ),
        clientBanks.query<{ wallet: string }>(
          `
        SELECT DISTINCT sl.wallet
        FROM public.smartlink sl
        JOIN public."orderSell" os ON os.wallet = sl.wallet
        WHERE sl.wallet = ANY($1)
        `,
          [baseWallets]
        ),
      ]);

      const allWallets = new Set<string>([
        ...baseWallets,
        ...orderBuyRows.rows.map((r) => r.sender),
        ...orderSellRows.rows.map((r) => r.wallet),
        ...smartlinkRows.rows.map((r) => r.wallet),
      ]);

      const allWalletsArray = Array.from(allWallets);

      if (allWalletsArray.length === 0) {
        return [];
      }

      const walletPatterns = allWalletsArray.map((w) => `%${w.toLowerCase()}%`);

      const filters: string[] = [];
      const values: any[] = [walletPatterns];
      let paramIndex = 2;

      if (params?.status) {
        filters.push(`LOWER(t.status) = LOWER($${paramIndex++})`);
        values.push(params.status);
      }

      if (params?.created_after) {
        filters.push(`t.created_at >= $${paramIndex++}`);
        values.push(params.created_after);
      }

      if (params?.created_before) {
        filters.push(`t.created_at <= $${paramIndex++}`);
        values.push(params.created_before);
      }

      if (params?.value) {
        filters.push(`t.value::numeric >= $${paramIndex++}`);
        values.push(params.value);
      }

      if (params?.hash) {
        filters.push(`t.tx_hash ILIKE $${paramIndex++}`);
        values.push(`%${params.hash}%`);
      }

      const queryBase = `
      FROM public.transactions t
      JOIN UNNEST($1::text[]) AS p(pattern)
        ON REPLACE(LOWER(t.from_address), '0x', '') ILIKE REPLACE(p.pattern, '0x', '')
        OR REPLACE(LOWER(t.to_address), '0x', '') ILIKE REPLACE(p.pattern, '0x', '')
      WHERE LOWER(t.type) IN ('blockchain', 'bridge')
      ${filters.length ? `AND ${filters.join(" AND ")}` : ""}
    `;

      const { rows: data } = await clientAdmin.query<UserTransaction>(
        `
      SELECT 
        t.id,
        t.uuid,
        t.token_id,
        t.user_id,
        t.from_address,
        t.to_address,
        t.value,
        t.fee_value,
        t.status,
        t.type,
        t.tx_hash,
        t.symbol,
        t.flow,
        t.created_at,
        t.updated_at
      ${queryBase}
      ORDER BY t.${sortBy} ${sortOrder}
      `,
        values
      );

      return data;
    } finally {
      clientBanks.release();
      clientAdmin.release();
    }
  }
}
