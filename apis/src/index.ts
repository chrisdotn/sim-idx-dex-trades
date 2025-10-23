import { inArray, desc, sql } from "drizzle-orm";
import { dexTrade } from "./db/schema/Listener";
import { types, db, App, middlewares } from "@duneanalytics/sim-idx"; // Import schema to ensure it's registered

const supportedChains: types.Uint[] = [
  1, 8453, 480, 34443, 57073, 130, 7777777, 60808, 1868, 360, 42161,
].map((id) => new types.Uint(BigInt(id)));

const app = App.create();
app.use("*", middlewares.authentication);

// Default route for backward compatibility
app.get("/", async (c) => {
  try {
    const chainIdsParam = c.req.query("chainIds");
    let chainIds: types.Uint[];
    if (!chainIdsParam) {
      chainIds = supportedChains;
    } else {
      chainIds = chainIdsParam
        .split(",")
        .map((id) => new types.Uint(BigInt(parseInt(id, 10))));
    }

    const result = await db
      .client(c)
      .select()
      .from(dexTrade)
      .where(inArray(dexTrade.chainId, chainIds))
      .orderBy(desc(dexTrade.blockTimestamp))
      .limit(10);

    return Response.json({
      result: result,
    });
  } catch (e) {
    console.error("Database operation failed:", e);
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
});

// New /lasttrades endpoint
app.get("/lasttrades", async (c) => {
  try {
    // Get minTradeCount from query parameter, default to 100
    // const minTradeCountParam = c.req.query("minTradeCount");
    // const minTradeCount = minTradeCountParam ? parseInt(minTradeCountParam, 10) : 100;
    
    // Get chainIds from query parameter, default to Base (8453)
    // const chainIdsParam = c.req.query("chainIds");
    // let chainIds: types.Uint[];
    // if (!chainIdsParam) {
    //   chainIds = [new types.Uint(BigInt(8453))];
    // } else {
    //   chainIds = chainIdsParam
    //     .split(",")
    //     .map((id) => new types.Uint(BigInt(parseInt(id, 10))));
    // }
    
    // Validate the parameters
    // if (isNaN(minTradeCount) || minTradeCount < 0) {
    //   return Response.json({ error: "minTradeCount must be a non-negative number" }, { status: 400 });
    // }
    
    // if (chainIds.some(id => isNaN(Number(id)))) {
    //   return Response.json({ error: "chainIds must be comma-separated valid numbers" }, { status: 400 });
    // }

    const result = await db.client(c).execute(sql`
      WITH norm AS (
        SELECT
          dex,
          LEAST(from_token_symbol, to_token_symbol)  AS tok1,
          GREATEST(from_token_symbol, to_token_symbol) AS tok2,
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN from_token
            ELSE to_token
          END AS token1,
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN to_token
            ELSE from_token
          END AS token2,
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN from_token_amt / POWER(10, from_token_decimals)
            ELSE to_token_amt / POWER(10, to_token_decimals)
          END AS amt1,
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN to_token_amt / POWER(10, to_token_decimals)
            ELSE from_token_amt / POWER(10, from_token_decimals)
          END AS amt2
        FROM dex_trade
        WHERE block_timestamp > EXTRACT(EPOCH FROM (NOW() - INTERVAL '5 minutes'))
          and chain_id = 8453
      )
      SELECT
        tok1 || '/' || tok2 AS token_pair,
        token1,
        token2,
        COUNT(*) AS trade_count,
        SUM(amt1) AS total_tok1_amt,
        SUM(amt2) AS total_tok2_amt 
      FROM norm
      GROUP BY tok1, tok2, token1, token2
      HAVING COUNT(*) > 100
      ORDER BY trade_count DESC
    `);

    const result = await db.client(c).execute(sql`
      // SELECT * FROM dex_trade WHERE chain_id = 8453 AND block_timestamp > EXTRACT(EPOCH FROM (NOW() - INTERVAL '5 minutes')) LIMIT 100;
    `);

    return Response.json({
      result: result.rows,
    });
  } catch (e) {
    console.error("Database operation failed:", e);
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
});

export default app;
