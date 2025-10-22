import { inArray, desc, sql } from "drizzle-orm";
import { dexTrade } from "./db/schema/Listener";
import { types, db, App, middlewares } from "@duneanalytics/sim-idx"; // Import schema to ensure it's registered

const supportedChains: types.Uint[] = [
  1, 8453, 480, 34443, 57073, 130, 7777777, 60808, 1868, 360, 42161,
].map((id) => new types.Uint(BigInt(id)));

const app = App.create();
app.use("*", middlewares.authentication);

app.get("/lasttrades", async (c) => {
  try {
    // Get the time interval parameter, default to 5 minutes
    const intervalParam = c.req.query("interval");
    const intervalMinutes = intervalParam ? parseInt(intervalParam, 10) : 5;
    
    // Validate the interval parameter
    if (isNaN(intervalMinutes) || intervalMinutes <= 0) {
      return Response.json({ error: "Invalid interval parameter. Must be a positive number of minutes." }, { status: 400 });
    }

    const result = await db.client(c).execute(sql.raw(`
      WITH norm AS (
        SELECT
          dex,
          LEAST(from_token_symbol, to_token_symbol)  AS tok1,
          GREATEST(from_token_symbol, to_token_symbol) AS tok2,
          -- Map amounts to the sorted side:
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN from_token_amt / POWER(10, from_token_decimals)
            ELSE to_token_amt / POWER(10, to_token_decimals)
          END AS amt1,
          CASE
            WHEN from_token_symbol <= to_token_symbol THEN to_token_amt / POWER(10, to_token_decimals)
            ELSE from_token_amt / POWER(10, from_token_decimals)
          END AS amt2
        FROM dex_trade
        WHERE block_timestamp > EXTRACT(EPOCH FROM (NOW() - INTERVAL '${intervalMinutes} minutes'))
      )
      SELECT
        tok1 || '/' || tok2 AS token_pair,
        COUNT(*) AS trade_count,
        SUM(amt1) AS total_tok1_amt,
        SUM(amt2) AS total_tok2_amt 
      FROM norm
      GROUP BY tok1, tok2
      ORDER BY trade_count DESC LIMIT 20
    `));

    return Response.json({
      result: result.rows,
    });
  } catch (e) {
    console.error("Database operation failed:", e);
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
});

app.get("/*", async (c) => {
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



export default app;
