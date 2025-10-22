import { inArray, desc } from "drizzle-orm";
import { dexTrade } from "./db/schema/Listener";
import { types, db, App, middlewares } from "@duneanalytics/sim-idx"; // Import schema to ensure it's registered

const supportedChains: types.Uint[] = [
  1, 8453, 480, 34443, 57073, 130, 7777777, 60808, 1868, 360, 42161,
].map((id) => new types.Uint(BigInt(id)));

const app = App.create();
app.use("*", middlewares.authentication);

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
