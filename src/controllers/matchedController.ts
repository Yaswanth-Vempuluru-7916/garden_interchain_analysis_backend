import { Request, Response, RequestHandler } from "express";
import { analysisPool } from "../config/db";
import { CreateOrder, MatchedOrder, PaginatedData, Swap } from "@gardenfi/orderbook";

export const getPaginatedMatchedOrders: RequestHandler = async (req: Request, res: Response) => {
  const page = parseInt(req.query.page as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 10;
  const offset = (page - 1) * perPage;

  // Input validation
  if (page < 1 || perPage < 1) {
    res.status(400).json({ error: "Page and per_page must be positive integers" });
    return;
  }

  try {
    const dataQuery = `
      SELECT  
        mo.created_at,
        mo.updated_at,
        mo.deleted_at,
        row_to_json(ss.*) as source_swap,
        row_to_json(ds.*) as destination_swap,
        row_to_json(co.*) as create_order
      FROM matched_orders mo
      JOIN create_orders co ON mo.create_order_id = co.create_id
      JOIN swaps ss ON mo.source_swap_id = ss.swap_id
      JOIN swaps ds ON mo.destination_swap_id = ds.swap_id
      ORDER BY mo.created_at DESC
      LIMIT $1 OFFSET $2
    `;

    const dataResult = await analysisPool.query(dataQuery, [perPage, offset]);

    const countResult = await analysisPool.query("SELECT COUNT(*) FROM matched_orders");
    const totalItems = parseInt(countResult.rows[0].count);
    const totalPages = Math.ceil(totalItems / perPage);

    const orders: MatchedOrder[] = dataResult.rows.map((row: any) => ({
      created_at: row.created_at,
      updated_at: row.updated_at,
      deleted_at: row.deleted_at,
      source_swap: row.source_swap as Swap,
      destination_swap: row.destination_swap as Swap,
      create_order: row.create_order as CreateOrder,
    }));

    const result: PaginatedData<MatchedOrder> = {
      data: orders,
      page,
      total_pages: totalPages,
      total_items: totalItems,
      per_page: perPage,
    };

    if (!result.data || result.data.length === 0) {
      res.status(404).json({ message: "No matched orders found" });
      return;
    }

    res.status(200).json(result);
  } catch (err: any) {
    console.error("Error fetching matched orders:", err.message);
    res.status(500).json({ error: "Failed to fetch matched orders" });
  }
};