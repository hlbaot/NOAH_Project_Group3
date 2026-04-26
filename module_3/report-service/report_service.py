import logging
import os
import time
from collections import OrderedDict

import httpx
import pandas as pd
from db import mysql_engine, postgres_engine

logger = logging.getLogger("report-service.report")
logging.getLogger("httpx").setLevel(logging.WARNING)


def get_ai_insight(summary: dict, revenue_by_customer: list, top_products: list) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "Chưa cấu hình GEMINI_API_KEY nên hệ thống chỉ hiển thị báo cáo dữ liệu, chưa tạo nhận định AI."

    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    top_customers = revenue_by_customer[:5]
    top_products = top_products[:5]
    prompt = f"""
Bạn là trợ lý phân tích dữ liệu bán lẻ cho dashboard NOAH.
Hãy viết 3 gạch đầu dòng ngắn bằng tiếng Việt, dễ hiểu cho quản lý.

Dữ liệu tổng quan:
- Tổng đơn hàng: {summary.get("total_orders", 0)}
- Đã thanh toán: {summary.get("paid_orders", 0)}
- Cần đối soát: {summary.get("pending_orders", 0)}
- Thất bại: {summary.get("failed_orders", 0)}
- Hoàn tiền: {summary.get("refunded_orders", 0)}
- Doanh thu thực: {summary.get("total_revenue", 0)}

Top khách hàng theo doanh thu:
{top_customers}

Top sản phẩm theo doanh thu đơn hàng:
{top_products}

Tập trung vào: doanh thu, sản phẩm nổi bật, rủi ro đối soát thanh toán, và hành động nên làm tiếp theo.
""".strip()

    try:
        with httpx.Client(timeout=15) as client:
            res = client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                params={"key": api_key},
                json={"contents": [{"parts": [{"text": prompt}]}]},
            )
            res.raise_for_status()
            payload = res.json()
            return payload["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as exc:
        logger.warning("Gemini insight generation failed: %s", exc)
        return "Chưa tạo được nhận định AI từ Gemini. Báo cáo dữ liệu vẫn đang hoạt động bình thường."


def get_report(page: int = 1, page_size: int = 20, include_ai: bool = False):
    started_at = time.perf_counter()
    offset = (page - 1) * page_size
    logger.info(
        "Generating stitched report | page=%s | page_size=%s | offset=%s",
        page,
        page_size,
        offset,
    )

    # MySQL: orders + products
    orders_query = """
    SELECT
        o.id AS order_id,
        o.user_id,
        o.product_id,
        o.quantity,
        o.total_price,
        o.status AS order_status,
        o.created_at,
        p.name AS product_name,
        p.price AS product_price
    FROM orders o
    LEFT JOIN products p ON o.product_id = p.id
    ORDER BY o.id DESC;
    """

    # PostgreSQL: payments + customers
    payments_query = """
    SELECT
        p.id AS payment_id,
        p.order_id,
        p.customer_id,
        c.name AS customer_name,
        p.amount,
        p.status AS payment_status,
        p.paid_at
    FROM payments p
    LEFT JOIN customers c ON p.customer_id = c.id
    ORDER BY p.id DESC;
    """

    orders_df = pd.read_sql(orders_query, mysql_engine)
    logger.info("Fetched MySQL dataset | rows=%s | columns=%s", len(orders_df), len(orders_df.columns))

    payments_df = pd.read_sql(payments_query, postgres_engine)
    logger.info("Fetched PostgreSQL dataset | rows=%s | columns=%s", len(payments_df), len(payments_df.columns))

    # Data stitching
    merged_df = pd.merge(
        orders_df,
        payments_df,
        on="order_id",
        how="left"
    )
    logger.info("Data stitching complete | rows=%s | cols=%s", len(merged_df), len(merged_df.columns))

    # Xử lý null
    if "amount" in merged_df.columns:
        merged_df["amount"] = merged_df["amount"].fillna(0)
    else:
        merged_df["amount"] = 0

    for col, default in [("customer_name", "Unknown"), ("payment_status", "UNPAID"), ("order_status", "PENDING")]:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(default)
        else:
            merged_df[col] = default

    merged_df["payment_status"] = (
        merged_df["payment_status"]
        .astype(str)
        .str.upper()
        .replace({"NAN": "UNPAID", "NONE": "UNPAID"})
    )

    # Fix datetime serialize
    if "created_at" in merged_df.columns:
        merged_df["created_at"] = merged_df["created_at"].astype(str)

    merged_df["_has_payment"] = (merged_df["payment_status"] == "PAID").astype(int)
    merged_df = merged_df.sort_values(by=["_has_payment", "order_id"], ascending=[False, False])
    paginated_df = merged_df.iloc[offset:offset + page_size]

    # KPI
    total_orders = int(len(merged_df))
    raw_status_counts = merged_df["payment_status"].value_counts().to_dict()
    paid_orders = int(raw_status_counts.get("PAID", 0))
    pending_orders = int(raw_status_counts.get("PENDING", 0) + raw_status_counts.get("UNPAID", 0))
    failed_orders = int(raw_status_counts.get("FAILED", 0))
    refunded_orders = int(raw_status_counts.get("REFUNDED", 0))
    paid_df = merged_df[merged_df["payment_status"] == "PAID"]
    total_revenue = float(paid_df["amount"].sum())

    # payment status counts
    preferred_status_order = ["PAID", "PENDING", "UNPAID", "FAILED", "REFUNDED"]
    payment_status_counts = OrderedDict()
    for status in preferred_status_order:
        if raw_status_counts.get(status, 0):
            payment_status_counts[status] = int(raw_status_counts[status])
    for status, count in raw_status_counts.items():
        if status not in payment_status_counts:
            payment_status_counts[status] = int(count)

    # order status counts
    raw_order_status_counts = merged_df["order_status"].value_counts().to_dict()
    order_status_counts = OrderedDict()
    for status, count in raw_order_status_counts.items():
        order_status_counts[str(status).upper()] = int(count)

    # Revenue by customer
    if paid_df.empty:
        revenue_by_customer_df = pd.DataFrame(columns=["customer_name", "total_revenue", "order_count"])
    else:
        revenue_by_customer_df = (
            paid_df.groupby("customer_name", dropna=False)
            .agg(
                total_revenue=("amount", "sum"),
                order_count=("order_id", "nunique"),
            )
            .reset_index()
            .sort_values(by="total_revenue", ascending=False)
        )

    # Top products from MySQL order detail
    top_products_df = (
        merged_df.groupby("product_name", dropna=False)
        .agg(
            total_order_value=("total_price", "sum"),
            total_quantity=("quantity", "sum"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
        .sort_values(by="total_order_value", ascending=False)
    )

    # Order detail table
    orders_result = paginated_df[
        [
            "order_id",
            "product_name",
            "quantity",
            "order_status",
            "customer_name",
            "payment_status",
            "amount",
            "created_at",
        ]
    ].fillna("").to_dict(orient="records")

    revenue_by_customer_result = revenue_by_customer_df.head(8).fillna("").to_dict(orient="records")
    top_products_result = top_products_df.head(8).fillna("").to_dict(orient="records")
    summary_result = {
        "total_orders": total_orders,
        "paid_orders": paid_orders,
        "pending_orders": pending_orders,
        "failed_orders": failed_orders,
        "refunded_orders": refunded_orders,
        "payment_status_counts": payment_status_counts,
        "order_status_counts": order_status_counts,
        "total_revenue": total_revenue
    }
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    logger.info(
        "Report ready | page=%s | returned_rows=%s | total_orders=%s | paid_orders=%s | duration_ms=%s",
        page,
        len(orders_result),
        total_orders,
        paid_orders,
        duration_ms,
    )

    result = {
        "success": True,
        "summary": summary_result,
        "revenue_by_customer": revenue_by_customer_result,
        "top_products": top_products_result,
        "orders": orders_result,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_orders
        }
    }

    if include_ai:
        result["ai_insight"] = get_ai_insight(summary_result, revenue_by_customer_result, top_products_result)

    return result
