import os
from datetime import datetime, timedelta
from time import time

import shopify
from dotenv import load_dotenv
from flask import Flask, request

load_dotenv()

shops = ["ES", "FR", "IT", "NL"]

# "Cash on Delivery" (COD)
cod = ["ES", "IT"]


def get_unfulfilled_products_by_country(start_date=None):
    created_at_min = ""
    if start_date:
        # Format that Shopify expects
        created_at_min = start_date.isoformat()

    sku_by_country_counts = {}

    for shop in shops:
        API_KEY = os.getenv(f"API_KEY_{shop}")
        PASSWORD = os.getenv(f"PASSWORD_{shop}")
        SHOP_NAME = os.getenv(f"SHOP_{shop}")
        shop_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin"

        shopify.ShopifyResource.set_site(shop_url)

        try:
            paid_orders = shopify.Order.find(
                financial_status="paid",
                fulfillment_status="unfulfilled",
                created_at_min=created_at_min,
            )

            if shop in cod:
                pending_orders = shopify.Order.find(
                    financial_status="pending",
                    fulfillment_status="unfulfilled",
                    created_at_min=created_at_min,
                )
                cod_orders = [
                    order for order in pending_orders if "COD" in order.tags.split(", ")
                ]
                orders = paid_orders + cod_orders
            else:
                orders = paid_orders

            sku_counts = {}
            for order in orders:
                for line_item in order.line_items:
                    if line_item.sku and line_item.sku.startswith("DIVAIN"):
                        sku_counts[line_item.sku] = (
                            sku_counts.get(line_item.sku, 0) + line_item.quantity
                        )

        except Exception as e:
            sku_counts = {"error": e}

        sku_by_country_counts[shop] = sku_counts

    return sku_by_country_counts


def get_unfulfilled_products(start_date=None):
    sku_by_country_counts = get_unfulfilled_products_by_country(start_date)
    # separate the data into two lists erros and the sum of the grouped by skus
    errors = [
        sku_by_country_counts[shop]["error"]
        for shop in shops
        if "error" in sku_by_country_counts[shop]
    ]
    sku_counts = [
        sku_by_country_counts[shop]
        for shop in shops
        if "error" not in sku_by_country_counts[shop]
    ]
    # sum the grouped by skus
    sku_sum = {}
    for sku_count in sku_counts:
        for sku in sku_count:
            if sku in sku_sum:
                sku_sum[sku] += sku_count[sku]
            else:
                sku_sum[sku] = sku_count[sku]
    return errors, sku_sum


def get_data(days_before):
    start = time()
    today = datetime.now()

    start_date = None
    if days_before:
        before = timedelta(days=days_before)
        start_date = today - before

    errors, sku_sum = get_unfulfilled_products(start_date)
    end = time()

    output = {
        "total_unfulfilled_orders": len(sku_sum),
        "products": [
            {"sku": sku, "quantity": quantity} for sku, quantity in sku_sum.items()
        ],
        "errors": errors,
        "time_elapsed": f"{end - start} seconds",
        "start_date": start_date.strftime("%d-%m-%Y %H:%M:%S") if start_date else "",
        "end_date": today.strftime("%d-%m-%Y %H:%M:%S"),
    }

    return output


# Path: app.py
app = Flask(__name__)


@app.errorhandler(500)
def handle_500(e):
    return {"error": str(e)}, 500


@app.route("/shopify/unfulfilled-orders/sku", methods=["GET"])
def sendcloud_unfulfilled_orders_sku():
    # Get the data

    days_before = request.args.get("days_before")
    if days_before:
        days_before = int(days_before)

    data = get_data(days_before)

    # Return the data as JSON
    return data


if __name__ == "__main__":
    app.run(debug=True, port=5500)
