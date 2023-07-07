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

def iter_all_orders(orders_params):
    """
    This function iterates over all the orders in the shopify store.
    The shopify API only returns 250 orders per request, so we need to iterate over all the pages.
    """
    
    orders = shopify.Order.find(**orders_params)
    for order in orders:
        yield order
    
    while orders.has_next_page():
        orders = orders.next_page()
        for order in orders:
            yield order
        

def get_unfulfilled_products_by_country(start_date=None, end_date=None):
    created_at_min = ""
    created_at_max = ""
  
    if start_date:
        # Format that Shopify expects
        created_at_min = start_date.isoformat()
    
    if end_date:
        # The Shopify API treats upper limit as exclusive, 
        # so we add one day to make it inclusive.
        end_date = end_date + timedelta(days=1)
        created_at_max = end_date.isoformat()

    sku_by_country_counts = {}

    for shop in shops:
        API_KEY = os.getenv(f"API_KEY_{shop}")
        PASSWORD = os.getenv(f"PASSWORD_{shop}")
        SHOP_NAME = os.getenv(f"SHOP_{shop}")
        shop_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin"

        shopify.ShopifyResource.set_site(shop_url)

        # Get orders between created_at_min and created_at_max
        orders_params= {
            "financial_status":"paid",
            "fulfillment_status":"unfulfilled",
            "created_at_min":created_at_min,
            "created_at_max":created_at_max,
            "limit":250
        }

        try:

            # The common_params will be used for querying both paid and pending orders.
            # paid_orders = shopify.Order.find(**orders_params)
            
            paid_orders = list(iter_all_orders(orders_params=orders_params))
            
            paid_orders = [order for order in paid_orders if order.cancelled_at is None]

            if shop in cod:
                # Modify the financial_status in params for querying COD orders
                orders_params['financial_status']='pending'
                # pending_orders = shopify.Order.find(**orders_params)
                pending_orders = list(iter_all_orders(orders_params=orders_params))

                pending_orders = [
                    order for order in pending_orders if order.financial_status != "voided"
                ]

                pending_orders = [
                    order for order in pending_orders if order.cancelled_at is None
                ]

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


def get_unfulfilled_products(start_date=None, end_date=None):
    sku_by_country_counts = get_unfulfilled_products_by_country(start_date, end_date)
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


def get_data(start_date=None, end_date=None):
    start = time()
    
    today = datetime.now()

    # if end_date is today add the current time to the end_date if not add 23:59:59
    if end_date and end_date.date() == today.date():
        end_date = datetime.now()
    elif end_date:
        end_date = end_date + timedelta(hours=23, minutes=59, seconds=59)

    end_date = end_date or today

    errors, sku_sum = get_unfulfilled_products(start_date=start_date, end_date=end_date)
    end = time()

    output = {
        # "total_unfulfilled_skus": len(sku_sum),
        "products": [
            {"sku": sku, "quantity": quantity} for sku, quantity in sku_sum.items()
        ],
        "errors": errors,
        "time_elapsed": f"{end - start} seconds",
        "start_date": start_date.strftime("%d-%m-%Y %H:%M:%S") if start_date else "",
        "end_date": end_date.strftime("%d-%m-%Y %H:%M:%S"),
    }

    return output


# Path: app.py
app = Flask(__name__)


@app.errorhandler(500)
def handle_500(e):
    return {"error": str(e)}, 500


@app.route("/shopify/unfulfilled/sku", methods=["GET"])
def shopify_unfilfilled_sku():
    
    # Get the data and try to convert the start_date and end_date to datetime objects
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    try:
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

    except ValueError as e:
        return {"error": str(e)}, 400


    data = get_data(start_date=start_date, end_date=end_date)

    # Return the data as JSON
    return data


if __name__ == "__main__":
    app.run(debug=True, port=5666)
