import logging
import multiprocessing
import os
from datetime import datetime, timedelta
from time import time

from dotenv import load_dotenv
from flask import Flask, request

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('app.log')
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.WARNING)

# create formatter
formatter = logging.Formatter(
    "[%(asctime)s.%(msecs)d] %(levelname)s \t[%(name)s.%(module)s.%(funcName)s:%(lineno)d] \t%(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# add formatter to handler
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

# add handler to logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


load_dotenv()

shops = ["ES", "FR", "IT", "NL"]


def format_dates(start_date=None, end_date=None):
    created_at_min = ""
    created_at_max = ""

    if start_date:
        created_at_min = start_date.isoformat()
    if end_date:
        created_at_max = end_date.isoformat()

    return created_at_min, created_at_max


def filter_orders(orders, avoid_status, status_type):
    return [
        order for order in orders if getattr(order, status_type) not in avoid_status
    ]


def get_unfulfilled_products_by_country(start_date=None, end_date=None):
    created_at_min, created_at_max = format_dates(start_date, end_date)

    orders_params = {
        "created_at_min": created_at_min,
        "created_at_max": created_at_max,
        # "cancelled_at" : None, <-- i don't shure if this works
        "fulfillment_status": "unfulfilled",
        "limit": 250,
    }

    sku_by_country_counts = {}

    with multiprocessing.Pool() as pool:
        sku_by_country_counts = dict(
            pool.starmap(process_shop, [(orders_params, shop) for shop in shops])
        )

    logger.info(f"Data retrieved for {len(sku_by_country_counts)} shops")
    return sku_by_country_counts


def process_shop(orders_params, shop):
    import shopify

    logger.info(f"Getting data for {shop}")
    API_KEY = os.getenv(f"API_KEY_{shop}")
    PASSWORD = os.getenv(f"PASSWORD_{shop}")
    SHOP_NAME = os.getenv(f"SHOP_{shop}")
    shop_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin"

    shopify.ShopifyResource.set_site(shop_url)

    try:

        def iter_all_orders(orders_params):
            orders = shopify.Order.find(**orders_params)
            for order in orders:
                yield order

            while orders.has_next_page():
                orders = orders.next_page()
                for order in orders:
                    yield order

        orders = list(iter_all_orders(orders_params=orders_params))

        orders = [order for order in orders if not order.cancelled_at]
        
        # avoid_fullfilled_status = ["fulfilled", "partial", "restocked"]
        avoid_financial_status = ["voided", "refunded", "partially_refunded"]

        # orders = filter_orders(orders, avoid_fullfilled_status, "fulfillment_status")
        
        orders = filter_orders(orders, avoid_financial_status, "financial_status")

        sku_counts = {}
        for order in orders:
            for line_item in order.line_items:
                if line_item.sku and line_item.sku.startswith("DIVAIN"):
                    sku_counts[line_item.sku] = (
                        sku_counts.get(line_item.sku, 0) + line_item.quantity
                    )

        shopify.ShopifyResource.clear_session()
        return (shop, sku_counts)

    except Exception as e:
        return (shop, {"error": e})


def adjust_end_date(end_date):
    today = datetime.now()
    if end_date and end_date.date() == today.date():
        end_date = datetime.now()
    elif end_date:
        end_date = end_date + timedelta(hours=23, minutes=59, seconds=59)

    return end_date or today


def get_unfulfilled_products(start_date=None, end_date=None):
    sku_by_country_counts = get_unfulfilled_products_by_country(start_date, end_date)

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

    end_date = adjust_end_date(end_date)

    errors, sku_sum = get_unfulfilled_products(start_date=start_date, end_date=end_date)
    end = time()

    output = {
        "products": [
            {"sku": sku, "quantity": quantity} for sku, quantity in sku_sum.items()
        ],
        "errors": errors,
        "time_elapsed": f"{end - start} seconds",
        "start_date": start_date.strftime("%d-%m-%Y %H:%M:%S") if start_date else "",
        "end_date": end_date.strftime("%d-%m-%Y %H:%M:%S"),
    }
    logger.info(F"Data retruieved: from {output['start_date']} to {output['end_date']} taken {output['time_elapsed']}")
    return output


# Path: app.py
app = Flask(__name__)


@app.errorhandler(500)
def handle_500(e):
    logger.error(f"Error: {e}")
    return {"error": str(e)}, 500


@app.route("/shopify/unfulfilled/sku", methods=["GET"])
def shopify_unfilfilled_sku():
    # Get the data and try to convert the start_date and end_date to datetime objects
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    
    logger.info(f"Getting data from {start_date} to {end_date}")

    try:
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")


    except ValueError as e:
        logger.error(f"Error parsing dates: {e}")
        return {f"error: Error parsing dates: {e}"}, 400

    data = get_data(start_date=start_date, end_date=end_date)

    # Return the data as JSON
    return data


if __name__ == "__main__":
    # app.run(debug=True, port=5666)

    from tornado.httpserver import HTTPServer
    from tornado.ioloop import IOLoop
    from tornado.wsgi import WSGIContainer

    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(5666)
    http_server.start(3)
    logger.info("Server started")
    IOLoop.current().start()
