import logging
import multiprocessing
import os
from datetime import datetime, timedelta
import re
from time import time

from dotenv import load_dotenv
from flask import Flask, request

# Create logger
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

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
log.addHandler(c_handler)
log.addHandler(f_handler)


load_dotenv()

# shops = ["ES"]
# shops = ["ES", "FR", "IT", "NL"]
# shops = ["ES", "FR", "IT", "NL", "DE", "EU", "PT",  "UK"]
#  pt is now in EU
# shops = ["ES", "FR", "IT", "NL", "DE", "EU",  "UK"]
shops = ["DE", "EU", "PT",  "UK"]
# shops = ["EU"]


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

def extract_sku(string):
    match = re.search(r'(DIVAIN-\d+|HOME-\d+)', string)
    if match:
        return match.group(0)
    return None


def _get_order_skus(orders):
    order_skus = []
    for order in orders:
        order_data = {
            "name": order.name,
            "processed_at": order.processed_at,
        }
        for line_item in order.line_items:
            order_line_item = order_data.copy()
            sku = line_item.sku or extract_sku(line_item.title)
            if sku and (sku.startswith("DIVAIN") or sku.startswith("HOME")):
                order_line_item["sku"] = sku
                order_line_item["quantity"] = line_item.quantity
                order_skus.append(order_line_item)
            elif not sku:
                log.warning(f"Order {order.name} has no sku")

    return order_skus

def _get_orders_and_line_items(orders):
    orders_and_line_items = []
    for order in orders:
        order_data = {
            "name": order.name,
            "country": order.shipping_address.country,
            "created_at": datetime.fromisoformat(order.created_at),
            "line_items": [],
        }
        for line_item in order.line_items:
            order_line_item = order_data.copy()
            sku = line_item.sku or extract_sku(line_item.title)
            if sku and sku != "DIVAIN-CAT" and (sku.startswith("DIVAIN") or sku.startswith("HOME")):
                order_line_item["line_items"].append({
                    "id": line_item.id,
                    "sku": sku,
                    "quantity": line_item.quantity,
                })
                orders_and_line_items.append(order_line_item)
            elif not sku:
                log.warning(f"Order {order.name} has no sku")

    return orders_and_line_items


def _get_sku_counts(orders):
    sku_counts = {}
    for order in orders:
        for line_item in order.line_items:
            sku = line_item.sku or extract_sku(line_item.title)
            if sku and (sku.startswith("DIVAIN") or sku.startswith("HOME")):
                sku_counts[sku] = (
                    sku_counts.get(sku, 0) + line_item.quantity
                )
            elif not sku:
                log.warning(f"Order {order.name} has no sku")
    return sku_counts
    

    
def process_shop(orders_params, shop, proccess_orders_func):
    import shopify

    log.info(f"Getting data for {shop}")
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

        # TODO: upgrede this if shop is Fr remove order name start with "#FI" and
        # if shop == "FR":
        #     orders = [order for order in orders if not order.name.startswith("#FI")]
        orders = [order for order in orders if not order.name.startswith("#FI")]

        orders = [order for order in orders if not order.cancelled_at]

        # orders = [order for order in orders if order.status == "open"]
        
        # print(len(orders))

        avoid_fullfilled_status = ["fulfilled", "partial", "restocked"]
        orders = filter_orders(orders, avoid_fullfilled_status, "fulfillment_status")
        
        avoid_financial_status = ["voided", "refunded", "partially_refunded"]
        orders = filter_orders(orders, avoid_financial_status, "financial_status")

        # for order in orders:
        #     print(order.name, order.fulfillment_status, order.financial_status)

        # print(len(orders))

        result = proccess_orders_func(orders)

        shopify.ShopifyResource.clear_session()
        return (shop, result)

    except Exception as e:
        error_message = f"Error getting data for {shop}: {e}"
        return (shop, {"error": error_message})



def adjust_end_date(end_date):
    today = datetime.now()
    if end_date and end_date.date() == today.date():
        end_date = datetime.now()
    elif end_date:
        end_date = end_date + timedelta(hours=23, minutes=59, seconds=59)

    return end_date or today

def get_unfulfilled_products_by_country(start_date=None, end_date=None, processing_function=None):
    created_at_min, created_at_max = format_dates(start_date, end_date)

    orders_params = {
        "created_at_min": created_at_min,
        "created_at_max": created_at_max,
        "status": "open",
        # "fulfillment_status": "unfulfilled", # TODO: check if works
        "limit": 250,
    }

    sku_by_country_counts = {}

    with multiprocessing.Pool() as pool:
        sku_by_country_counts = dict(
            pool.starmap(process_shop, [(orders_params, shop, processing_function) for shop in shops])
        )

    log.info(f"Data retrieved for {len(sku_by_country_counts)} shops")
    return sku_by_country_counts

def get_unfulfilled_products(start_date=None, end_date=None):
    sku_by_country_counts = get_unfulfilled_products_by_country(start_date, end_date, _get_sku_counts)

    errors = {}
    for shop in shops:
        if "error" in sku_by_country_counts[shop]:
            errors[shop] = sku_by_country_counts[shop]["error"]

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

def get_unfulfilled_products2(start_date=None, end_date=None):
    sku_by_country_counts = get_unfulfilled_products_by_country(start_date, end_date, _get_order_skus)

    errors = [
        sku_by_country_counts[shop]["error"]
        for shop in shops
        if "error" in sku_by_country_counts[shop]
    ]
    
    # one flat list if "error" not in shop
    skus_by_order = []
    for shop in shops:
        if "error" not in sku_by_country_counts[shop]:
            skus_by_order.extend(sku_by_country_counts[shop])


    return errors, skus_by_order

def get_unfulfilled_orders_and_line_items(start_date=None, end_date=None):
    orders_and_line_items = get_unfulfilled_products_by_country(start_date, end_date, _get_orders_and_line_items)

    errors = [
        orders_and_line_items[shop]["error"]
        for shop in shops
        if "error" in orders_and_line_items[shop]
    ]

    orders_and_line_items = [
        orders_and_line_items[shop]
        for shop in shops
        if "error" not in orders_and_line_items[shop]
    ]

    # Flatten the list
    orders_and_line_items = [item for sublist in orders_and_line_items for item in sublist]

    return errors, orders_and_line_items



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
        "shops": shops,
    }
    log.info(F"Data retrieved: from {output['start_date']} to {output['end_date']} taken {output['time_elapsed']}")
    return output


def get_data2(start_date=None, end_date=None):
    start = time()

    end_date = adjust_end_date(end_date)

    errors, skus_by_order = get_unfulfilled_products2(start_date=start_date, end_date=end_date)
    end = time()

    output = {
        "skus_by_order": skus_by_order,
        "errors": errors,
        "time_elapsed": f"{end - start} seconds",
        "start_date": start_date.strftime("%d-%m-%Y %H:%M:%S") if start_date else "",
        "end_date": end_date.strftime("%d-%m-%Y %H:%M:%S"),
        "shops": shops,
    }
    log.info(F"Data retrieved: from {output['start_date']} to {output['end_date']} taken {output['time_elapsed']}")
    return output

def get_data3(start_date=None, end_date=None):
    start = time()

    end_date = adjust_end_date(end_date)

    # Esta es la función que necesitaríamos implementar para obtener los pedidos y sus line items.
    errors, orders_and_line_items = get_unfulfilled_orders_and_line_items(start_date=start_date, end_date=end_date)
    end = time()

    # Formatear los datos en la estructura deseada.
    output = {
        "orders": orders_and_line_items,
        "errors": errors,
        "time_elapsed": f"{end - start} seconds",
        "start_date": start_date.strftime("%d-%m-%Y %H:%M:%S") if start_date else "",
        "end_date": end_date.strftime("%d-%m-%Y %H:%M:%S"),
        "shops": shops,
    }
    log.info(F"Data retrieved: from {output['start_date']} to {output['end_date']} taken {output['time_elapsed']}")
    return output



# Path: app.py
app = Flask(__name__)


@app.errorhandler(500)
def handle_500(e):
    log.error(f"Error: {e}")
    return {"error": str(e)}, 500


def handle_request(processing_function):
    # Get the data and try to convert the start_date and end_date to datetime objects
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    log.info(f"Getting data from {start_date} to {end_date}")

    try:
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now()
    except ValueError as e:
        log.error(f"Error parsing dates: {e}")
        return {f"error: Error parsing dates: {e}"}, 400

    data = processing_function(start_date=start_date, end_date=end_date)

    # Return the data as JSON
    return data

@app.route("/shopify/unfulfilled/sku", methods=["GET"])
def shopify_unfilfilled_sku():
    return handle_request(get_data)

@app.route("/shopify/unfulfilled/skus-by-order", methods=["GET"])
def shopify_unfilfilled_orders_skus():
    return handle_request(get_data2)



# to fill divain pro ShopifyOrder and ShopifyOrderLineItem
@app.route("/shopify/unfulfilled/orders_and_line_items", methods=["GET"])
def shopify_unfilfilled_orders_and_line_items():
    return handle_request(get_data3)

if __name__ == "__main__":
    # app.run(debug=True, port=5666)

    from tornado.httpserver import HTTPServer
    from tornado.ioloop import IOLoop
    from tornado.wsgi import WSGIContainer

    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(5666)
    http_server.start()
    log.info("Server started")
    IOLoop.current().start()

