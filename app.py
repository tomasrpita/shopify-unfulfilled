import shopify
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

API_KEY = os.getenv("API_KEY_ES")
PASSWORD = os.getenv("PASSWORD_ES")
SHOP_NAME = os.getenv("SHOP_ES")

shop_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin"

def main():
    one_day_ago = datetime.now() - timedelta(days=1)

    # Format that Shopify expects
    one_day_ago_iso = one_day_ago.isoformat()

    shopify.ShopifyResource.set_site(shop_url)
    orders = shopify.Order.find(financial_status="paid", fulfillment_status="unfulfilled", created_at_min=one_day_ago_iso)

    sku_counts = {}
    for order in orders:
        for line_item in order.line_items:
            sku_counts[line_item.sku] = sku_counts.get(line_item.sku, 0) + line_item.quantity
    # print(sku_counts)

if __name__ == "__main__":
    main()