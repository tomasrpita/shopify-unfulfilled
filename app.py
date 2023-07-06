import shopify
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

shops = ["ES", "FR", "IT", "NL"]

# "Cash on Delivery" (COD) 
cod = ["ES", "IT"]


def main():
    one_day_ago = datetime.now() - timedelta(days=1)

    # Format that Shopify expects
    one_day_ago_iso = one_day_ago.isoformat()
    
    sku_all_counts = {}

    for shop in shops:

        API_KEY = os.getenv(f"API_KEY_{shop}")
        PASSWORD = os.getenv(f"PASSWORD_{shop}")
        SHOP_NAME = os.getenv(f"SHOP_{shop}")
        shop_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin"
        
        shopify.ShopifyResource.set_site(shop_url)
        
        paid_orders = shopify.Order.find(financial_status="paid", fulfillment_status="unfulfilled", created_at_min=one_day_ago_iso)
        
        if shop in cod:
            pending_orders = shopify.Order.find(financial_status="pending", fulfillment_status="unfulfilled", created_at_min=one_day_ago_iso)
            cod_orders = [order for order in pending_orders if 'COD' in order.tags.split(', ')]
            orders = paid_orders + cod_orders
        else:
            orders = paid_orders
        
        sku_counts = {}
        for order in orders:
            for line_item in order.line_items:
                if line_item.sku.startswith("DIVAIN"):
                    sku_counts[line_item.sku] = sku_counts.get(line_item.sku, 0) + line_item.quantity

        sku_all_counts[shop] = sku_counts

    print(sku_all_counts)

if __name__ == "__main__":
    main()