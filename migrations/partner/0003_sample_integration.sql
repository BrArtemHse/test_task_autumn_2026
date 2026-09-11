INSERT INTO partner.restaurant_integrations (
    id,
    restaurant_id,
    external_store_id,
    callback_url,
    shared_secret_hash,
    status
) VALUES (
    'integration-sample-pizza',
    'rst-pizza-1',
    'store-pizza-1',
    'http://sample-restaurant-service:8084/orders',
    'development-only:not-used-in-consumer-slice',
    'active'
)
ON CONFLICT (restaurant_id) DO NOTHING;
