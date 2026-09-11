INSERT INTO catalog.restaurants (
    id,
    name,
    cuisine,
    is_open
) VALUES (
    'rst-pizza-1',
    'Pizza Roma',
    'Italian',
    TRUE
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.menu_categories (
    id,
    restaurant_id,
    name,
    sort_order
) VALUES
    ('cat-pizza', 'rst-pizza-1', 'Pizza', 10),
    ('cat-drinks', 'rst-pizza-1', 'Drinks', 20)
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.menu_items (
    id,
    restaurant_id,
    category_id,
    name,
    description,
    price_cents,
    available
) VALUES
    (
        'item-margherita',
        'rst-pizza-1',
        'cat-pizza',
        'Margherita',
        'Tomato sauce, mozzarella, basil',
        69000,
        TRUE
    ),
    (
        'item-lasagna',
        'rst-pizza-1',
        'cat-pizza',
        'Lasagna',
        'Temporarily unavailable',
        82000,
        FALSE
    ),
    (
        'item-lemonade',
        'rst-pizza-1',
        'cat-drinks',
        'Lemonade',
        'House lemonade',
        22000,
        TRUE
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.menu_item_modifiers (
    id,
    menu_item_id,
    name,
    price_delta_cents,
    available
) VALUES (
    'mod-extra-cheese',
    'item-margherita',
    'Extra cheese',
    0,
    TRUE
)
ON CONFLICT (id) DO NOTHING;
