PRODUCTS = [
    [1, 'product 1', 1],
    [2, 'product 2', 2],
    [3, 'product 3', 3],
    [4, 'product 4', 4],
    [5, 'product 5', 5],
]

ORDERS = [
    [1, [1, 2], 3],
    [2, [3, 4, 5], 0],
]

EXTRACTED_DATA = { 'products': PRODUCTS, 'orders': ORDERS }

TRANSFORMED_ORDERS = [
    {
        'order_id': 1,
        'products': [
            {'product_id': 1, 'product_name': 'product 1', 'price': 1},
            {'product_id': 2, 'product_name': 'product 2', 'price': 2},
        ],
        'total': 3
    },
    {
        'order_id': 1,
        'products': [
            {'product_id': 3, 'product_name': 'product 3', 'price': 3},
            {'product_id': 4, 'product_name': 'product 4', 'price': 4},
            {'product_id': 5, 'product_name': 'product 5', 'price': 5},
        ],
        'total': 12
    }
]
