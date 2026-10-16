# Olist E-Commerce Database Overview

## Entity-Relationship Diagram

```mermaid
erDiagram
    orders ||--o{ order_items : "has"
    orders ||--o{ payments : "paid via"
    orders ||--o{ reviews : "reviewed in"
    orders }o--|| customers : "placed by"
    order_items }o--|| products : "contains"
    order_items }o--|| sellers : "sold by"
    products }o--o| category_translation : "translated by"
    customers }o--o{ geolocation : "located via zip"
    sellers }o--o{ geolocation : "located via zip"

    orders {
        string order_id PK
        string customer_id FK
        string order_status
        timestamp order_purchase_timestamp
        timestamp order_approved_at
        timestamp order_delivered_carrier_date
        timestamp order_delivered_customer_date
        timestamp order_estimated_delivery_date
    }

    order_items {
        string order_id FK
        int order_item_id PK
        string product_id FK
        string seller_id FK
        timestamp shipping_limit_date
        double price
        double freight_value
    }

    customers {
        string customer_id PK
        string customer_unique_id
        int customer_zip_code_prefix FK
        string customer_city
        string customer_state
    }

    products {
        string product_id PK
        string product_category_name FK
        int product_name_lenght
        int product_description_lenght
        int product_photos_qty
        int product_weight_g
        int product_length_cm
        int product_height_cm
        int product_width_cm
    }

    sellers {
        string seller_id PK
        int seller_zip_code_prefix FK
        string seller_city
        string seller_state
    }

    payments {
        string order_id FK
        int payment_sequential PK
        string payment_type
        int payment_installments
        double payment_value
    }

    reviews {
        string review_id PK
        string order_id FK
        int review_score
        string review_comment_title
        string review_comment_message
        timestamp review_creation_date
        timestamp review_answer_timestamp
    }

    geolocation {
        int geolocation_zip_code_prefix PK
        double geolocation_lat
        double geolocation_lng
        string geolocation_city
        string geolocation_state
    }

    category_translation {
        string product_category_name PK
        string product_category_name_english
    }
```

## Tables Summary

| Table | Rows (approx) | Primary Key | Description |
|-------|---------------|-------------|-------------|
| `orders` | 99,441 | `order_id` | One row per order — status and timestamps |
| `order_items` | 112,650 | `order_id` + `order_item_id` | One row per item in an order (an order can have multiple items) |
| `customers` | 99,441 | `customer_id` | One row per order-customer pair. `customer_unique_id` identifies the actual person across orders |
| `products` | 32,951 | `product_id` | Product catalog with dimensions and category |
| `sellers` | 3,095 | `seller_id` | Marketplace sellers with location |
| `payments` | 103,886 | `order_id` + `payment_sequential` | One row per payment method used on an order (an order can be split across methods) |
| `reviews` | 99,224 | `review_id` + `order_id` | Customer reviews with scores (1–5) and optional comments |
| `geolocation` | 1,000,163 | `zip_code_prefix` (non-unique) | Multiple lat/lng points per zip code — no true PK |
| `category_translation` | 71 | `product_category_name` | Portuguese → English category name lookup |

## Relationship Details

```mermaid
flowchart LR
    subgraph Central["Central Entity"]
        O["orders"]
    end

    subgraph Facts["Transactional"]
        OI["order_items"]
        P["payments"]
        R["reviews"]
    end

    subgraph Dimensions["Dimensions"]
        C["customers"]
        PR["products"]
        S["sellers"]
        CT["category_translation"]
    end

    subgraph Geo["Geography"]
        G["geolocation"]
    end

    O -->|"1:N via order_id"| OI
    O -->|"1:N via order_id"| P
    O -->|"1:N via order_id"| R
    O -->|"N:1 via customer_id"| C
    OI -->|"N:1 via product_id"| PR
    OI -->|"N:1 via seller_id"| S
    PR -->|"N:1 via product_category_name"| CT
    C -.->|"via zip_code_prefix"| G
    S -.->|"via zip_code_prefix"| G
```

## Join Paths

| From | To | Join Key | Cardinality |
|------|----|----------|-------------|
| `orders` | `order_items` | `order_id` | 1 → N |
| `orders` | `payments` | `order_id` | 1 → N |
| `orders` | `reviews` | `order_id` | 1 → N |
| `orders` | `customers` | `customer_id` | N → 1 |
| `order_items` | `products` | `product_id` | N → 1 |
| `order_items` | `sellers` | `seller_id` | N → 1 |
| `products` | `category_translation` | `product_category_name` | N → 1 |
| `customers` | `geolocation` | `customer_zip_code_prefix = geolocation_zip_code_prefix` | N → N |
| `sellers` | `geolocation` | `seller_zip_code_prefix = geolocation_zip_code_prefix` | N → N |

## Key Gotchas

> [!WARNING]
> **`customer_id` ≠ person.** Each `customer_id` is unique to one order. The actual person is identified by `customer_unique_id`. You must use `customer_unique_id` when counting repeat customers.

> [!NOTE]
> **`geolocation` has no true primary key.** A single zip code maps to many lat/lng points (multiple neighborhoods). Joining directly will produce duplicates — aggregate or deduplicate first.

> [!NOTE]
> **`order_items` is the fact table grain.** A single order can have multiple items from different sellers at different prices. `order_id` + `order_item_id` is the composite key.
