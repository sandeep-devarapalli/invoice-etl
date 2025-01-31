CREATE TABLE IF NOT EXISTS invoices (
    invoice_id STRING,
    status STRING,
    vendor_info STRING,
    total_amount FLOAT,
    tax_details STRING,
    line_items STRING,
    json_extract STRING
);
