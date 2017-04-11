CREATE TABLE IF NOT EXISTS customer_lifetime_value (
    date date,
    version varchar(10),
    month_of_first_contact VARCHAR(100),
    accumulated_gmv DOUBLE PRECISION,
    number_of_orders BIGINT,
    PRIMARY KEY (date, month_of_first_contact, version)
);