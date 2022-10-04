CREATE TABLE Inventory(
    location INTEGER,
    date INTEGER,
    ksn INTEGER,
    inventory NUMERIC
);--add fillfactor if updates
\COPY inventory FROM 'C:\Users\massi\Downloads\retailer\retailer\Inventory.tbl' DELIMITER '|' CSV;
ALTER TABLE inventory ADD COLUMN id SERIAL PRIMARY KEY;
UPDATE inventory SET location = NULL WHERE id <= 4200000;
UPDATE inventory SET date = NULL WHERE (id <= 8400000 AND id >= 4200000);
