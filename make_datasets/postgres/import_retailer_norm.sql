DROP TABLE IF EXISTS retailer.Inventory;
DROP TABLE IF EXISTS retailer.Weather;
DROP TABLE IF EXISTS retailer.Location;
DROP TABLE IF EXISTS retailer.Census;
DROP TABLE IF EXISTS retailer.Item;
DROP SCHEMA IF EXISTS retailer;

CREATE SCHEMA retailer;

CREATE TABLE retailer.Census(
    zip INTEGER,
    population FLOAT,
    white FLOAT,
    asian FLOAT,
    pacific FLOAT,
    black FLOAT,
    medianage FLOAT,
    occupiedhouseunits FLOAT,
    houseunits FLOAT,
    families FLOAT,
    households FLOAT,
    husbwife FLOAT,
    males FLOAT,
    females FLOAT,
    householdschildren FLOAT,
    hispanic FLOAT,
    PRIMARY KEY (zip)
);



CREATE TABLE retailer.Location(
    locn INTEGER,
    zip INTEGER,
    rgn_cd FLOAT,
    clim_zn_nbr FLOAT,
    tot_area_sq_ft FLOAT,
    sell_area_sq_ft FLOAT,
    avghhi FLOAT,
    supertargetdistance FLOAT,
    supertargetdrivetime FLOAT,
    targetdistance FLOAT,
    targetdrivetime FLOAT,
    walmartdistance FLOAT,
    walmartdrivetime FLOAT,
    walmartsupercenterdistance FLOAT,
    walmartsupercenterdrivetime FLOAT,
    PRIMARY KEY (locn)
);


CREATE TABLE retailer.Item(
    ksn INTEGER,
    subcategory INTEGER,            -- category(31)
    category INTEGER,               -- category(10)
    categoryCluster INTEGER,        -- category(8)
    prize FLOAT,
    feat_1 FLOAT,
    PRIMARY KEY (ksn)
);


CREATE TABLE retailer.Weather(
    locn INTEGER,
    dateid INTEGER,
    rain INTEGER,                   -- category(2)
    snow INTEGER,                   -- category(2)
    maxtemp FLOAT,
    mintemp FLOAT,
    meanwind FLOAT,
    thunder INTEGER,                -- category(2)
    PRIMARY KEY (locn, dateid)
);


CREATE TABLE retailer.Inventory(
	id INTEGER,
    locn INTEGER,
    dateid INTEGER,
    ksn INTEGER,
    inventoryunits FLOAT,
    PRIMARY KEY (locn, dateid, ksn)
);

\copy retailer.Census FROM 'census_dataset.csv' WITH (FORMAT CSV, NULL '');
\copy retailer.Location FROM 'location_dataset.csv' WITH (FORMAT CSV, NULL '');
\copy retailer.Item FROM 'item_dataset.csv' WITH (FORMAT CSV, NULL '');
\copy retailer.Weather FROM 'weather_dataset.csv' WITH (FORMAT CSV, NULL '');
\copy retailer.Inventory FROM 'inventory_dataset.csv' WITH (FORMAT CSV, NULL '');