-- This script was generated by the ERD tool in pgAdmin 4.
-- Please log an issue at https://github.com/pgadmin-org/pgadmin4/issues/new/choose if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public."softcartDimDate"
(
    dateid integer NOT NULL,
    year integer,
    month integer,
    monthname character varying(50),
    day integer,
    weekday integer,
    weekdayname character varying(50),
    PRIMARY KEY (dateid)
);

CREATE TABLE IF NOT EXISTS public."softcartDimCategory"
(
    categoryid integer NOT NULL,
    category character varying(50),
    PRIMARY KEY (categoryid)
);

CREATE TABLE IF NOT EXISTS public."softcartDimItem"
(
    itemid integer,
    item character varying(100),
    price double precision,
    PRIMARY KEY (itemid)
);

CREATE TABLE IF NOT EXISTS public."softcartDimCountry"
(
    countryid integer,
    country character varying(150),
    PRIMARY KEY (countryid)
);

CREATE TABLE IF NOT EXISTS public."softcartFactSales"
(
    orderid integer NOT NULL,
    itemid integer,
    categoryid integer,
    countryid integer,
    dateid integer,
    PRIMARY KEY (orderid)
);

ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (itemid)
    REFERENCES public."softcartDimItem" (itemid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES public."softcartDimCategory" (categoryid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."softcartDimCountry" (countryid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."softcartFactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."softcartDimDate" (dateid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;