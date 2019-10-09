-- add all your SQL setup statements here. 

-- You can assume that the following base table has been created with data loaded for you when we test your submission 
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column 
-- names / types if you like.

CREATE TABLE FLIGHTS (
    fid int PRIMARY KEY, 
    month_id int,        -- 1-12
    day_of_month int,    -- 1-31 
    day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
    carrier_id varchar(7), 
    flight_num int,
    origin_city varchar(34), 
    origin_state varchar(47), 
    dest_city varchar(34), 
    dest_state varchar(46), 
    departure_delay int, -- in mins
    taxi_out int,        -- in mins
    arrival_delay int,   -- in mins
    canceled int,        -- 1 means canceled
    actual_time int,     -- in mins
    distance int,        -- in miles
    capacity int, 
    price int,            -- in $          
);

CREATE TABLE Users (
    username varchar(50),
    password varchar(50),
    balance int,
    PRIMARY KEY(username) 
);


CREATE TABLE Reservations (
    rev_id int ,
    it_id int,
    username varchar(50),
    paid int,
    fid1 int,
    fid2 int,
    day int,
    PRIMARY KEY(rev_id)
);

CREATE TABLE Capacity (
    fid int,
    capacity int,
    FOREIGN KEY(fid) REFERENCES Flights(fid)
);