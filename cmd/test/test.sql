-- test integer
CREATE TABLE test_int(
    a1 tinyint,
    a2 tinyint unsigned,
    b1 smallint,
    b2 smallint unsigned,
    c1 mediumint,
    c2 mediumint unsigned,
    d1 int,
    d2 int unsigned,
    e1 bigint,
    e2 bigint unsigned,
    primary key (`a1`));


insert into test_int() values(-126, 253, -4000, 4000,  -400000, 400000, -4000000, 4000000, -5000000000, 5000000000);
update test_int set a2 = 0 where a1 = -126;


-- test float
create table test_float(
    a1 float,
    a2 float unsigned,
    b1 double,
    b2 double unsigned,
    c1 decimal(5,2),
    c2 decimal(5,2) unsigned,
    d1 numeric(5,2),
    d2 numeric(5,2) unsigned,
    e1 bit,
    primary key (`a1`)
);

insert into test_float() values(-1.23, 1.23, -2.56, 2.56, -999.99, 999.99, -888.88, 888.88, b'100');

-- test date
create table test_date(
    a1 date,
    a2 date,
    b1 datetime,
    b2 datetime,
    c1 timestamp,
    c2 timestamp,
    d1 time,
    d2 time,
    e1 year,
    e2 year,
    primary key (`a1`)
);

insert into test_date() values('2020-11-16', '1000-01-01', '2020-11-16 20:00:00', '2020-11-16 20:00:00.999999', '1970-01-02 00:00:01', '2020-02-24 00:00:01.999999', '-600:59:59', '12:59:59', '1901', '2020' );

-- test string
create table test_string(
    a1 char(6),
    a2 char,
    b1 varchar(20),
    c1 binary(6),
    c2 binary,
    d1 VARBINARY(10),
    e1 TINYBLOB,
    f1 BLOB,
    g1 MEDIUMBLOB,
    h1 LONGBLOB,
    i1 TINYTEXT,
    j1 TEXT,
    k1 MEDIUMTEXT,
    l1 LONGTEXT,
    m1 ENUM('test-a', 'test-b', 'test-c', 'test-d'),
    o1 SET('one', 'two'),
    o2 SET('one', 'two'),
    primary key(`a1`)
);

insert into test_string() values('asdf', 'a', 'asdfsdaf', unhex('FA34E'), unhex('F'), unhex('FA34E1'), 'TINYBLOB', 'blob', 'MEDIUMBLOB', 'LONGBLOB', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'test-c', '', 'one,two');
