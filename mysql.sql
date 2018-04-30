-- Create database transfers
CREATE DATABASE IF NOT EXISTS `transfers`;
USE `transfers`;

-- Create table customers
CREATE TABLE IF NOT EXISTS `customers` (
  `idcust` tinyint(4) NOT NULL AUTO_INCREMENT,
  `customer` varchar(200) NOT NULL,
  `ena` tinyint(4) NOT NULL,
  `alarmlimitsec` int(11) NOT NULL,
  `emailalarm` text NOT NULL,
  PRIMARY KEY (`idcust`)
) ENGINE=InnoDB;

-- Create table files
CREATE TABLE IF NOT EXISTS `files` (
  `idfiles` int(11) NOT NULL AUTO_INCREMENT,
  `idcust` tinyint(4) NOT NULL,
  `tsstart` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `tsend` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `fromfile` blob NOT NULL,
  `tofile` blob NOT NULL,
  `status` enum('U','F','S','C') NOT NULL,
  PRIMARY KEY (`idfiles`)
) ENGINE=InnoDB;

-- Create table log
CREATE TABLE IF NOT EXISTS `log` (
  `idlog` int(11) NOT NULL AUTO_INCREMENT,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `idcust` tinyint(4) NOT NULL DEFAULT '0',
  `msg` blob,
  `etype` enum('I','E') NOT NULL,
  PRIMARY KEY (`idlog`),
  KEY `idx_tscust` (`idcust`,`ts`)
) ENGINE=InnoDB;

-- Create procedure logerror
DELIMITER //
CREATE PROCEDURE `logerror`(cust VARCHAR(100),msg BLOB)
BEGIN
    DECLARE idc TINYINT DEFAULT 0;
    DECLARE x INT DEFAULT 0;
    
    SELECT COUNT(idcust) INTO x FROM customers WHERE customer=cust;
    IF x = 0 THEN
        INSERT INTO customers (customer) VALUES (cust);
    END IF;
    SELECT idcust INTO idc FROM customers WHERE customer=cust;
    INSERT INTO log (idcust,msg,etype) VALUES (idc,msg,"E");
    SELECT 0;
END//
DELIMITER ;

-- Create procedure logfileC
DELIMITER //
CREATE PROCEDURE `logfileC`(i INT)
BEGIN
    UPDATE files SET tsstart=tsstart,tsend=NOW(),status="C" WHERE idfiles=i;
END//
DELIMITER ;

-- Create procedure logfileF
DELIMITER //
CREATE PROCEDURE `logfileF`(i INT)
BEGIN
    UPDATE files SET tsstart=tsstart,tsend=tsend,status="F" WHERE idfiles=i;
END//
DELIMITER ;

-- Create procedure logfileS
DELIMITER //
CREATE PROCEDURE `logfileS`(i INT)
BEGIN
    UPDATE files SET tsstart=tsstart,tsend=NOW(),status="S" WHERE idfiles=i;
END//
DELIMITER ;

-- Create procedure logfileU
DELIMITER //
CREATE PROCEDURE `logfileU`(cust VARCHAR(100),ffile BLOB,tfile BLOB, OUT xid INT)
BEGIN
    DECLARE idc TINYINT DEFAULT 0;
    DECLARE x INT DEFAULT 0;
    
    SELECT COUNT(idcust) INTO x FROM customers WHERE customer=cust;
    IF x = 0 THEN
        INSERT INTO customers (customer) VALUES (cust);
    END IF;
    SELECT idcust INTO idc FROM customers WHERE customer=cust;
    INSERT INTO files (idcust,tsstart,tsend,fromfile,tofile,status) VALUES (idc,NOW(),'00000000000000',ffile,tfile,"U");
    SET xid=LAST_INSERT_ID();
END//
DELIMITER ;

-- Create procedure loginfo
DELIMITER //
CREATE PROCEDURE `loginfo`(cust VARCHAR(100),msg BLOB)
BEGIN
    DECLARE idc TINYINT DEFAULT 0;
    DECLARE x INT DEFAULT 0;
    
    SELECT COUNT(idcust) INTO x FROM customers WHERE customer=cust;
    IF x = 0 THEN
        INSERT INTO customers (customer) VALUES (cust);
    END IF;
    SELECT idcust INTO idc FROM customers WHERE customer=cust;
    INSERT INTO log (idcust,msg,etype) VALUES (idc,msg,"I");
    SELECT 0;
END//
DELIMITER ;
