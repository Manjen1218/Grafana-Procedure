DELIMITER //
CREATE OR REPLACE PROCEDURE get_all_sku_by_sn(IN in_sn VARCHAR(255))
BEGIN
DECLARE done INT DEFAULT FALSE;
DECLARE db_name VARCHAR(64);
DECLARE tbl_name VARCHAR(64);
DECLARE cur CURSOR FOR
    SELECT table_schema, table_name 
    FROM information_schema.columns
    WHERE column_name = 'sn'
    AND table_schema LIKE 'k2%';
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

-- Clear old data at the start
TRUNCATE TABLE admin.sn_wo_db;

OPEN cur;

read_loop: LOOP
    FETCH cur INTO db_name, tbl_name;
    IF done THEN
    LEAVE read_loop;
    END IF;

    SET @query = CONCAT(
    'INSERT IGNORE INTO admin.sn_wo_db (db_name) ',
    'SELECT ''', db_name, ''' ',
    'FROM `', db_name, '`.`', tbl_name, '` ',
    'WHERE sn = ''', in_sn, ''' LIMIT 1;'
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END LOOP;

CLOSE cur;

SELECT distinct * FROM admin.sn_wo_db;
END//