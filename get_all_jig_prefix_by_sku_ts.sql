DELIMITER //

CREATE OR REPLACE PROCEDURE get_all_jig_prefix_by_sku_ts(
    IN in_db VARCHAR(64),
    IN in_ts VARCHAR(64),
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE sql_query TEXT;
    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    SET @final_sql = CONCAT(
        'SELECT DISTINCT SUBSTRING_INDEX(jig, ''-'', 1) AS jig_prefix ',
        'FROM `', in_db, '`.`', in_ts, '` ',
        'WHERE jig IS NOT NULL ',
        'AND is_y = 1 ',
        'AND tbeg BETWEEN ''', @from_dt, ''' AND ''', @to_dt, ''' ',
        'ORDER BY jig_prefix'
    );

    PREPARE stmt FROM @final_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END//

DELIMITER ;
