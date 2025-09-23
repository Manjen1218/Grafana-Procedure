DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_full_history_temperature`(
    IN in_jig VARCHAR(64),
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE db_name VARCHAR(64);
    DECLARE tbl_name VARCHAR(64);
    DECLARE dyn_sql TEXT;
    DECLARE cur CURSOR FOR
      SELECT TABLE_SCHEMA, TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA LIKE 'k2%'
        AND TABLE_NAME IN ('pt', 'pts', 'pdlp');
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Convert time strings to datetime
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

    -- Temp table to collect all results
    DROP TEMPORARY TABLE IF EXISTS tmp_jig_history;
    CREATE TEMPORARY TABLE tmp_jig_history (
      wo VARCHAR(64),
      jig VARCHAR(64),
      tbeg DATETIME,
      tend DATETIME,
      soc_temp_max FLOAT,
      err_id VARCHAR(64),
      err_msg TEXT
    );

    OPEN cur;
    read_loop: LOOP
      FETCH cur INTO db_name, tbl_name;
      IF done THEN
          LEAVE read_loop;
      END IF;

      -- Construct and execute dynamic SQL
      SET @sql = CONCAT(
        'INSERT INTO tmp_jig_history (wo, jig, tbeg, tend, soc_temp_max, err_id, err_msg) ',
        'SELECT wo, jig, tbeg, tend, soc_temp_max, err_id, err_msg ',
        'FROM `', db_name, '`.`', tbl_name, '` ',
        'WHERE jig = ? AND tbeg < ? AND is_y = 1'
      );

      PREPARE stmt FROM @sql;
      EXECUTE stmt USING in_jig, @to_dt;
      DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;

    -- Return the collected results
    SELECT * FROM tmp_jig_history ORDER BY tbeg DESC;
END //