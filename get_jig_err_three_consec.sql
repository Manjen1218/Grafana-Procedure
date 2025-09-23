DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_err_three_consec`(
      IN in_db VARCHAR(64), 
      IN from_time VARCHAR(30), 
      IN to_time VARCHAR(30)
    )
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE db_name VARCHAR(64);
  DECLARE tbl_name VARCHAR(64);

  DECLARE cur CURSOR FOR
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = in_db
      AND TABLE_NAME IN ('pt', 'pts', 'pdlp');

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  SET @sql_query = '';
  SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
  SET @to_dt   = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO db_name, tbl_name;
    IF done THEN
      LEAVE read_loop;
    END IF;

    SET @table_sql = CONCAT(
      "SELECT jig, err_msg, err_id, '", tbl_name, "' AS station, min(tbeg), max(consecutive_count) ",
        "FROM (",
          "SELECT jig, err_msg, err_id, tbeg, ",
            "@consec := IF(@prev_jig = jig AND @prev_err = err_msg, @consec + 1, 1) AS consecutive_count, ",
            "@prev_jig := jig, ",
            "@prev_err := err_msg ",
          "FROM (",
            "SELECT * FROM `", db_name, "`.`", tbl_name, "` ",
              "WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' ",
                "AND is_y = 1 ",
          ") AS ordered_logs ORDER BY jig, tbeg ",
        ") AS counted ",
      "GROUP BY jig, err_msg ",
      "HAVING err_msg IS NOT NULL AND MAX(consecutive_count) >= 3"
    );

    IF @sql_query = '' THEN
      SET @sql_query = @table_sql;
    ELSE
      SET @sql_query = CONCAT(@sql_query, " UNION ALL ", @table_sql);
    END IF;

  END LOOP;

  CLOSE cur;

  IF @sql_query = '' THEN
    SELECT 'No pt/pts/pdlp tables with matching records found.' AS message;
  ELSE
    SET @final_query = CONCAT(@sql_query, " ORDER BY jig");

    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END //