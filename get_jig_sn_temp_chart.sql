DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_sn_temp_chart`(
    IN in_db VARCHAR(64),
    IN in_tbl VARCHAR(64),
    IN in_wo TEXT,
    IN in_jig VARCHAR(64)
)
BEGIN
    DECLARE temp_expr TEXT;
    DECLARE select_temp_cols TEXT;
    DECLARE full_sql TEXT;

    -- Dynamically detect available temperature columns and build expressions
    SELECT 
      GROUP_CONCAT(CONCAT('IFNULL(', column_name, ', -9999)') ORDER BY column_name SEPARATOR ', '),
      GROUP_CONCAT(CONCAT(column_name, ' IS NOT NULL') ORDER BY column_name SEPARATOR ' OR ')
    INTO 
      temp_expr,
      select_temp_cols
    FROM information_schema.columns
    WHERE table_schema = in_db
      AND table_name = in_tbl
      AND column_name IN (
        'soc_temp_max', 'critical_temp_max', 'berg5_soc_t_max', 'hpl_soc_temp_max'
      );

    -- Wrap in CASE WHEN to avoid GREATEST(NULL, NULL, ...) returning NULL
    IF temp_expr IS NOT NULL AND temp_expr != '' AND select_temp_cols IS NOT NULL AND select_temp_cols != '' THEN
      SET temp_expr = CONCAT(
        "CASE WHEN (", select_temp_cols, ") ",
        "THEN GREATEST(", temp_expr, ") ELSE NULL END AS max_temp"
      );
    ELSE
      SET temp_expr = "NULL AS max_temp";
      SET select_temp_cols = '';  -- No available temperature columns
    END IF;

    -- Build the final dynamic SQL query
    SET full_sql = CONCAT(
      "SELECT jig, tbeg, tend, sn, err_msg, ", temp_expr, 
      " FROM ", in_db, ".", in_tbl,
      " WHERE wo IN (", in_wo, ") AND jig = '", in_jig, "' AND is_y = 1",
      " ORDER BY tbeg"
    );

    -- Execute the dynamically built SQL
    PREPARE stmt FROM full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //