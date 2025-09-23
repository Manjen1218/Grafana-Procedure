DELIMITER //
CREATE OR REPLACE PROCEDURE `get_jig_sku_ts_detail`(
    IN in_db VARCHAR(64),
    IN in_tbl VARCHAR(64),
    IN in_jig VARCHAR(64),
    IN from_time VARCHAR(30), 
    IN to_time VARCHAR(30)
)
BEGIN
    DECLARE temp_expr TEXT;
    DECLARE select_temp_cols TEXT;
    DECLARE full_sql TEXT;
    DECLARE rpi_mac_col TEXT;
    DECLARE berg5_soc_t_max_col TEXT;
    DECLARE mcf_soc_t_max_col TEXT;
    DECLARE hpl_soc_temp_max_col TEXT;

    SET @from_dt = STR_TO_DATE(SUBSTRING_INDEX(from_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');
    SET @to_dt = STR_TO_DATE(SUBSTRING_INDEX(to_time, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

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
        'soc_temp_max', 'critical_temp_max', 'berg5_soc_t_max', 'hpl_soc_temp_max', 'mcf_soc_t_max'
      );
    
    -- Check if rpi_mac column exists
    SELECT 
      IF(COUNT(*) > 0, 'rpi_mac', 'NULL AS rpi_mac')
    INTO 
      rpi_mac_col
    FROM 
      information_schema.columns
    WHERE 
      table_schema = in_db
      AND table_name = in_tbl
      AND column_name = 'rpi_mac';

    SELECT 
      IF(COUNT(*) > 0, 'berg5_soc_t_max', 'NULL AS berg5_soc_t_max')
    INTO 
      berg5_soc_t_max_col
    FROM 
      information_schema.columns
    WHERE 
      table_schema = in_db
      AND table_name = in_tbl
      AND column_name = 'berg5_soc_t_max';

    SELECT 
      IF(COUNT(*) > 0, 'mcf_soc_t_max', 'NULL AS mcf_soc_t_max')
    INTO 
      mcf_soc_t_max_col
    FROM 
      information_schema.columns
    WHERE 
      table_schema = in_db
      AND table_name = in_tbl
      AND column_name = 'mcf_soc_t_max';

    SELECT 
      IF(COUNT(*) > 0, 'hpl_soc_temp_max', 'NULL AS hpl_soc_temp_max')
    INTO 
      hpl_soc_temp_max_col
    FROM 
      information_schema.columns
    WHERE 
      table_schema = in_db
      AND table_name = in_tbl
      AND column_name = 'hpl_soc_temp_max';

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
      "SELECT sn, bname, tbeg, err_msg, err_id, ", rpi_mac_col, ", rpi_temp_max, soc_temp_max, critical_temp_max, ", berg5_soc_t_max_col, ",", hpl_soc_temp_max_col, ",", mcf_soc_t_max_col, ",", temp_expr, ", fullpath "
      " FROM ", in_db, ".", in_tbl,
      " WHERE tbeg BETWEEN '", @from_dt, "' AND '", @to_dt, "' AND jig = '", in_jig, "' AND is_y = 1",
      " ORDER BY tbeg, sn"
    );

    -- Execute the dynamically built SQL
    PREPARE stmt FROM full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //