# GrafanaStoredProcedures

This repository contains all stored procedures used in the Grafana dashboard.

Below is the matching of dashboard page to the stored procedures used within:
- Timely Dashboard
    - [`get_station_yield_summary_threshold`](./get_station_yield_summary_threshold.sql)
- JIG Temperature Distribution - sku_level
    - [`get_jig_heat_distribution`](./get_jig_heat_distribution.sql)
- JIG RPI Temperature Distribution
    - [`get_jig_rpi_distribution`](./get_jig_rpi_distribution.sql)
- PT Fail Distribution - sku_level
    - [`get_error_distribution_by_sku`](./get_error_distribution_by_sku.sql)
- PT Yield - sku_level
    - [`get_test_summary_by_jig`](./get_test_summary_by_jig.sql)
- JIG Heatmap
    - [`get_jig_heatmap`](./get_jig_heatmap.sql)
- Miscellaneous
    - [`get_gbic_sn_yield_summary_threshold`](./get_gbic_sn_yield_summary_threshold.sql)
    - [`get_jig_temp_last_ten`](./get_jig_temp_last_ten.sql)
    - [`get_jig_err_three_consec`](./get_jig_err_three_consec.sql)
- Work Order History
    - [`get_station_yield_summary_wo`](./get_station_yield_summary_wo.sql)
    - [`get_jig_yield_summary_wo`](./get_jig_yield_summary_wo.sql)
- WO-JIG-TS Detail
    - [`get_wo_jig_ts_detail`](./get_wo_jig_ts_detail.sql)
    - [`get_jig_sn_details`](./get_jig_sn_details.sql)
    - [`get_jig_sn_temp_chart`](./get_jig_sn_temp_chart.sql)
- SN History
    - [`get_sn_detail`](./get_sn_detail.sql)
- JIG Status
    - [`get_jig_error_history`](./get_jig_error_history.sql)
- JIG History
    - [`get_jig_full_history`](./get_jig_full_history.sql)
    - [`get_jig_full_history_temperature`](./get_jig_full_history_temperature.sql)
- JIG Info
    - [`get_jig_heat_sku_ts`](./get_jig_heat_sku_ts.sql)
    - [`get_jig_sku_ts_detail`](./get_jig_sku_ts_detail.sql)

## How to use create / update

1. `ssh` into your SQL server
2. Clone this repository
    ```
    $ git clone https://gitea.buwiki.bouzzing.com/K2_COMM/GrafanaStoredProcedures.git
    ```
3. Run the following in your terminal. We will be storing the procedures in database `admin`. This command will then prompt you to type in your password. After inputting your password, the procedure will be created or updated if already exists in the `admin` database.
    ```
    $ mysql -u <USERNAME> -p admin < <YOUR_PROCEDURE>.sql 
    ```
