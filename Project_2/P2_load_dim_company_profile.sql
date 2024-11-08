MERGE INTO airflow1007.bf_dev.dim_company_profile_team1 AS tgt
    USING (
        SELECT 
            SYMBOL,
            COMPANYNAME,
            SECTOR,
            INDUSTRY,
            WEBSITE
        FROM us_stock_daily.dccm.company_profile
        ) AS src
    ON tgt.symbol = src.symbol
    WHEN MATCHED THEN UPDATE SET
        tgt.COMPANYNAME = src.COMPANYNAME,
        tgt.SECTOR = src.SECTOR,
        tgt.INDUSTRY = src.INDUSTRY,
        tgt.WEBSITE = src.WEBSITE
    WHEN NOT MATCHED THEN INSERT (symbol, COMPANYNAME, SECTOR, INDUSTRY, WEBSITE)
    VALUES (src.symbol, src.COMPANYNAME, src.SECTOR, src.INDUSTRY, src.WEBSITE)
;

