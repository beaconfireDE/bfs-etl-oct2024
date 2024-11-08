MERGE INTO airflow1007.bf_dev.dim_company_financials_team1 AS tgt
    USING (
        SELECT 
            ID,
            SYMBOL,
            PRICE,
            BETA,
            VOLAVG,
            MKTCAP,
            LASTDIV,
            DCFDIFF,
            DCF
        FROM us_stock_daily.dccm.company_profile
        ) AS src
    ON tgt.ID = src.ID
    WHEN MATCHED THEN UPDATE SET
        tgt.SYMBOL = src.SYMBOL,
        tgt.PRICE = src.PRICE,
        tgt.BETA = src.BETA,
        tgt.VOLAVG = src.VOLAVG,
        tgt.MKTCAP = src.MKTCAP,
        tgt.LASTDIV = src.LASTDIV,
        tgt.DCFDIFF = src.DCFDIFF,
        tgt.DCF = src.DCF
    WHEN NOT MATCHED THEN INSERT (ID, SYMBOL, PRICE, BETA, VOLAVG, MKTCAP,LASTDIV, DCFDIFF,DCF)
    VALUES (src.ID, src.SYMBOL, src.PRICE, src.BETA, src.VOLAVG, src.MKTCAP, src.LASTDIV, src.DCFDIFF, src.DCF);

