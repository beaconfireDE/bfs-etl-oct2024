MERGE INTO airflow1007.bf_dev.dim_symbols_team1 AS tgt
    USING (
        SELECT *
        FROM us_stock_daily.dccm.symbols
        ) AS src
    ON tgt.symbol = src.symbol
    WHEN MATCHED THEN UPDATE SET
        tgt.name = src.name,
        tgt.exchange = src.exchange
    WHEN NOT MATCHED THEN INSERT (symbol, name, exchange)
    VALUES (src.symbol, src.name, src.exchange);

