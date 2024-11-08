MERGE INTO airflow1007.bf_dev.fact_stock_history_team1 AS tgt
    USING (
        SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE) AS row_rnk
            FROM us_stock_daily.dccm.stock_history
            ) AS src_no_dup
        WHERE row_rnk = 1
        ) AS src
    ON tgt.date = src.date AND tgt.symbol = src.symbol
    WHEN MATCHED THEN UPDATE SET
        tgt.open = src.open,
        tgt.high = src.high,
        tgt.low = src.low,
        tgt.close = src.close,
        tgt.volume = src.volume,
        tgt.adjclose = src.adjclose
    WHEN NOT MATCHED THEN INSERT (symbol, date, open, high, low, close, volume, adjclose)
    VALUES (src.symbol, src.date, src.open, src.high, src.low, src.close, src.volume, src.adjclose)
;
