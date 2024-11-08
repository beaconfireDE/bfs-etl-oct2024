MERGE INTO DIM_SYMBOLS_TEAM6 AS target
USING US_STOCK_DAILY.DCCM.SYMBOLS AS source
    ON target.SYMBOL = source.SYMBOL
    WHEN MATCHED THEN
        UPDATE SET
            target.NAME = source.NAME,
            target.EXCHANGE = source.EXCHANGE
    WHEN NOT MATCHED THEN
        INSERT (SYMBOL,NAME,EXCHANGE)
        VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);