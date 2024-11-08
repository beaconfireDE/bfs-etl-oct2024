MERGE INTO FACT_STOCK_HISTORY_TEAM6 AS target
USING
(
    SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE DESC) AS row_num
        FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY
    ) AS deduped_source
    WHERE row_num = 1
) AS source
ON target.SYMBOL = source.SYMBOL
AND target.TRADEDATE = source.DATE
WHEN MATCHED THEN
    UPDATE SET
        target.OPEN = source.OPEN,
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target.CLOSE = source.CLOSE,
        target.VOLUME = source.VOLUME,
        target.ADJCLOSE = source.ADJCLOSE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, TRADEDATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);