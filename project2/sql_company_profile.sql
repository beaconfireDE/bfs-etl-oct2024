MERGE INTO DIM_COMPANY_PROFILE_TEAM6 AS target
USING US_STOCK_DAILY.DCCM.COMPANY_PROFILE AS source
    ON target.ID = source.ID
    WHEN MATCHED THEN
        UPDATE SET
            target.SYMBOL = source.SYMBOL,
            target.PRICE = source.PRICE,
            target.BETA = source.BETA,
            target.VOLAVG = source.VOLAVG,
            target.INDUSTRY = source.INDUSTRY,
            target.SECTOR = source.SECTOR,
            target.WEBSITE = source.WEBSITE,
            target.NAME = source.COMPANYNAME
    WHEN NOT MATCHED THEN
        INSERT (ID, SYMBOL, PRICE, BETA, VOLAVG, INDUSTRY, SECTOR,WEBSITE, NAME)
        VALUES (source.ID, source.SYMBOL, source.PRICE, source.BETA, source.VOLAVG, source.INDUSTRY, source.SECTOR, source.WEBSITE, source.COMPANYNAME);