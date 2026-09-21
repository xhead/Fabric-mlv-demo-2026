SELECT
     s.SalesDate
    ,s.SalesId
    ,s.SeasonCode
    ,s.ItemNumber
    ,s.StoreNumber
    ,s.StationNumber
    ,s.RouteNumber
    ,s.SalespersonId
    ,s.DocumentNumber
    ,s.SalesType
    ,CAST(s.SaleUnitQuantity / COALESCE(i.QuantityPerContainer, 1) AS DECIMAL(14, 4)) AS SaleQuantity
    ,s.SaleUnitQuantity
    ,s.SalePrice
    ,s.SaleAmount
    ,CAST(s.ReturnUnitQuantity / COALESCE(i.QuantityPerContainer, 1) AS DECIMAL(14, 4)) AS ReturnQuantity
    ,s.ReturnUnitQuantity
    ,s.ReturnPrice
    ,s.ReturnAmount
    ,s.LastUpdated
FROM silver.Sales s
LEFT JOIN silver.Item i ON s.ItemNumber = i.ItemNumber
