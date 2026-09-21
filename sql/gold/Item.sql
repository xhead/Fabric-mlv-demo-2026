SELECT
     i.ItemNumber
    ,i.BaseMaterialItemNumber
    ,i.Item
    ,i.UPC
    ,i.ItemCategoryCode
    ,i.ItemGroup
    ,i.UnitofMeasure
    ,i.CurrentUnitCost
    ,i.ContainerCode
    ,i.QuantityPerContainer
    ,i.FGCode
    ,i.Blocked
    ,i.Genus
    ,i.Discontinued
    ,i.LastUpdated
FROM silver.Item i
