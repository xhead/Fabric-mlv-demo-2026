SELECT
     d.CalendarDate AS `Date`
    ,d.DateOffset
    ,d.DateOffsetSort
    ,d.WeekOffset
    ,d.WeekOffsetSort
    ,d.FiscalYear
    ,d.FiscalYearNumber
    ,d.FiscalQuarter
    ,d.FiscalQuarterNumber
    ,d.Year
    ,d.YearMonth
    ,d.YearMonthSort
    ,d.Month
    ,d.MonthSort
    ,d.WeekEnding
    ,d.WeekStarting
    ,d.Week
    ,d.WeekSort
    ,d.Weekday
    ,d.WeekdaySort
    ,d.LastUpdated
FROM silver.Date d
