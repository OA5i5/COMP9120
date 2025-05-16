-- Function 1: 计算所有已售车辆的总销售额
CREATE OR REPLACE FUNCTION calculate_total_sales()
RETURNS NUMERIC AS $$
DECLARE
    total_sales NUMERIC;
BEGIN
    SELECT COALESCE(SUM(Price), 0)
    INTO total_sales
    FROM CarSales
    WHERE IsSold = TRUE;

    RETURN total_sales;
END;
$$ LANGUAGE plpgsql;

-- Function 2: 按品牌统计已售车辆的总销售额
CREATE OR REPLACE FUNCTION get_sales_by_make()
RETURNS TABLE (MakeCode VARCHAR, TotalSales NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        MakeCode,
        SUM(Price)
    FROM CarSales
    WHERE IsSold = TRUE
    GROUP BY MakeCode;
END;
$$ LANGUAGE plpgsql;