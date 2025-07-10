-- Extrae fechas únicas de la tabla Venta y genera los campos para la dimensión DIM_Tiempo
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY Fecha) AS idtiempo,
    DATEPART(DAY, Fecha) AS dia,
    DATEPART(YEAR, Fecha) AS anio,
    DATEPART(MONTH, Fecha) AS mes,
    DATENAME(MONTH, Fecha) AS nombremes,
    DATENAME(WEEKDAY, Fecha) AS nombredia,
    CAST(Fecha AS DATE) AS fechaventa
FROM Venta
WHERE Fecha IS NOT NULL;
