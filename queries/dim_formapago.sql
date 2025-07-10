-- Extrae las formas de pago únicas desde la tabla Venta para poblar la dimensión DIM_FormaPago
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY FormaPago) AS idformapago,
    FormaPago AS formapago
FROM Venta
WHERE FormaPago IS NOT NULL;
