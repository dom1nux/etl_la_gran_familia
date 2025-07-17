-- Script optimizado para poblar la tabla de hechos Hechos_Venta usando solo tablas de la base OLTP y calculando montocosto y montoganancia
SELECT
    v.Fecha AS fecha,                -- Para poblar DIM_Tiempo después
    v.FormaPago AS formapago,        -- Para poblar DIM_FormaPago después
    e.EmpleadoID AS idempleado,
    p.ProyectoID AS idproyecto,
    i.InmuebleID AS idinmueble,
    c.ClienteID AS idcliente,
    v.MontoTotal AS montototal,
    -- Suma de costos del proyecto hasta la fecha de la venta
    ISNULL((SELECT SUM(cs.Monto * cs.Cantidad) FROM Costo cs WHERE cs.ProyectoID = p.ProyectoID AND cs.Fecha <= v.Fecha), 0) AS montocosto,
    -- Ganancia: monto de la venta menos el costo
    v.MontoTotal - ISNULL((SELECT SUM(cs.Monto * cs.Cantidad) FROM Costo cs WHERE cs.ProyectoID = p.ProyectoID AND cs.Fecha <= v.Fecha), 0) AS montoganancia,
    p.Presupuesto AS presupuesto,
    -- Agregar campos necesarios para el mapeo de IDs
    c.Nombre AS nombrecliente,
    c.Apellido AS apellidocliente,
    e.Nombre AS nombreempleado,
    e.Apellido AS apellidoempleado,
    p.Nombre AS nombreproyecto,
    i.Direccion AS direccion
FROM Venta v
INNER JOIN Cliente c ON v.ClienteID = c.ClienteID
INNER JOIN Empleado e ON v.EmpleadoID = e.EmpleadoID
INNER JOIN Inmueble i ON v.InmuebleID = i.InmuebleID
INNER JOIN Proyecto p ON i.ProyectoID = p.ProyectoID
WHERE v.Fecha IS NOT NULL
  AND v.FormaPago IS NOT NULL
ORDER BY v.Fecha, v.VentaID;
