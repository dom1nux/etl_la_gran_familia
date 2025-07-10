-- Script optimizado para poblar la tabla de hechos Hechos_Venta usando dimensiones ya pobladas
SELECT
    t.idtiempo,
    e.EmpleadoID AS idempleado,
    p.ProyectoID AS idproyecto,
    i.InmuebleID AS idinmueble,
    f.idformapago,
    c.ClienteID AS idcliente,
    v.MontoTotal AS montototal,
    0 AS montoganancia, -- Placeholder, calcular si tienes la lógica
    0 AS montocosto,    -- Placeholder, calcular si tienes la lógica
    p.Presupuesto AS presupuesto
FROM Venta v
JOIN Cliente c ON v.ClienteID = c.ClienteID
JOIN Empleado e ON v.EmpleadoID = e.EmpleadoID
JOIN Inmueble i ON v.InmuebleID = i.InmuebleID
JOIN Proyecto p ON i.ProyectoID = p.ProyectoID
JOIN DIM_Tiempo t ON CAST(v.Fecha AS DATE) = CAST(t.fecha AS DATE)
JOIN DIM_FormaPago f ON v.FormaPago = f.formapago
;
