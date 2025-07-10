CREATE DATABASE GranFamilia;
GO

USE GranFamilia;
GO

-- Tabla de Proyectos (esencial)
CREATE TABLE Proyecto (
    ProyectoID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    Tipo VARCHAR(50) CHECK (Tipo IN ('Residencial','Comercial','Industrial')),
    Ubicacion VARCHAR(200) NOT NULL,
    FechaInicio DATE,
    FechaFinEstimada DATE,
    Presupuesto DECIMAL(15,2),
    Estado VARCHAR(20) DEFAULT 'Planificado'
);

-- Tabla de Inmuebles (core del negocio)
CREATE TABLE Inmueble (
    InmuebleID INT PRIMARY KEY IDENTITY(1,1),
    ProyectoID INT NOT NULL,
    Tipo VARCHAR(50) CHECK (Tipo IN ('Casa','Departamento','Oficina','Local')),
    Direccion VARCHAR(200),
    Area DECIMAL(10,2),
    Precio DECIMAL(12,2) NOT NULL,
    Estado VARCHAR(20) DEFAULT 'Disponible',
    FOREIGN KEY (ProyectoID) REFERENCES Proyecto(ProyectoID)
);

-- Tabla de Clientes (necesaria para ventas)
CREATE TABLE Cliente (
    ClienteID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(50) NOT NULL,
    Apellido VARCHAR(50),
    Contacto VARCHAR(100),
    Email VARCHAR(100),
    FechaRegistro DATE DEFAULT GETDATE()
);

-- Tabla de Empleados (reducida a lo esencial)
CREATE TABLE Empleado (
    EmpleadoID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(50) NOT NULL,
    Apellido VARCHAR(50) NOT NULL,
    Cargo VARCHAR(50),
    Activo BIT DEFAULT 1
);

-- Tabla de Ventas (transacción principal)
CREATE TABLE Venta (
    VentaID INT PRIMARY KEY IDENTITY(1,1),
    InmuebleID INT NOT NULL,
    ClienteID INT NOT NULL,
    EmpleadoID INT NOT NULL, -- Vendedor
    Fecha DATE DEFAULT GETDATE(),
    MontoTotal DECIMAL(12,2) NOT NULL,
    FormaPago VARCHAR(20),
    FOREIGN KEY (InmuebleID) REFERENCES Inmueble(InmuebleID),
    FOREIGN KEY (ClienteID) REFERENCES Cliente(ClienteID),
    FOREIGN KEY (EmpleadoID) REFERENCES Empleado(EmpleadoID)
);

-- Tabla de Proveedores (nueva)
CREATE TABLE Proveedor (
    ProveedorID INT PRIMARY KEY IDENTITY(1,1),
    Nombre VARCHAR(100) NOT NULL,
    RUC VARCHAR(20),
    Contacto VARCHAR(100),
    Telefono VARCHAR(20),
    Email VARCHAR(100),
    Direccion VARCHAR(200),
    Especialidad VARCHAR(100)
);

-- Tabla de Materiales (nueva)
CREATE TABLE Material (
    MaterialID INT PRIMARY KEY IDENTITY(1,1),
    ProveedorID INT,
    Codigo VARCHAR(50) UNIQUE,
    Nombre VARCHAR(100) NOT NULL,
    Tipo VARCHAR(50) CHECK (Tipo IN ('Construcción','Acabados','Instalaciones','Herramientas')),
    UnidadMedida VARCHAR(20) DEFAULT 'Unidad',
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    StockMinimo DECIMAL(10,2) DEFAULT 0,
    FOREIGN KEY (ProveedorID) REFERENCES Proveedor(ProveedorID)
);

-- Tabla de Costos (solo lo básico para análisis)
CREATE TABLE Costo (
    CostoID INT PRIMARY KEY IDENTITY(1,1),
    ProyectoID INT NOT NULL,
    Tipo VARCHAR(50) CHECK (Tipo IN ('Materiales','ManoObra','Logistica','Permisos')),
    Descripcion VARCHAR(200),
    Monto DECIMAL(12,2) NOT NULL,
    Fecha DATE,
    MaterialID INT NULL,
    Cantidad DECIMAL(10,2) DEFAULT 1,
    FOREIGN KEY (ProyectoID) REFERENCES Proyecto(ProyectoID),
    FOREIGN KEY (MaterialID) REFERENCES Material(MaterialID)
);

-- Tabla de Compras (nueva)
CREATE TABLE Compra (
    CompraID INT PRIMARY KEY IDENTITY(1,1),
    ProveedorID INT NOT NULL,
    Fecha DATE DEFAULT GETDATE(),
    Total DECIMAL(12,2) NOT NULL,
    Estado VARCHAR(20) DEFAULT 'Pendiente' CHECK (Estado IN ('Pendiente','Pagado','Cancelado')),
    EmpleadoID INT NOT NULL, -- Quién realizó la compra
    FOREIGN KEY (ProveedorID) REFERENCES Proveedor(ProveedorID),
    FOREIGN KEY (EmpleadoID) REFERENCES Empleado(EmpleadoID)
);

-- Tabla de Detalle de Compras (nueva)
CREATE TABLE DetalleCompra (
    DetalleCompraID INT PRIMARY KEY IDENTITY(1,1),
    CompraID INT NOT NULL,
    MaterialID INT NOT NULL,
    Cantidad DECIMAL(10,2) NOT NULL,
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (CompraID) REFERENCES Compra(CompraID),
    FOREIGN KEY (MaterialID) REFERENCES Material(MaterialID)
);