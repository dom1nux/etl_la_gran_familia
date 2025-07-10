USE master;
GO

-- 1. Eliminar la base de datos si ya existe
IF EXISTS (SELECT name FROM sys.databases WHERE name =
'GranFamiliaMart')
BEGIN
	ALTER DATABASE GranFamiliaMart SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE GranFamiliaMart;
	PRINT 'Base de datos GranFamiliaMart eliminada.';
END
GO

-- 2. Crear la base de datos
CREATE DATABASE GranFamiliaMart;
PRINT 'Base de datos GranFamiliaMart creada.';
GO
-- 3. Usar la base de datos
USE GranFamiliaMart;
GO

CREATE SCHEMA DIM
go

CREATE TABLE DIM.Cliente
( 
	idcliente            integer IDENTITY ( 1,1 ) ,
	nombrecliente        varchar(50)  NOT NULL ,
	apellidocliente      varchar(50)  NULL ,
	contacto             VARCHAR(100)  NULL ,
	email                VARCHAR(100)  NULL 
)
go



ALTER TABLE DIM.Cliente
	ADD CONSTRAINT XPKDIM_Cliente PRIMARY KEY  CLUSTERED (idcliente ASC)
go



CREATE TABLE DIM.Empleado
( 
	idempleado           integer IDENTITY ( 1,1 ) ,
	nombreempleado       varchar(50)  NOT NULL ,
	cargo                varchar(50)  NULL ,
	apellidoempleado     varchar(50)  NOT NULL 
)
go



ALTER TABLE DIM.Empleado
	ADD CONSTRAINT XPKDIM_Empleado PRIMARY KEY  CLUSTERED (idempleado ASC)
go



CREATE TABLE DIM.FormaPago
( 
	idformapago          integer IDENTITY ( 1,1 ) ,
	formapago            varchar(20)  NULL 
)
go



ALTER TABLE DIM.FormaPago
	ADD CONSTRAINT XPKDIM_FormaPago PRIMARY KEY  CLUSTERED (idformapago ASC)
go



CREATE TABLE DIM.Inmueble
( 
	idinmueble           integer IDENTITY ( 1,1 ) ,
	tipoinmueble         varchar(50)  NULL ,
	direccion            varchar(200)  NULL ,
	area                 decimal(10,2)  NULL ,
	estadoinmueble       varchar(20)  NULL ,
	precio               decimal(12,2)  NOT NULL ,
	idproyecto           integer  NOT NULL 
)
go



ALTER TABLE DIM.Inmueble
	ADD CONSTRAINT XPKDIM_Inmueble PRIMARY KEY  CLUSTERED (idinmueble ASC)
go



CREATE TABLE DIM.Proyecto
( 
	idproyecto           integer IDENTITY ( 1,1 ) ,
	nombreproyecto       varchar(100)  NOT NULL ,
	tipoproyecto         varchar(50)  NULL ,
	ubicacion            varchar(200)  NOT NULL ,
	estadoproyecto       varchar(20)  NULL ,
	fechainicio          date  NULL ,
	fechafinestimada     date  NULL ,
	presupuesto          decimal(15,2)  NULL 
)
go



ALTER TABLE DIM.Proyecto
	ADD CONSTRAINT XPKDIM_Proyecto PRIMARY KEY  CLUSTERED (idproyecto ASC)
go



CREATE TABLE DIM.Tiempo
( 
	idtiempo             integer IDENTITY ( 1,1 ) ,
	dia                  integer  NULL ,
	anio                 integer  NULL ,
	mes                  integer  NULL ,
	nombremes            varchar(20)  NULL ,
	nombredia            varchar(20)  NULL ,
	fechaventa           datetime  NOT NULL 
)
go



ALTER TABLE DIM.Tiempo
	ADD CONSTRAINT XPKDIM_Tiempo PRIMARY KEY  CLUSTERED (idtiempo ASC)
go



CREATE TABLE DIM.Hechos_Venta
( 
	idtiempo             integer  NOT NULL ,
	idempleado           integer  NOT NULL ,
	idproyecto           integer  NOT NULL ,
	idinmueble           integer  NOT NULL ,
	idformapago          integer  NOT NULL ,
	idcliente            integer  NOT NULL ,
	montototal           decimal(10,2)  NOT NULL ,
	montoganancia        decimal(10,2)  NOT NULL ,
	montocosto           decimal(12,2)  NULL ,
	presupuesto          decimal(15,2)  NULL 
)
go



ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT XPKDIM_Hechos_Venta PRIMARY KEY  CLUSTERED (idtiempo ASC,idempleado ASC,idproyecto ASC,idinmueble ASC,idformapago ASC,idcliente ASC)
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_8 FOREIGN KEY (idtiempo) REFERENCES DIM.Tiempo(idtiempo)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_9 FOREIGN KEY (idempleado) REFERENCES DIM.Empleado(idempleado)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_10 FOREIGN KEY (idcliente) REFERENCES DIM.Cliente(idcliente)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_11 FOREIGN KEY (idformapago) REFERENCES DIM.FormaPago(idformapago)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_12 FOREIGN KEY (idinmueble) REFERENCES DIM.Inmueble(idinmueble)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go




ALTER TABLE DIM.Hechos_Venta
	ADD CONSTRAINT R_13 FOREIGN KEY (idproyecto) REFERENCES DIM.Proyecto(idproyecto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go

