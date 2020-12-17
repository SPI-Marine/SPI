set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadRowLevelSecurity;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	10/19/2020
Description:	Creates the LoadRowLevelSecurity stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadRowLevelSecurity
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Warehouse table
	if object_id(N'Warehouse.RowLevelSecurity', 'U') is not null
		truncate table Warehouse.RowLevelSecurity;

	begin try
		with DimensionAlternateKeys(DimName, AlternateKey, FieldValue)
		as
		(
			select distinct 'Product', p.ProductAlternateKey, p.ProductName from Warehouse.Dim_Product p
			union
			select distinct 'ChartererParent', cp.ChartererParentAlternateKey, cp.ChartererParentName from Warehouse.Dim_ChartererParent cp
			union
			select distinct 'OwnerParent', op.OwnerParentAlternateKey, op.OwnerParentName from Warehouse.Dim_OwnerParent op
			union
			select distinct 'Charterer', C.ChartererAlternateKey, c.FullStyleName from Warehouse.Dim_Charterer c
			union
			select distinct 'Owner', o.OwnerAlternateKey, o.FullStyleName from Warehouse.Dim_Owner o
			union
			select distinct 'Region', r.RegionAlternateKey, r.RegionName from Warehouse.Dim_Region r
		)
		insert
				Warehouse.RowLevelSecurity with (tablock)	(
																RecordID,
																Product,
																ChartererParent,
																OwnerParent,
																UserName,
																LoadRegion,
																DischargeRegion,
																FullStyleName,
																[GUID],
																MinCPDateToPull
															)
		select
				rls.RecordID						RecordID,
				rls.Product							Product,
				rls.ChartererParent					ChartererParent,
				rls.OwnerParent						OwnerParent,
				rls.UserName						UserName,
				rls.LoadRegion						LoadRegion,
				rls.DischargeRegion					DischargeRegion,
				rls.FullStyleName					FullStyleName,
				rls.[GUID]							[GUID],
				rls.MinCPDateToPull					MinCPDateToPull
			from
				Staging.RowLevelSecurity rls
					left join DimensionAlternateKeys prod
						on prod.FieldValue = rls.Product
							and prod.DimName = 'Product'
					left join DimensionAlternateKeys chpar
						on chpar.FieldValue = rls.ChartererParent
							and chpar.DimName = 'ChartererParent'
					left join DimensionAlternateKeys ownpar
						on ownpar.FieldValue = rls.OwnerParent
							and ownpar.DimName = 'OwnerParent'
					left join DimensionAlternateKeys reg
						on reg.FieldValue = rls.Product
							and reg.DimName = 'Region'
					left join DimensionAlternateKeys prod
						on prod.FieldValue = rls.Product
							and prod.DimName = 'Product'
					left join DimensionAlternateKeys prod
						on prod.FieldValue = rls.Product
							and prod.DimName = 'Product'
					;
	end try
	begin catch
		select @ErrorMsg = 'Loading RowLevelSecurity records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end