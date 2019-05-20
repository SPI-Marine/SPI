/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the LoadDim_Product stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/10/2019	Added ProductType ETL
Brian Boswick	05/20/2019	Remove deleted records from Warehouse
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Product;
go

create procedure ETL.LoadDim_Product
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Product', 'U') is not null
		truncate table Staging.Dim_Product;

	begin try
		insert
				Staging.Dim_Product
		select
				product.QBRecId						ProductAlternateKey,
				product.ProductName					ProductName,
				product.SpecificGravity				SpecificGravity,
				product.RequiredCoating				RequiredCoating,
				product.EU							EU,
				product.CIQ							CIQ,
				product.NIOP						NIOP,
				product.Notes						Notes,
				product.LiquidType					LiquidType,
				prodtype.TypeName					ProductType,
				0									Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				Products product
					left join ProductType prodtype
						on product.RelatedProductTypeId = prodtype.QBRecId
					left join	(
									select
											@ExistingRecord RecordStatus,
											ProductAlternateKey
										from
											Warehouse.Dim_Product
								) rs
						on rs.ProductAlternateKey = [Product].QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Product records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Product
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																ProductName,
																SpecificGravity,
																RequiredCoating,
																EU,
																CIQ,
																NIOP,
																Notes,
																LiquidType,
																ProductType
															)
												);
		
		update
				Staging.Dim_Product
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Product wp
			where
				wp.ProductAlternateKey = Staging.Dim_Product.ProductAlternateKey
				and wp.Type1HashValue <> Staging.Dim_Product.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Products into Warehouse table
	begin try
		insert
				Warehouse.Dim_Product
			select
					product.ProductAlternateKey,
					product.ProductName,
					product.SpecificGravity,
					product.RequiredCoating,
					product.EU,
					product.CIQ,
					product.NIOP,
					product.Notes,
					product.LiquidType,
					product.ProductType,
					product.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Product product
				where
					product.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Product
			set
				ProductName = product.ProductName,
				SpecificGravity = product.SpecificGravity,
				RequiredCoating = product.RequiredCoating,
				EU = product.EU,
				CIQ = product.CIQ,
				NIOP = product.NIOP,
				Notes = product.Notes,
				LiquidType = product.LiquidType,
				ProductType = product.ProductType,
				Type1HashValue = product.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Product product
			where
				product.RecordStatus & @ExistingRecord = @ExistingRecord
				and product.ProductAlternateKey = Warehouse.Dim_Product.ProductAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Product
			where
				not exists	(
								select
										1
									from
										Products p
									where
										p.QBRecId = ProductAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Product where ProductKey = -1)
		begin
			delete
					Warehouse.Dim_Product
				where
					ProductKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Product on;
			insert
					Warehouse.Dim_Product	(
														ProductKey,
														ProductAlternateKey,
														ProductName,
														SpecificGravity,
														RequiredCoating,
														EU,
														CIQ,
														NIOP,
														Notes,
														LiquidType,
														ProductType,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- ProductKey
							0,				-- ProductAlternateKey
							'Unknown',		-- ProductName
							0.0,			-- SpecificGravity
							'Unknown',		-- RequiredCoating
							'Unknown',		-- EU
							'Unknown',		-- CIQ
							'Unknown',		-- NIOP
							'Unknown',		-- Notes
							'Unknown',		-- LiquidType
							'Unknown',		-- ProductType
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Product off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end