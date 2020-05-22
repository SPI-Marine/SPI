set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.TruncateStagingTable;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	05/20/2020
Description:	Used by Data Factory to truncate the given table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.TruncateStagingTable
	@TableName varchar(500)
as
begin

	declare	@SQL				nvarchar(1000),
			@ErrorMsg			varchar(1000);

	begin try
		-- Clear Staging table
		if object_id(@TableName, 'U') is not null
			set @SQL = N'truncate table ' + @TableName + ';';
			exec sp_executesql @SQL;
		end try
	begin catch
		select @ErrorMsg = 'Truncating ' + @TableName + ' table - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
end