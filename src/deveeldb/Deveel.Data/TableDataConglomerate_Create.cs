// 
//  Copyright 2010-2011  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed partial class TableDataConglomerate {
		/// <summary>
		/// Create the system tables that must be present in a conglomerates.
		/// </summary>
		/// <remarks>
		/// These tables consist of contraint and table management data.
		/// <para>
		/// <list type="table">
		///   <listheader>
		///     <item>Table name</item>
		///   </listheader>
		///   <item>pkey_info</item>
		///   <description>Primary key constraint information.</description>
		///   <item>fkey_info</item>
		///   <description>Foreign key constraint information.</description>
		///   <item>unique_info</item>
		///   <description>Unique set constraint information.</description>
		///   <item>check_info</item>
		///   <description>Check constraint information.</description>
		///   <item>primary_columns</item>
		///   <description>Primary columns information (refers to PKeyInfo)</description>
		///   <item>unique_columns</item>
		///   <description>Unique columns information (refers to UniqueInfo)</description>
		///   <item>sUSRForeignColumns1</item>
		///   <description>Foreign column information (refers to FKeyInfo)</description>
		///   <item>sUSRForeignColumns2</item>
		///   <description>Secondary Foreign column information (refers to FKeyInfo).</description>
		/// </list>
		/// These tables handle data for referential integrity. There are also
		/// some additional tables containing general table information.
		/// <list type="table">
		///   <listheader>
		///     <item>Table name</item>
		///   </listheader>
		///   <item>table_column_info</item>
		///   <description>All table and column information.</description>
		/// </list>
		/// The design is fairly elegant in that we are using the database to 
		/// store information to maintain referential integrity.
		/// </para>
		/// <para>
		/// The schema layout for these tables:
		/// <code>
		/// CREATE TABLE pkey_info (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the primary key constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		PRIMARY KEY (id),
		///		UNIQUE (schema, table)
		///	);
		///	
		/// CREATE TABLE fkey_info (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the foreign key constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		ref_schema  TEXT NOT NULL,  // The name of the schema referenced
		///		ref_table   TEXT NOT NULL,  // The name of the table referenced
		///		update_rule TEXT NOT NULL,  // The rule for updating to table
		///		delete_rule TEXT NOT NULL,  // The rule for deleting from table
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		
		///		PRIMARY KEY (id)
		///	);
		///	
		/// CREATE TABLE unique_info (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the unique constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		
		///		PRIMARY KEY (id)
		///	);
		///	
		/// CREATE TABLE check_info (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the check constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		expression  TEXT NOT NULL,  // The check expression
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		
		///		PRIMARY KEY (id)
		///	);
		///	
		/// CREATE TABLE primary_columns (
		///		pk_id   NUMERIC NOT NULL, // The primary key constraint id
		///		column  TEXT NOT NULL,    // The name of the primary
		///		seq_no  INTEGER NOT NULL, // The sequence number of this constraint
		///		
		///		FOREIGN KEY pk_id REFERENCES pkey_info
		///	);
		///	
		/// CREATE TABLE unique_columns (
		///		un_id   NUMERIC NOT NULL, // The unique constraint id
		///		column  TEXT NOT NULL,    // The column that is unique
		///		seq_no  INTEGER NOT NULL, // The sequence number of this constraint
		///		
		///		FOREIGN KEY un_id REFERENCES unique_info
		///	);
		///	
		/// CREATE TABLE sUSRForeignColumns1 (
		///		fk_id   NUMERIC NOT NULL, // The foreign key constraint id
		///		fcolumn TEXT NOT NULL,    // The column in the foreign key
		///		pcolumn TEXT NOT NULL,    // The column in the primary key
		///								  // (referenced)
		///		seq_no  INTEGER NOT NULL, // The sequence number of this constraint
		///		
		///		FOREIGN KEY fk_id REFERENCES fkey_info
		///	);
		///	
		/// CREATE TABLE schema_info (
		///		id     NUMERIC NOT NULL,
		///		name   TEXT NOT NULL,
		///		type   TEXT,              // Schema type (system, etc)
		///		other  TEXT,
		///		
		///		UNIQUE ( name )
		///	);
		///	
		/// CREATE TABLE table_info (
		///		id     NUMERIC NOT NULL,
		///		name   TEXT NOT NULL,     // The name of the table
		///		schema TEXT NOT NULL,     // The name of the schema of this table
		///		type   TEXT,              // Table type (temporary, system, etc)
		///		other  TEXT,              // Notes, etc
		///		
		///		UNIQUE ( name )
		///	);
		///	
		/// CREATE TABLE sUSRColumnColumns (
		///		t_id    NUMERIC NOT NULL,  // Foreign key to table_info
		///		column  TEXT NOT NULL,     // The column name
		///		seq_no  INTEGER NOT NULL,  // The sequence in the table
		///		type    TEXT NOT NULL,     // The SQL type of this column
		///		size    NUMERIC,           // The size of the column if applicable
		///		scale   NUMERIC,           // The scale of the column if applicable
		///		default TEXT NOT NULL,     // The default expression
		///		constraints TEXT NOT NULL, // The constraints of this column
		///		other   TEXT,              // Notes, etc
		///		
		///		FOREIGN KEY t_id REFERENCES table_info,
		///		UNIQUE ( t_id, column )
		///	);
		///	</code>
		/// </para>
		/// </remarks>
		internal void UpdateSystemTableSchema() {
			// Create the transaction
			Transaction transaction = CreateTransaction();

			DataTableDef table;

			table = new DataTableDef();
			table.TableName = SysSequenceInfo;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("type"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = SysSequence;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_id"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("last_value"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("increment"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("minvalue"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("maxvalue"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("start"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("cache"));
			table.AddColumn(DataTableColumnDef.CreateBooleanColumn("cycle"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = PrimaryInfoTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = ForeignInfoTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("ref_schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("ref_table"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("update_rule"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("delete_rule"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = UniqueInfoTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = CheckInfoTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("expression"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			table.AddColumn(
					DataTableColumnDef.CreateBinaryColumn("serialized_expression"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = PrimaryColsTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("pk_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("column"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = UniqueColsTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("un_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("column"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = ForeignColsTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("fk_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("fcolumn"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("pcolumn"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = SchemaInfoTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("type"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("other"));
			transaction.AlterCreateTable(table, 91, 128);

			// Stores misc variables of the database,
			table = new DataTableDef();
			table.TableName = PersistentVarTable;
			table.AddColumn(DataTableColumnDef.CreateStringColumn("variable"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("value"));
			transaction.AlterCreateTable(table, 91, 128);

			// the UDT tables...
			table = new DataTableDef();
			table.TableName = UdtTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("attrs"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("parent"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("ext_parent"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = UdtMembersTable;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("type_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("col_type"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("size"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("scale"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("not_null"));
			transaction.AlterCreateTable(table, 91, 128);

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Debug.WriteException(e);
				throw new ApplicationException("Transaction Exception creating conglomerate.");
			}
		}


		/// <summary>
		/// Populates the system table schema with initial data for an empty
		/// conglomerate.
		/// </summary>
		/// <remarks>
		/// This sets up the standard variables and table constraint data.
		/// </remarks>
		private void InitializeSystemTableSchema() {
			// Create the transaction
			Transaction transaction = CreateTransaction();

			// Insert the two default schema names,
			transaction.CreateSchema(SystemSchema, "SYSTEM");

			// -- Primary Keys --
			// The 'id' columns are primary keys on all the system tables,
			String[] id_col = new String[] { "id" };
			transaction.AddPrimaryKeyConstraint(PrimaryInfoTable,
					  id_col, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PK_PK");
			transaction.AddPrimaryKeyConstraint(ForeignInfoTable,
					  id_col, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_FK_PK");
			transaction.AddPrimaryKeyConstraint(UniqueInfoTable,
					  id_col, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_UNIQUE_PK");
			transaction.AddPrimaryKeyConstraint(CheckInfoTable,
					  id_col, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_CHECK_PK");
			transaction.AddPrimaryKeyConstraint(SchemaInfoTable,
					  id_col, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_SCHEMA_PK");

			// -- Foreign Keys --
			// Create the foreign key references,
			String[] fk_col = new String[1];
			String[] fk_ref_col = new String[] { "id" };
			fk_col[0] = "pk_id";
			transaction.AddForeignKeyConstraint(
					  PrimaryColsTable, fk_col, PrimaryInfoTable, fk_ref_col,
					  ConstraintAction.NoAction, ConstraintAction.NoAction,
					  ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PK_FK");
			fk_col[0] = "fk_id";
			transaction.AddForeignKeyConstraint(
					  ForeignColsTable, fk_col, ForeignInfoTable, fk_ref_col,
					  ConstraintAction.NoAction, ConstraintAction.NoAction,
					  ConstraintDeferrability.InitiallyImmediate, "SYSTEM_FK_FK");
			fk_col[0] = "un_id";
			transaction.AddForeignKeyConstraint(
					  UniqueColsTable, fk_col, UniqueInfoTable, fk_ref_col,
					  ConstraintAction.NoAction, ConstraintAction.NoAction,
					  ConstraintDeferrability.InitiallyImmediate, "SYSTEM_UNIQUE_FK");

			// pkey_info 'schema', 'table' column is a unique set,
			// (You are only allowed one primary key per table).
			String[] columns = new String[] { "schema", "table" };
			transaction.AddUniqueConstraint(PrimaryInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PKEY_ST_UNIQUE");
			// schema_info 'name' column is a unique column,
			columns = new String[] { "name" };
			transaction.AddUniqueConstraint(SchemaInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_SCHEMA_UNIQUE");
			//    columns = new String[] { "name" };
			columns = new String[] { "name", "schema" };
			// pkey_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(PrimaryInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PKEY_UNIQUE");
			// fkey_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(ForeignInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_FKEY_UNIQUE");
			// unique_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(UniqueInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_UNIQUE_UNIQUE");
			// check_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(CheckInfoTable,
				 columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_CHECK_UNIQUE");

			// database_vars 'variable' is unique
			columns = new String[] { "variable" };
			transaction.AddUniqueConstraint(PersistentVarTable,
			   columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_DATABASEVARS_UNIQUE");

			// Insert the version number of the database
			transaction.SetPersistentVariable("database.version", "1.4");

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Debug.WriteException(e);
				throw new ApplicationException("Transaction Exception initializing conglomerate.");
			}

		}

		/// <summary>
		/// Initializes the <see cref="BlobStore"/>.
		/// </summary>
		/// <remarks>
		/// If the <see cref="BlobStore"/> doesn't exist it will be created, 
		/// and if it does exist it will be initialized.
		/// </remarks>
		private void InitializeBlobStore() {

			// Does the file already exist?
			bool blob_store_exists = StoreSystem.StoreExists("BlobStore");
			// If the blob store doesn't exist and we are read_only, we can't do
			// anything further so simply return.
			if (!blob_store_exists && IsReadOnly) {
				return;
			}

			// The blob store,
			if (blob_store_exists) {
				actBlobStore = StoreSystem.OpenStore("BlobStore");
			} else {
				actBlobStore = StoreSystem.CreateStore("BlobStore");
			}

			try {
				actBlobStore.LockForWrite();

				// Create the BlobStore object
				blobStore = new BlobStore(actBlobStore);

				// Get the 64 byte fixed area
				IMutableArea fixed_area = actBlobStore.GetMutableArea(-1);
				// If the blob store didn't exist then we need to create it here,
				if (!blob_store_exists) {
					long header_p = blobStore.Create();
					fixed_area.WriteInt8(header_p);
					fixed_area.CheckOut();
				} else {
					// Otherwise we need to initialize the blob store
					long header_p = fixed_area.ReadInt8();
					blobStore.Init(header_p);
				}
			} finally {
				actBlobStore.UnlockForWrite();
			}

		}

		/// <summary>
		/// Minimally creates a new conglomerate but does <b>not</b> initialize 
		/// any of the system tables.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This is a useful feature for a copy function that requires a 
		/// <see cref="TableDataConglomerate"/> object to copy data into but 
		/// does not require any initial system tables (because this information 
		/// is copied from the source conglomerate.
		/// </remarks>
		internal void MinimalCreate(String name) {
			this.name = name;

			if (Exists(name))
				throw new IOException("Conglomerate already exists: " + name);

			// Lock the store system (generates an IOException if exclusive Lock
			// can not be made).
			if (!IsReadOnly) {
				StoreSystem.Lock(name);
			}

			// Create/Open the state store
			actStateStore = StoreSystem.CreateStore(name + StatePost);
			try {
				actStateStore.LockForWrite();

				stateStore = new StateStore(actStateStore);
				long head_p = stateStore.Create();
				// Get the fixed area
				IMutableArea fixed_area = actStateStore.GetMutableArea(-1);
				fixed_area.WriteInt8(head_p);
				fixed_area.CheckOut();
			} finally {
				actStateStore.UnlockForWrite();
			}

			SetupInternal();

			// Init the conglomerate blob store
			InitializeBlobStore();

			// Create the system table (but don't initialize)
			UpdateSystemTableSchema();

		}

		/// <summary>
		/// Creates a new conglomerate at the given path in the file system.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This must be an empty directory where files can be stored. This 
		/// will create the conglomerate and exit in an open (read/write) state.
		/// </remarks>
		public void Create(String name) {
			MinimalCreate(name);

			// Initialize the conglomerate system tables.
			InitializeSystemTableSchema();

			// Commit the state
			stateStore.Commit();

		} 
	}
}