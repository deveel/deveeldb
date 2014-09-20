// 
//  Copyright 2010-2014 Deveel
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
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
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
		///   <item>fkey_columns1</item>
		///   <description>Foreign column information (refers to FKeyInfo)</description>
		///   <item>fkey_columns2</item>
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
		/// CREATE TABLE fkey_columns1 (
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
		/// CREATE TABLE table_columns (
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
		private void UpdateSystemSchema() {
			// Create the transaction
			ICommitableTransaction transaction = CreateTransaction();

			UpdateSystemSchema(transaction);

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Logger.Error(this, e);
				throw new ApplicationException("Transaction Exception creating conglomerate.", e);
			}
		}

		private void UpdateSystemSchema(ICommitableTransaction transaction) {
			SystemSchema.AddSystemTables(transaction);
		}


		/// <summary>
		/// Populates the system table schema with initial data for an empty
		/// conglomerate.
		/// </summary>
		/// <remarks>
		/// This sets up the standard variables and table constraint data.
		/// </remarks>
		private void InitializeSystemSchema() {
			// Create the transaction
			ICommitableTransaction transaction = CreateTransaction();

			// Insert the two default schema names,
			transaction.CreateSchema(SystemSchema.Name, "SYSTEM");

			InitializeSystemSchema(transaction);

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Logger.Error(this, e);
				throw new ApplicationException("Transaction Exception initializing conglomerate.", e);
			}
		}

		private void InitializeSystemSchema(ICommitableTransaction transaction) {
			SystemSchema.Initialize(transaction);
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
			bool blobStoreExists = StoreSystem.StoreExists("BlobStore");
			// If the blob store doesn't exist and we are read_only, we can't do
			// anything further so simply return.
			if (!blobStoreExists && IsReadOnly) {
				return;
			}

			// The blob store,
			if (blobStoreExists) {
				actBlobStore = StoreSystem.OpenStore("BlobStore");
			} else {
				actBlobStore = StoreSystem.CreateStore("BlobStore");
			}

			try {
				actBlobStore.LockForWrite();

				// Create the BlobStore object
				BlobStore = new BlobStore(actBlobStore);

				// Get the 64 byte fixed area
				IMutableArea fixedArea = actBlobStore.GetMutableArea(-1);
				// If the blob store didn't exist then we need to create it here,
				if (!blobStoreExists) {
					long headerP = BlobStore.Create();
					fixedArea.WriteInt8(headerP);
					fixedArea.CheckOut();
				} else {
					// Otherwise we need to initialize the blob store
					long headerP = fixedArea.ReadInt8();
					BlobStore.Init(headerP);
				}
			} finally {
				actBlobStore.UnlockForWrite();
			}

		}

		/// <summary>
		/// Minimally creates a new conglomerate but does <b>not</b> initialize 
		/// any of the system tables.
		/// </summary>
		/// <remarks>
		/// This is a useful feature for a copy function that requires a 
		/// <see cref="TableDataConglomerate"/> object to copy data into but 
		/// does not require any initial system tables (because this information 
		/// is copied from the source conglomerate.
		/// </remarks>
		internal void MinimalCreate() {
			if (Exists())
				throw new IOException("Conglomerate already exists: " + Name);

			// Lock the store system (generates an IOException if exclusive Lock
			// can not be made).
			if (!IsReadOnly) {
				StoreSystem.Lock(Name);
			}

			// Create/Open the state store
			actStateStore = StoreSystem.CreateStore(Name + StatePost);
			try {
				actStateStore.LockForWrite();

				StateStore = new StateStore(actStateStore);
				long headP = StateStore.Create();
				// Get the fixed area
				IMutableArea fixedArea = actStateStore.GetMutableArea(-1);
				fixedArea.WriteInt8(headP);
				fixedArea.CheckOut();
			} finally {
				actStateStore.UnlockForWrite();
			}

			SetupInternal();

			// Init the conglomerate blob store
			InitializeBlobStore();

			// Create the system table (but don't initialize)
			UpdateSystemSchema();

		}

		/// <summary>
		/// Creates a new conglomerate at the given path in the file system.
		/// </summary>
		/// <remarks>
		/// This must be an empty directory where files can be stored. This 
		/// will create the conglomerate and exit in an open (read/write) state.
		/// </remarks>
		public void Create() {
			MinimalCreate();

			// Initialize the conglomerate system tables.
			InitializeSystemSchema();

			// Commit the state
			StateStore.Commit();
		} 
	}
}