//  
//  TableDataConglomerate.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Data.Collections;
using Deveel.Data.Store;

using Deveel.Diagnostics;
using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// A conglomerate of data that represents the contents of all tables in a
	/// complete database.
	/// </summary>
	/// <remarks>
	/// This object handles all data persistance management (storage, retrieval, 
	/// removal) issues. It is a transactional manager for both data and indices 
	/// in the database.
	/// </remarks>
	public class TableDataConglomerate : IDisposable {
		/// <summary>
		/// The postfix on the name of the state file for the database store name.
		/// </summary>
		public const String STATE_POST = "_sf";

		// ---------- The standard constraint/schema tables ----------

		/// <summary>
		/// The name of the system schema where persistant conglomerate 
		/// state is stored.
		/// </summary>
		public const String SystemSchema = "SYSTEM";

		/**
		 * The schema info table.
		 */
		public static readonly TableName SCHEMA_INFO_TABLE = new TableName(SystemSchema, "sUSRSchemaInfo");
		public static readonly TableName PERSISTENT_VAR_TABLE = new TableName(SystemSchema, "sUSRDatabaseVars");
		public static readonly TableName FOREIGN_COLS_TABLE = new TableName(SystemSchema, "sUSRForeignColumns");
		public static readonly TableName UNIQUE_COLS_TABLE = new TableName(SystemSchema, "sUSRUniqueColumns");
		public static readonly TableName PRIMARY_COLS_TABLE = new TableName(SystemSchema, "sUSRPrimaryColumns");
		public static readonly TableName CHECK_INFO_TABLE = new TableName(SystemSchema, "sUSRCheckInfo");
		public static readonly TableName UNIQUE_INFO_TABLE = new TableName(SystemSchema, "sUSRUniqueInfo");
		public static readonly TableName FOREIGN_INFO_TABLE = new TableName(SystemSchema, "sUSRFKeyInfo");
		public static readonly TableName PRIMARY_INFO_TABLE = new TableName(SystemSchema, "sUSRPKeyInfo");
		public static readonly TableName SYS_SEQUENCE_INFO = new TableName(SystemSchema, "sUSRSequenceInfo");
		public static readonly TableName SYS_SEQUENCE = new TableName(SystemSchema, "sUSRSequence");
		public static readonly TableName UDT_TABLE = new TableName(SystemSchema, "sUSRUDT");
		public static readonly TableName UDT_COLS_TABLE = new TableName(SystemSchema, "sUSRUDTColumns");

		/// <summary>
		/// The TransactionSystem that this Conglomerate is a child of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The <see cref="IStoreSystem"/> object used by this conglomerate to 
		/// store the underlying representation.
		/// </summary>
		private readonly IStoreSystem store_system;

		/// <summary>
		/// The name given to this conglomerate.
		/// </summary>
		private String name;

		/// <summary>
		/// The actual store that backs the state store.
		/// </summary>
		private IStore act_state_store;

		/// <summary>
		/// A store for the conglomerate state container.
		/// </summary>
		/// <remarks>
		/// This file stores information persistantly about the state of this object.
		/// </remarks>
		private StateStore state_store;

		/// <summary>
		/// The current commit id for committed transactions.
		/// </summary>
		/// <remarks>
		/// Whenever transactional changes are committed to the conglomerate, this id 
		/// is incremented.
		/// </remarks>
		private long commit_id;


		/// <summary>
		/// The list of all tables that are currently open in this conglomerate.
		/// </summary>
		/// <remarks>
		/// This includes tables that are not committed.
		/// </remarks>
		private ArrayList table_list;

		/// <summary>
		/// The actual <see cref="IStore"/> implementation that maintains the <see cref="IBlobStore"/> 
		/// information for this conglomerate (if there is one).
		/// </summary>
		private IStore act_blob_store;

		/// <summary>
		/// The <see cref="IBlobStore"/> object for this conglomerate.
		/// </summary>
		private BlobStore blob_store;

		/// <summary>
		/// The <see cref="Data.SequenceManager"/> object for this conglomerate.
		/// </summary>
		private readonly SequenceManager sequence_manager;

		/// <summary>
		/// The <see cref="Data.UDTManager"/> object for this conglomerate.
		/// </summary>
		private readonly UDTManager udt_manager;

		/// <summary>
		/// The list of transactions that are currently open over this conglomerate.
		/// </summary>
		/// <remarks>
		/// This list is ordered from lowest commit_id to highest.  This object is
		/// shared with all the children MasterTableDataSource objects.
		/// </remarks>
		private readonly OpenTransactionList open_transactions;

		/// <summary>
		/// The list of all name space journals for the history of committed 
		/// transactions.
		/// </summary>
		private readonly ArrayList namespace_journal_list;

		// ---------- Table event listener ----------

		/// <summary>
		/// All listeners for modification events on tables in this conglomerate.
		/// </summary>
		/// <remarks>
		/// This is a mapping from TableName -> ArrayList of listeners.
		/// </remarks>
		private readonly Hashtable modification_listeners;




		// ---------- Locks ----------

		/// <summary>
		/// This Lock is obtained when we go to commit a change to the table.
		/// </summary>
		/// <remarks>
		/// Grabbing this lock ensures that no other commits can occur at the same
		/// time on this conglomerate.
		/// </remarks>
		internal readonly Object commit_lock = new Object();



		internal TableDataConglomerate(TransactionSystem system, IStoreSystem store_system) {
			this.system = system;
			this.store_system = store_system;
			open_transactions = new OpenTransactionList(system);
			modification_listeners = new Hashtable();
			namespace_journal_list = new ArrayList();

			sequence_manager = new SequenceManager(this);
			udt_manager = new UDTManager(this);

		}

		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> that this conglomerate is part of.
		/// </summary>
		public TransactionSystem System {
			get { return system; }
		}

		/// <summary>
		/// Returns the IStoreSystem used by this conglomerate to manage the persistent 
		/// state of the database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return store_system; }
		}

		/// <summary>
		/// Returns the SequenceManager object for this conglomerate.
		/// </summary>
		internal SequenceManager SequenceManager {
			get { return sequence_manager; }
		}

		internal UDTManager UDTManager {
			get { return udt_manager; }
		}


		/// <summary>
		/// Returns the BlobStore for this conglomerate.
		/// </summary>
		internal BlobStore BlobStore {
			get { return blob_store; }
		}

		/// <summary>
		/// Returns the name given to this conglomerate.
		/// </summary>
		internal string Name {
			get { return name; }
		}

		internal IDebugLogger Debug {
			get { return System.Debug; }
		}

		// ---------- Conglomerate state methods ----------

		/// <summary>
		/// Marks the given table id as committed dropped.
		/// </summary>
		/// <param name="table_id"></param>
		private void MarkAsCommittedDropped(int table_id) {
			MasterTableDataSource master_table = GetMasterTable(table_id);
			state_store.AddDeleteResource(new StateStore.StateResource(table_id, CreateEncodedTableFile(master_table)));
		}

		/// <summary>
		/// Loads the master table given the table_id and the name of the table
		/// resource in the database path.
		/// </summary>
		/// <param name="table_id"></param>
		/// <param name="table_str"></param>
		/// <param name="table_type"></param>
		/// <remarks>
		/// The <paramref name="table_str"/> string is a specially formatted string that we parse to 
		/// determine the file structure of the table.
		/// </remarks>
		/// <returns></returns>
		private MasterTableDataSource LoadMasterTable(int table_id, string table_str, int table_type) {
			// Open the table
			if (table_type == 1)
				throw new NotSupportedException();
			if (table_type == 2) {
				V2MasterTableDataSource master =
					new V2MasterTableDataSource(System,
						   StoreSystem, open_transactions, blob_store);
				if (master.Exists(table_str)) {
					return master;
				}
			}

			// If not exists, then generate an error message
			Debug.Write(DebugLevel.Error, this,
						  "Couldn't find table source - resource name: " +
						  table_str + " table_id: " + table_id);

			return null;
		}

		/// <summary>
		/// Returns a string that is an encoded table file name.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// An encoded table file name includes information about the table 
		/// type with the name of the table.
		/// </remarks>
		/// <example>
		/// <c>:2ThisTable</c> represents a <see cref="V2MasterTableDataSource"/> 
		/// table with file name <c>ThisTable</c>.
		/// </example>
		/// <returns></returns>
		private static String CreateEncodedTableFile(MasterTableDataSource table) {
			char type;
			/*
			TODO:
			if (table is V1MasterTableDataSource) {
				type = '1';
			} else 
			*/
			if (table is V2MasterTableDataSource) {
				type = '2';
			} else {
				throw new Exception("Unrecognised MasterTableDataSource class.");
			}
			StringBuilder buf = new StringBuilder();
			buf.Append(':');
			buf.Append(type);
			buf.Append(table.SourceIdentity);
			return buf.ToString();
		}

		/// <summary>
		/// Reads in the list of committed tables in this conglomerate.
		/// </summary>
		/// <remarks>
		/// This should only be called during an <see cref="Open"/> like method.
		/// This method fills the 'committed_tables' and 'table_list' lists 
		/// with the tables in this conglomerate.
		/// </remarks>
		private void ReadVisibleTables() {

			// The list of all visible tables from the state file
			StateStore.StateResource[] tables = state_store.GetVisibleList();
			// For each visible table
			for (int i = 0; i < tables.Length; ++i) {
				StateStore.StateResource resource = tables[i];

				int master_table_id = (int)resource.table_id;
				String file_name = resource.name;

				// Parse the file name string and determine the table type.
				int table_type = 1;
				if (file_name.StartsWith(":")) {
					if (file_name[1] == '1') {
						table_type = 1;
					} else if (file_name[1] == '2') {
						table_type = 2;
					} else {
						throw new Exception("Table type is not known.");
					}
					file_name = file_name.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master =
								 LoadMasterTable(master_table_id, file_name, table_type);

				if (master == null) {
					throw new ApplicationException("Table file for " + file_name + " was not found.");
				}

				/*
				TODO:
				if (master is V1MasterTableDataSource) {
					V1MasterTableDataSource v1_master = (V1MasterTableDataSource)master;
					v1_master.open(file_name);
				} else 
				*/
				if (master is V2MasterTableDataSource) {
					V2MasterTableDataSource v2_master = (V2MasterTableDataSource)master;
					v2_master.Open(file_name);
				} else {
					throw new ApplicationException("Unknown master table type: " + master.GetType());
				}

				// Add the table to the table list
				table_list.Add(master);

			}

		}

		/// <summary>
		/// Checks the list of committed tables in this conglomerate.
		/// </summary>
		/// <param name="terminal"></param>
		/// <remarks>
		/// This should only be called during an 'check' like method.  This method 
		/// fills the 'committed_tables' and 'table_list' lists with the tables in 
		/// this conglomerate.
		/// </remarks>
		public void CheckVisibleTables(IUserTerminal terminal) {

			// The list of all visible tables from the state file
			StateStore.StateResource[] tables = state_store.GetVisibleList();
			// For each visible table
			for (int i = 0; i < tables.Length; ++i) {
				StateStore.StateResource resource = tables[i];

				int master_table_id = (int)resource.table_id;
				String file_name = resource.name;

				// Parse the file name string and determine the table type.
				int table_type = 1;
				if (file_name.StartsWith(":")) {
					if (file_name[1] == '1') {
						table_type = 1;
					} else if (file_name[1] == '2') {
						table_type = 2;
					} else {
						throw new Exception("Table type is not known.");
					}
					file_name = file_name.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master =
								 LoadMasterTable(master_table_id, file_name, table_type);

				/*
				TODO:
				if (master is V1MasterTableDataSource) {
					V1MasterTableDataSource v1_master = (V1MasterTableDataSource)master;
					v1_master.CheckAndRepair(file_name, terminal);
				} else
				*/
				if (master is V2MasterTableDataSource) {
					V2MasterTableDataSource v2_master = (V2MasterTableDataSource)master;
					v2_master.CheckAndRepair(file_name, terminal);
				} else {
					throw new ApplicationException("Unknown master table type: " + master.GetType());
				}

				// Add the table to the table list
				table_list.Add(master);

				// Set a check point
				store_system.SetCheckPoint();

			}

		}








		/// <summary>
		/// Reads in the list of committed dropped tables on this conglomerate.
		/// </summary>
		/// <remarks>
		/// This should only be called during an 'open' like method. This method 
		/// fills the 'committed_dropped' and 'table_list' lists with the tables 
		/// in this conglomerate.
		/// </remarks>
		private void ReadDroppedTables() {

			// The list of all dropped tables from the state file
			StateStore.StateResource[] tables = state_store.GetDeleteList();
			// For each visible table
			for (int i = 0; i < tables.Length; ++i) {
				StateStore.StateResource resource = tables[i];

				int master_table_id = (int)resource.table_id;
				String file_name = resource.name;

				// Parse the file name string and determine the table type.
				int table_type = 1;
				if (file_name.StartsWith(":")) {
					if (file_name[1] == '1') {
						table_type = 1;
					} else if (file_name[1] == '2') {
						table_type = 2;
					} else {
						throw new Exception("Table type is not known.");
					}
					file_name = file_name.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master =
								 LoadMasterTable(master_table_id, file_name, table_type);

				// File wasn't found so remove from the delete resources
				if (master == null) {
					state_store.RemoveDeleteResource(resource.name);
				} else {
					/*
					TODO:
					if (master is V1MasterTableDataSource) {
						V1MasterTableDataSource v1_master = (V1MasterTableDataSource)master;
						v1_master.open(file_name);
					} else 
					*/
					if (master is V2MasterTableDataSource) {
						V2MasterTableDataSource v2_master = (V2MasterTableDataSource)master;
						v2_master.Open(file_name);
					} else {
						throw new ApplicationException("Unknown master table type: " + master.GetType());
					}

					// Add the table to the table list
					table_list.Add(master);
				}

			}

			// Commit any changes to the state store
			state_store.Commit();

		}

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
		///   <item>sUSRPKeyInfo</item>
		///   <description>Primary key constraint information.</description>
		///   <item>sUSRFKeyInfo</item>
		///   <description>Foreign key constraint information.</description>
		///   <item>sUSRUniqueInfo</item>
		///   <description>Unique set constraint information.</description>
		///   <item>sUSRCheckInfo</item>
		///   <description>Check constraint information.</description>
		///   <item>sUSRPrimaryColumns</item>
		///   <description>Primary columns information (refers to PKeyInfo)</description>
		///   <item>sUSRUniqueColumns</item>
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
		/// CREATE TABLE sUSRPKeyInfo (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the primary key constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		PRIMARY KEY (id),
		///		UNIQUE (schema, table)
		///	);
		///	
		/// CREATE TABLE sUSRFKeyInfo (
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
		/// CREATE TABLE sUSRUniqueInfo (
		///		id          NUMERIC NOT NULL,
		///		name        TEXT NOT NULL,  // The name of the unique constraint
		///		schema      TEXT NOT NULL,  // The name of the schema
		///		table       TEXT NOT NULL,  // The name of the table
		///		deferred    Bit  NOT NULL,  // Whether deferred or immediate
		///		
		///		PRIMARY KEY (id)
		///	);
		///	
		/// CREATE TABLE sUSRCheckInfo (
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
		/// CREATE TABLE sUSRPrimaryColumns (
		///		pk_id   NUMERIC NOT NULL, // The primary key constraint id
		///		column  TEXT NOT NULL,    // The name of the primary
		///		seq_no  INTEGER NOT NULL, // The sequence number of this constraint
		///		
		///		FOREIGN KEY pk_id REFERENCES pkey_info
		///	);
		///	
		/// CREATE TABLE sUSRUniqueColumns (
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
		/// CREATE TABLE sUSRSchemaInfo (
		///		id     NUMERIC NOT NULL,
		///		name   TEXT NOT NULL,
		///		type   TEXT,              // Schema type (system, etc)
		///		other  TEXT,
		///		
		///		UNIQUE ( name )
		///	);
		///	
		/// CREATE TABLE sUSRTableInfo (
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
		///		t_id    NUMERIC NOT NULL,  // Foreign key to sUSRTableInfo
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
			table.TableName = SYS_SEQUENCE_INFO;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("type"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = SYS_SEQUENCE;
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
			table.TableName = PRIMARY_INFO_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = FOREIGN_INFO_TABLE;
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
			table.TableName = UNIQUE_INFO_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("table"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("deferred"));
			transaction.AlterCreateTable(table, 187, 128);

			table = new DataTableDef();
			table.TableName = CHECK_INFO_TABLE;
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
			table.TableName = PRIMARY_COLS_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("pk_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("column"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = UNIQUE_COLS_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("un_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("column"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = FOREIGN_COLS_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("fk_id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("fcolumn"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("pcolumn"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("seq_no"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = SCHEMA_INFO_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("type"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("other"));
			transaction.AlterCreateTable(table, 91, 128);

			// Stores misc variables of the database,
			table = new DataTableDef();
			table.TableName = PERSISTENT_VAR_TABLE;
			table.AddColumn(DataTableColumnDef.CreateStringColumn("variable"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("value"));
			transaction.AlterCreateTable(table, 91, 128);

			// the UDT tables...
			table = new DataTableDef();
			table.TableName = UDT_TABLE;
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("id"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("attrs"));
			table.AddColumn(DataTableColumnDef.CreateNumericColumn("parent"));
			table.AddColumn(DataTableColumnDef.CreateStringColumn("ext_parent"));
			transaction.AlterCreateTable(table, 91, 128);

			table = new DataTableDef();
			table.TableName = UDT_COLS_TABLE;
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
		/// Given a table with a 'id' field, this will check that the sequence
		/// value for the table is at least greater than the maximum id in the column.
		/// </summary>
		/// <param name="tname"></param>
		internal void ResetTableID(TableName tname) {
			// Create the transaction
			Transaction transaction = CreateTransaction();
			// Get the table
			IMutableTableDataSource table = transaction.GetTable(tname);
			// Find the index of the column name called 'id'
			DataTableDef table_def = table.DataTableDef;
			int col_index = table_def.FindColumnName("id");
			if (col_index == -1) {
				throw new ApplicationException("Column name 'id' not found.");
			}
			// Find the maximum 'id' value.
			SelectableScheme scheme = table.GetColumnScheme(col_index);
			IntegerVector ivec = scheme.SelectLast();
			if (ivec.Count > 0) {
				TObject ob = table.GetCellContents(col_index, ivec[0]);
				BigNumber b_num = ob.ToBigNumber();
				if (b_num != null) {
					// Set the unique id to +1 the maximum id value in the column
					transaction.SetUniqueID(tname, b_num.ToInt64() + 1L);
				}
			}

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Debug.WriteException(e);
				throw new ApplicationException("Transaction Exception creating conglomerate.");
			}
		}

		/// <summary>
		/// Resets the table sequence id for all the system tables managed by 
		/// the conglomerate.
		/// </summary>
		internal void ResetAllSystemTableID() {
			ResetTableID(PRIMARY_INFO_TABLE);
			ResetTableID(FOREIGN_INFO_TABLE);
			ResetTableID(UNIQUE_INFO_TABLE);
			ResetTableID(CHECK_INFO_TABLE);
			ResetTableID(SCHEMA_INFO_TABLE);
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
			transaction.AddPrimaryKeyConstraint(PRIMARY_INFO_TABLE,
					  id_col, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_PK_PK");
			transaction.AddPrimaryKeyConstraint(FOREIGN_INFO_TABLE,
					  id_col, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_FK_PK");
			transaction.AddPrimaryKeyConstraint(UNIQUE_INFO_TABLE,
					  id_col, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_PK");
			transaction.AddPrimaryKeyConstraint(CHECK_INFO_TABLE,
					  id_col, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_PK");
			transaction.AddPrimaryKeyConstraint(SCHEMA_INFO_TABLE,
					  id_col, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_PK");

			// -- Foreign Keys --
			// Create the foreign key references,
			String[] fk_col = new String[1];
			String[] fk_ref_col = new String[] { "id" };
			fk_col[0] = "pk_id";
			transaction.AddForeignKeyConstraint(
					  PRIMARY_COLS_TABLE, fk_col, PRIMARY_INFO_TABLE, fk_ref_col,
					  ConstraintAction.NO_ACTION, ConstraintAction.NO_ACTION,
					  ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_PK_FK");
			fk_col[0] = "fk_id";
			transaction.AddForeignKeyConstraint(
					  FOREIGN_COLS_TABLE, fk_col, FOREIGN_INFO_TABLE, fk_ref_col,
					  ConstraintAction.NO_ACTION, ConstraintAction.NO_ACTION,
					  ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_FK_FK");
			fk_col[0] = "un_id";
			transaction.AddForeignKeyConstraint(
					  UNIQUE_COLS_TABLE, fk_col, UNIQUE_INFO_TABLE, fk_ref_col,
					  ConstraintAction.NO_ACTION, ConstraintAction.NO_ACTION,
					  ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_FK");

			// sUSRPKeyInfo 'schema', 'table' column is a unique set,
			// (You are only allowed one primary key per table).
			String[] columns = new String[] { "schema", "table" };
			transaction.AddUniqueConstraint(PRIMARY_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_ST_UNIQUE");
			// sUSRSchemaInfo 'name' column is a unique column,
			columns = new String[] { "name" };
			transaction.AddUniqueConstraint(SCHEMA_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_UNIQUE");
			//    columns = new String[] { "name" };
			columns = new String[] { "name", "schema" };
			// sUSRPKeyInfo 'name' column is a unique column,
			transaction.AddUniqueConstraint(PRIMARY_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_UNIQUE");
			// sUSRFKeyInfo 'name' column is a unique column,
			transaction.AddUniqueConstraint(FOREIGN_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_FKEY_UNIQUE");
			// sUSRUniqueInfo 'name' column is a unique column,
			transaction.AddUniqueConstraint(UNIQUE_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_UNIQUE");
			// sUSRCheckInfo 'name' column is a unique column,
			transaction.AddUniqueConstraint(CHECK_INFO_TABLE,
				 columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_UNIQUE");

			// sUSRDatabaseVars 'variable' is unique
			columns = new String[] { "variable" };
			transaction.AddUniqueConstraint(PERSISTENT_VAR_TABLE,
			   columns, ConstraintDeferrability.INITIALLY_IMMEDIATE, "SYSTEM_DATABASEVARS_UNIQUE");

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
				act_blob_store = StoreSystem.OpenStore("BlobStore");
			} else {
				act_blob_store = StoreSystem.CreateStore("BlobStore");
			}

			try {
				act_blob_store.LockForWrite();

				// Create the BlobStore object
				blob_store = new BlobStore(act_blob_store);

				// Get the 64 byte fixed area
				IMutableArea fixed_area = act_blob_store.GetMutableArea(-1);
				// If the blob store didn't exist then we need to create it here,
				if (!blob_store_exists) {
					long header_p = blob_store.Create();
					fixed_area.WriteInt8(header_p);
					fixed_area.CheckOut();
				} else {
					// Otherwise we need to initialize the blob store
					long header_p = fixed_area.ReadInt8();
					blob_store.Init(header_p);
				}
			} finally {
				act_blob_store.UnlockForWrite();
			}

		}



		// ---------- Private methods ----------

		/// <summary>
		/// Returns true if the system is in read-only mode.
		/// </summary>
		private bool IsReadOnly {
			get { return system.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the path of the database.
		/// </summary>
		private string Path {
			get { return system.DatabasePath; }
		}

		/// <summary>
		/// Returns the next unique table_id value for a new table and updates the
		/// conglomerate state information as appropriate.
		/// </summary>
		/// <returns></returns>
		private int NextUniqueTableID() {
			return state_store.NextTableId();
		}


		/// <summary>
		/// Sets up the internal state of this object.
		/// </summary>
		private void SetupInternal() {
			commit_id = 0;
			table_list = new ArrayList();
		}

		// ---------- Public methods ----------

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
			act_state_store = StoreSystem.CreateStore(name + STATE_POST);
			try {
				act_state_store.LockForWrite();

				state_store = new StateStore(act_state_store);
				long head_p = state_store.Create();
				// Get the fixed area
				IMutableArea fixed_area = act_state_store.GetMutableArea(-1);
				fixed_area.WriteInt8(head_p);
				fixed_area.CheckOut();
			} finally {
				act_state_store.UnlockForWrite();
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
			state_store.Commit();

		}

		/// <summary>
		/// Opens a conglomerate.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// Once a conglomerate is open, we may start opening transactions and 
		/// altering the data within it.
		/// </remarks>
		/// <exception cref="IOException">
		/// If the conglomerate does not exist.  
		/// </exception>
		public void Open(String name) {
			this.name = name;

			if (!Exists(name)) {
				throw new IOException("Conglomerate doesn't exists: " + name);
			}

			// Check the file Lock
			if (!IsReadOnly) {
				// Obtain the Lock (generate error if this is not possible)
				StoreSystem.Lock(name);
			}

			// Open the state store
			act_state_store = StoreSystem.OpenStore(name + STATE_POST);
			state_store = new StateStore(act_state_store);
			// Get the fixed 64 byte area.
			IArea fixed_area = act_state_store.GetArea(-1);
			long head_p = fixed_area.ReadInt8();
			state_store.init(head_p);

			SetupInternal();

			// Init the conglomerate blob store
			InitializeBlobStore();

			ReadVisibleTables();
			ReadDroppedTables();

			// We possibly have things to clean up if there are deleted columns.
			CleanUpConglomerate();

		}

		/// <summary>
		/// Closes this conglomerate.
		/// </summary>
		/// <remarks>
		/// The conglomerate must be open for it to be closed. When closed, 
		/// any use of this object is undefined.
		/// </remarks>
		public void Close() {
			lock (commit_lock) {

				// We possibly have things to clean up.
				CleanUpConglomerate();

				// Set a check point
				store_system.SetCheckPoint();

				// Go through and close all the committed tables.
				int size = table_list.Count;
				for (int i = 0; i < size; ++i) {
					MasterTableDataSource master =
												(MasterTableDataSource)table_list[i];
					master.Dispose(false);
				}

				state_store.Commit();
				StoreSystem.CloseStore(act_state_store);

				table_list = null;

			}

			// Unlock the storage system
			StoreSystem.Unlock(name);

			if (blob_store != null) {
				StoreSystem.CloseStore(act_blob_store);
			}

			//    removeShutdownHook();
		}

		/// <summary>
		/// Deletes and closes the conglomerate.
		/// </summary>
		/// <remarks>
		/// This will delete all the files in the file system associated with 
		/// this conglomerate, so this method should be used with care.
		/// <para>
		/// <b>Warning</b> Will result in total loss of all data stored in the 
		/// conglomerate.
		/// </para>
		/// </remarks>
		public void Delete() {
			lock (commit_lock) {

				// We possibly have things to clean up.
				CleanUpConglomerate();

				// Go through and delete and close all the committed tables.
				int size = table_list.Count;
				for (int i = 0; i < size; ++i) {
					MasterTableDataSource master =
												(MasterTableDataSource)table_list[i];
					master.Drop();
				}

				// Delete the state file
				state_store.Commit();
				StoreSystem.CloseStore(act_state_store);
				StoreSystem.DeleteStore(act_state_store);

				// Delete the blob store
				if (blob_store != null) {
					StoreSystem.CloseStore(act_blob_store);
					StoreSystem.DeleteStore(act_blob_store);
				}

				// Invalidate this object
				table_list = null;

			}

			// Unlock the storage system.
			StoreSystem.Unlock(name);
		}

		/// <summary>
		/// Returns true if the conglomerate is closed.
		/// </summary>
		public bool IsClosed {
			get {
				lock (commit_lock) {
					return table_list == null;
				}
			}
		}


		/// <summary>
		/// Returns true if the conglomerate exists in the file system and can
		/// be opened.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public bool Exists(String name) {
			return StoreSystem.StoreExists(name + STATE_POST);
		}

		/// <summary>
		/// Makes a complete copy of this database to the position represented 
		/// by the given TableDataConglomerate object.
		/// </summary>
		/// <param name="dest_conglomerate"></param>
		/// <remarks>
		/// The given TableDataConglomerate object must <b>not</b> be being 
		/// used by another database running in the environment. This may take 
		/// a while to complete. The backup operation occurs within its own 
		/// transaction and the copy transaction is read-only meaning there 
		/// is no way for the copy process to interfere with other transactions 
		/// running concurrently.
		/// <para>
		/// The conglomerate must be open before this method is called.
		/// </para>
		/// </remarks>
		public void LiveCopyTo(TableDataConglomerate dest_conglomerate) {

			// The destination store system
			IStoreSystem dest_store_system = dest_conglomerate.StoreSystem;

			// Copy all the blob data from the given blob store to the current blob
			// store.
			dest_conglomerate.blob_store.CopyFrom(dest_store_system, blob_store);

			// Open new transaction - this is the current view we are going to copy.
			Transaction transaction = CreateTransaction();

			try {

				// Copy the data in this transaction to the given destination store system.
				transaction.liveCopyAllDataTo(dest_conglomerate);

			} finally {
				// Make sure we close the transaction
				try {
					transaction.Commit();
				} catch (TransactionException e) {
					throw new Exception("Transaction Error: " + e.Message);
				}
			}

			// Finished - increment the live copies counter.
			System.Stats.Increment("TableDataConglomerate.liveCopies");

		}

		// ---------- Diagnostic and repair ----------

		/// <summary>
		/// Returns a RawDiagnosticTable object that is used for diagnostics of 
		/// the table with the given file name.
		/// </summary>
		/// <param name="table_file_name"></param>
		/// <returns></returns>
		public IRawDiagnosticTable GetDiagnosticTable(String table_file_name) {
			lock (commit_lock) {
				for (int i = 0; i < table_list.Count; ++i) {
					MasterTableDataSource master =
												(MasterTableDataSource)table_list[i];
					if (master.SourceIdentity.Equals(table_file_name)) {
						return master.GetRawDiagnosticTable();
					}
				}
			}
			return null;
		}

		///<summary>
		/// Returns the list of file names for all tables in this conglomerate.
		///</summary>
		///<returns></returns>
		public String[] GetAllTableFileNames() {
			lock (commit_lock) {
				String[] list = new String[table_list.Count];
				for (int i = 0; i < table_list.Count; ++i) {
					MasterTableDataSource master =
												(MasterTableDataSource)table_list[i];
					list[i] = master.SourceIdentity;
				}
				return list;
			}
		}

		// ---------- Conglomerate event notification ----------

		///<summary>
		/// Adds a listener for transactional modification events that occur on 
		/// the given table in this conglomerate.
		///</summary>
		///<param name="table_name">The name of the table in the conglomerate to 
		/// listen for events from.</param>
		///<param name="listener">The listener to be notified of events.</param>
		/// <remarks>
		/// A transactional modification event is an event fired immediately upon the 
		/// modification of a table by a transaction, either immediately before the 
		/// modification or immediately after.  Also an event is fired when a modification to 
		/// a table is successfully committed.
		/// <para>
		/// The BEFORE_* type triggers are given the opportunity to modify the contents 
		/// of the DataRow before the update or insert occurs.  All triggers may generate 
		/// an exception which will cause the transaction to rollback.
		/// </para>
		/// <para>
		/// The event carries with it the event type, the transaction that the event
		/// occurred in, and any information regarding the modification itself.
		/// </para>
		/// <para>
		/// This event/listener mechanism is intended to be used to implement higher
		/// layer database triggering systems.  Note that care must be taken with
		/// the commit level events because they occur inside a commit Lock on this
		/// conglomerate and so synchronization and deadlock issues need to be
		/// carefully considered.
		/// </para>
		/// <para>
		/// <b>Note</b>: A listener on the given table will be notified of ALL table
		/// modification events by all transactions at the time they happen.
		/// </para>
		/// </remarks>
		public void AddTransactionModificationListener(TableName table_name,
										 TransactionModificationListener listener) {
			lock (modification_listeners) {
				ArrayList list = (ArrayList)modification_listeners[table_name];
				if (list == null) {
					// If the mapping doesn't exist then create the list for the table
					// here.
					list = new ArrayList();
					modification_listeners[table_name] = list;
				}

				list.Add(listener);
			}
		}

		/// <summary>
		/// Removes a listener for transaction modification events on the given table in 
		/// this conglomerate as previously set by the <see cref="AddTransactionModificationListener"/> 
		/// method.
		/// </summary>
		/// <param name="table_name">The name of the table in the conglomerate to remove 
		/// from the listener list.</param>
		/// <param name="listener">The listener to be removed.</param>
		public void RemoveTransactionModificationListener(TableName table_name,
										 TransactionModificationListener listener) {
			lock (modification_listeners) {
				ArrayList list = (ArrayList)modification_listeners[table_name];
				if (list != null) {
					int sz = list.Count;
					for (int i = sz - 1; i >= 0; --i) {
						if (list[i] == listener) {
							list.RemoveAt(i);
						}
					}
				}
			}
		}

		// ---------- Transactional management ----------

		/// <summary>
		/// Starts a new transaction.
		/// </summary>
		/// <remarks>
		/// The <see cref="Transaction"/> object returned by this method is 
		/// used to read the contents of the database at the time the transaction 
		/// was started. It is also used if any modifications are required to 
		/// be made.
		/// </remarks>
		/// <returns></returns>
		public Transaction CreateTransaction() {
			long this_commit_id;
			ArrayList this_committed_tables = new ArrayList();

			// Don't let a commit happen while we are looking at this.
			lock (commit_lock) {

				this_commit_id = commit_id;
				StateStore.StateResource[] committed_table_list = state_store.GetVisibleList();
				for (int i = 0; i < committed_table_list.Length; ++i) {
					this_committed_tables.Add(
								  GetMasterTable((int)committed_table_list[i].table_id));
				}

				// Create a set of IIndexSet for all the tables in this transaction.
				int sz = this_committed_tables.Count;
				ArrayList index_info = new ArrayList(sz);
				for (int i = 0; i < sz; ++i) {
					MasterTableDataSource mtable =
								   (MasterTableDataSource)this_committed_tables[i];
					index_info.Add(mtable.CreateIndexSet());
				}

				// Create the transaction and record it in the open transactions list.
				Transaction t = new Transaction(this,
								  this_commit_id, this_committed_tables, index_info);
				open_transactions.AddTransaction(t);
				return t;

			}

		}

		/// <summary>
		/// This is called to notify the conglomerate that the transaction has
		/// closed.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is always called from either the rollback or commit method
		/// of the transaction object.
		/// <para>
		/// <b>Note</b> This increments 'commit_id' and requires that the 
		/// conglomerate is commit locked.
		/// </para>
		/// </remarks>
		private void CloseTransaction(Transaction transaction) {
			bool last_transaction = false;
			// Closing must happen under a commit Lock.
			lock (commit_lock) {
				open_transactions.RemoveTransaction(transaction);
				// Increment the commit id.
				++commit_id;
				// Was that the last transaction?
				last_transaction = open_transactions.Count == 0;
			}

			// If last transaction then schedule a clean up event.
			if (last_transaction) {
				try {
					CleanUpConglomerate();
				} catch (IOException e) {
					Debug.Write(DebugLevel.Error, this, "Error cleaning up conglomerate");
					Debug.WriteException(DebugLevel.Error, e);
				}
			}

		}


		/// <summary>
		/// Closes and drops the <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="table_file_name"></param>
		/// <remarks>
		/// This should only be called from the 
		/// <see cref="CleanUpConglomerate">clean up method</see>.
		/// <para>
		/// A drop may fail if, for example, the roots of the table are locked.
		/// </para>
		/// <para>
		/// Note that the table_file_name will be encoded with the table type.  
		/// For example, ":2mighty.mds"
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the drop succeeded.
		/// </returns>
		private bool CloseAndDropTable(String table_file_name) {
			// Find the table with this file name.
			for (int i = 0; i < table_list.Count; ++i) {
				MasterTableDataSource t = (MasterTableDataSource)table_list[i];
				String enc_fn = table_file_name.Substring(2);
				if (t.SourceIdentity.Equals(enc_fn)) {
					// Close and remove from the list.
					if (t.IsRootLocked) {
						// We can't drop a table that has roots locked..
						return false;
					}

					// This drops if the table has been marked as being dropped.
					bool b = t.Drop();
					if (b) {
						table_list.RemoveAt(i);
					}
					return b;
				}
			}
			return false;
		}

		/// <summary>
		/// Closes the MasterTableDataSource with the given source ident.
		/// </summary>
		/// <param name="table_file_name"></param>
		/// <param name="pending_drop"></param>
		/// <remarks>
		/// This should only be called from the 
		/// <see cref="CleanUpConglomerate">clean up method</see>.
		/// <para>
		/// Note that the table_file_name will be encoded with the table type.  
		/// For example, ":2mighty.mds"
		/// </para>
		/// </remarks>
		private void CloseTable(String table_file_name, bool pending_drop) {
			// Find the table with this file name.
			for (int i = 0; i < table_list.Count; ++i) {
				MasterTableDataSource t = (MasterTableDataSource)table_list[i];
				String enc_fn = table_file_name.Substring(2);
				if (t.SourceIdentity.Equals(enc_fn)) {
					// Close and remove from the list.
					if (t.IsRootLocked) {
						// We can't drop a table that has roots locked..
						return;
					}

					// This closes the table
					t.Dispose(pending_drop);
					return;
				}
			}
			return;
		}

		/// <summary>
		/// Cleans up the conglomerate by deleting all tables marked as deleted.
		/// </summary>
		/// <remarks>
		/// This should be called when the conglomerate is opened, shutdown and
		/// when there are no transactions open.
		/// </remarks>
		private void CleanUpConglomerate() {
			lock (commit_lock) {
				if (IsClosed) {
					return;
				}

				// If no open transactions on the database, then clean up.
				if (open_transactions.Count == 0) {

					StateStore.StateResource[] delete_list = state_store.GetDeleteList();
					if (delete_list.Length > 0) {
						int drop_count = 0;

						for (int i = delete_list.Length - 1; i >= 0; --i) {
							String fn = (String)delete_list[i].name;
							CloseTable(fn, true);
						}

						//          // NASTY HACK: The native win32 file mapping will not
						//          //   let you delete a file that is mapped.  The NIO API does not allow
						//          //   you to manually unmap a file, and the only way to unmap
						//          //   memory under win32 is to wait for the garbage collector to
						//          //   free it.  So this is a hack to try and make the engine
						//          //   unmap the memory mapped buffer.
						//          //
						//          //   This is not a problem under Unix/Linux because the OS has no
						//          //   difficulty deleting a file that is mapped.
						//
						//          System.gc();
						//          try {
						//            Thread.sleep(5);
						//          }
						//          catch (InterruptedException e) { /* ignore */ }

						for (int i = delete_list.Length - 1; i >= 0; --i) {
							String fn = (String)delete_list[i].name;
							bool dropped = CloseAndDropTable(fn);
							// If we managed to drop the table, remove from the list.
							if (dropped) {
								state_store.RemoveDeleteResource(fn);
								++drop_count;
							}
						}

						// If we dropped a table, commit an update to the conglomerate state.
						if (drop_count > 0) {
							state_store.Commit();
						}
					}

				}
			}
		}

		// ---------- Detection of constraint violations ----------

		/// <summary>
		/// A variable resolver for a single row of a table source.
		/// </summary>
		/// <remarks>
		/// Used when evaluating a check constraint for newly added row.
		/// </remarks>
		private sealed class TableRowVariableResolver : IVariableResolver {

			private ITableDataSource table;
			private int row_index = -1;

			public TableRowVariableResolver(ITableDataSource table, int row) {
				this.table = table;
				this.row_index = row;
			}

			private int findColumnName(VariableName variable) {
				int col_index = table.DataTableDef.FindColumnName(
																  variable.Name);
				if (col_index == -1) {
					throw new ApplicationException("Can't find column: " + variable);
				}
				return col_index;
			}

			// --- Implemented ---

			public int SetId {
				get { return row_index; }
			}

			public TObject Resolve(VariableName variable) {
				int col_index = findColumnName(variable);
				return table.GetCellContents(col_index, row_index);
			}

			public TType ReturnTType(VariableName variable) {
				int col_index = findColumnName(variable);
				return table.DataTableDef[col_index].TType;
			}

		}

		/// <summary>
		/// Converts a String[] array to a comma deliminated string list.
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		internal static String StringColumnList(String[] list) {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < list.Length - 1; ++i) {
				buf.Append(list[i]);
			}
			buf.Append(list[list.Length - 1]);
			return buf.ToString();
		}

		/// <summary>
		/// Returns either 'Immediate' or 'Deferred' dependant on the deferred short.
		/// </summary>
		/// <param name="deferred"></param>
		/// <returns></returns>
		internal static String DeferredString(ConstraintDeferrability deferred) {
			switch (deferred) {
				case (ConstraintDeferrability.INITIALLY_IMMEDIATE):
					return "Immediate";
				case (ConstraintDeferrability.INITIALLY_DEFERRED):
					return "Deferred";
				default:
					throw new ApplicationException("Unknown deferred string.");
			}
		}

		/// <summary>
		/// Returns a list of column indices into the given <see cref="DataTableDef"/>
		/// for the given column names.
		/// </summary>
		/// <param name="table_def"></param>
		/// <param name="cols"></param>
		/// <returns></returns>
		internal static int[] FindColumnIndices(DataTableDef table_def, String[] cols) {
			// Resolve the list of column names to column indexes
			int[] col_indexes = new int[cols.Length];
			for (int i = 0; i < cols.Length; ++i) {
				col_indexes[i] = table_def.FindColumnName(cols[i]);
			}
			return col_indexes;
		}

		/// <summary>
		/// Checks the uniqueness of the columns in the row of the table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="rindex"></param>
		/// <param name="cols"></param>
		/// <param name="nulls_are_allowed"></param>
		/// <remarks>
		/// If the given column information in the row data is not unique then 
		/// it returns false. We also check for a NULL values - a PRIMARY KEY 
		/// constraint does not allow NULL values, whereas a UNIQUE constraint 
		/// does.
		/// </remarks>
		/// <returns></returns>
		private static bool IsUniqueColumns(
							 ITableDataSource table, int rindex, String[] cols,
							 bool nulls_are_allowed) {

			DataTableDef table_def = table.DataTableDef;
			// 'identical_rows' keeps a tally of the rows that match our added cell.
			IntegerVector identical_rows = null;

			// Resolve the list of column names to column indexes
			int[] col_indexes = FindColumnIndices(table_def, cols);

			// If the value being tested for uniqueness contains NULL, we return true
			// if nulls are allowed.
			for (int i = 0; i < col_indexes.Length; ++i) {
				TObject cell = table.GetCellContents(col_indexes[i], rindex);
				if (cell.IsNull) {
					return nulls_are_allowed;
				}
			}


			for (int i = 0; i < col_indexes.Length; ++i) {

				int col_index = col_indexes[i];

				// Get the column definition and the cell being inserted,
				//      DataTableColumnDef column_def = table_def.columnAt(col_index);
				TObject cell = table.GetCellContents(col_index, rindex);

				// We are assured of uniqueness if 'identical_rows != null &&
				// identical_rows.size() == 0'  This is because 'identical_rows' keeps
				// a running tally of the rows in the table that contain unique columns
				// whose cells match the record being added.

				if (identical_rows == null || identical_rows.Count > 0) {

					// Ask SelectableScheme to return pointers to row(s) if there is
					// already a cell identical to this in the table.

					SelectableScheme ss = table.GetColumnScheme(col_index);
					IntegerVector ivec = ss.SelectEqual(cell);

					// If 'identical_rows' hasn't been set up yet then set it to 'ivec'
					// (the list of rows where there is a cell which is equal to the one
					//  being added)
					// If 'identical_rows' has been set up, then perform an
					// 'intersection' operation on the two lists (only keep the numbers
					// that are repeated in both lists).  Therefore we keep the rows
					// that match the row being added.

					if (identical_rows == null) {
						identical_rows = ivec;
					} else {
						ivec.QuickSort();
						int row_index = identical_rows.Count - 1;
						while (row_index >= 0) {
							int val = identical_rows[row_index];
							int found_index = ivec.SortedIndexOf(val);
							// If we _didn't_ find the index in the array
							if (found_index >= ivec.Count ||
								ivec[found_index] != val) {
								identical_rows.RemoveIntAt(row_index);
							}
							--row_index;
						}
					}

				}

			} // for each column

			// If there is 1 (the row we added) then we are unique, otherwise we are
			// not.
			if (identical_rows != null) {
				int sz = identical_rows.Count;
				if (sz == 1) {
					return true;
				}
				if (sz > 1) {
					return false;
				} else if (sz == 0) {
					throw new ApplicationException("Assertion failed: We must be able to find the " +
									"row we are testing uniqueness against!");
				}
			}
			return true;

		}


		/// <summary>
		/// Returns the key indices found in the given table.
		/// </summary>
		/// <param name="t2"></param>
		/// <param name="col2_indexes"></param>
		/// <param name="key_value"></param>
		/// <remarks>
		/// The keys are in the given column indices, and the key is in the 
		/// 'key' array. This can be used to count the number of keys found 
		/// in a table for constraint violation checking.
		/// </remarks>
		/// <returns></returns>
		internal static IntegerVector FindKeys(ITableDataSource t2, int[] col2_indexes,
									  TObject[] key_value) {

			int key_size = key_value.Length;
			// Now command table 2 to determine if the key values are present.
			// Use index scan on first key.
			SelectableScheme ss = t2.GetColumnScheme(col2_indexes[0]);
			IntegerVector list = ss.SelectEqual(key_value[0]);
			if (key_size > 1) {
				// Full scan for the rest of the columns
				int sz = list.Count;
				// For each element of the list
				for (int i = sz - 1; i >= 0; --i) {
					int r_index = list[i];
					// For each key in the column list
					for (int c = 1; c < key_size; ++c) {
						int col_index = col2_indexes[c];
						TObject c_value = key_value[c];
						if (c_value.CompareTo(t2.GetCellContents(col_index, r_index)) != 0) {
							// If any values in the key are not equal set this flag to false
							// and remove the index from the list.
							list.RemoveIntAt(i);
							// Break the for loop
							break;
						}
					}
				}
			}

			return list;
		}

		/// <summary>
		/// Finds the number of rows that are referenced between the given 
		/// row of <paramref name="table1"/> and that match <paramref name="table2"/>.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="row_index"></param>
		/// <param name="table1"></param>
		/// <param name="cols1"></param>
		/// <param name="table2"></param>
		/// <param name="cols2"></param>
		/// <param name="check_source_table_key"></param>
		/// <remarks>
		/// This method is used to determine if there are referential links.
		/// <para>
		/// If this method returns -1 it means the value being searched for is 
		/// <c>NULL</c> therefore we can't determine if there are any referenced 
		/// links.
		/// </para>
		/// <para>
		/// <b>Hack</b>: If <paramref name="check_source_table_key"/> is set then the 
		/// key is checked for in the source table and if it exists returns 0. 
		/// Otherwise it looks for references to the key in table2.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private static int RowCountOfReferenceTable(
					   SimpleTransaction transaction,
					   int row_index, TableName table1, String[] cols1,
									  TableName table2, String[] cols2,
									  bool check_source_table_key) {

			// Get the tables
			ITableDataSource t1 = transaction.GetTableDataSource(table1);
			ITableDataSource t2 = transaction.GetTableDataSource(table2);
			// The table defs
			DataTableDef dtd1 = t1.DataTableDef;
			DataTableDef dtd2 = t2.DataTableDef;
			// Resolve the list of column names to column indexes
			int[] col1_indexes = FindColumnIndices(dtd1, cols1);
			int[] col2_indexes = FindColumnIndices(dtd2, cols2);

			int key_size = col1_indexes.Length;
			// Get the data from table1
			TObject[] key_value = new TObject[key_size];
			int null_count = 0;
			for (int n = 0; n < key_size; ++n) {
				key_value[n] = t1.GetCellContents(col1_indexes[n], row_index);
				if (key_value[n].IsNull) {
					++null_count;
				}
			}

			// If we are searching for null then return -1;
			if (null_count > 0) {
				return -1;
			}

			// HACK: This is a hack.  The purpose is if the key exists in the source
			//   table we return 0 indicating to the delete check that there are no
			//   references and it's valid.  To the semantics of the method this is
			//   incorrect.
			if (check_source_table_key) {
				IntegerVector keys = FindKeys(t1, col1_indexes, key_value);
				int key_count = keys.Count;
				if (key_count > 0) {
					return 0;
				}
			}

			return FindKeys(t2, col2_indexes, key_value).Count;
		}


		/// <summary>
		/// Checks that the nullibility and class of the fields in the given
		/// rows are valid.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <param name="row_indices"></param>
		/// <remarks>
		/// Should be used as part of the insert procedure.
		/// </remarks>
		internal static void CheckFieldConstraintViolations(
										SimpleTransaction transaction,
										ITableDataSource table, int[] row_indices) {

			// Quick exit case
			if (row_indices == null || row_indices.Length == 0) {
				return;
			}

			// Check for any bad cells - which are either cells that are 'null' in a
			// column declared as 'not null', or duplicated in a column declared as
			// unique.

			DataTableDef table_def = table.DataTableDef;
			TableName table_name = table_def.TableName;

			// Check not-null columns are not null.  If they are null, throw an
			// error.  Additionally check that OBJECT columns are correctly
			// typed.

			// Check each field of the added rows
			int len = table_def.ColumnCount;
			for (int i = 0; i < len; ++i) {

				// Get the column definition and the cell being inserted,
				DataTableColumnDef column_def = table_def[i];
				// For each row added to this column
				for (int rn = 0; rn < row_indices.Length; ++rn) {
					TObject cell = table.GetCellContents(i, row_indices[rn]);

					// Check: Column defined as not null and cell being inserted is
					// not null.
					if (column_def.IsNotNull && cell.IsNull) {
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.NullableViolation,
							"You tried to add 'null' cell to column '" +
							table_def[i].Name +
							"' which is declared as 'not_null'");
					}

					// Check: If column is an object, then deserialize and check the
					//        object is an instance of the class constraint,
					if (!cell.IsNull &&
						column_def.SqlType == SqlType.Object) {
						String class_constraint = column_def.TypeConstraintString;
						// Everything is derived from System.Object so this optimization
						// will not cause an object deserialization.
						if (!class_constraint.Equals("System.Object")) {
							// Get the binary representation of the object
							ByteLongObject serialized_jobject = (ByteLongObject)cell.Object;
							// Deserialize the object
							Object ob = ObjectTranslator.Deserialize(serialized_jobject);
							// Check it's assignable from the constraining class
							if (!ob.GetType().IsAssignableFrom(
										column_def.TypeConstraint)) {
								throw new DatabaseConstraintViolationException(
								  DatabaseConstraintViolationException.ObjectTypeViolation,
								  "The object being inserted is not derived from the " +
								  "class constraint defined for the column (" +
								  class_constraint + ")");
							}
						}
					}

				} // For each row being added

			} // for each column

		}

		/// <summary>
		/// Performs constraint violation checks on an addition of the given 
		/// set of row indices into the <see cref="ITableDataSource"/> in the
		/// given transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance 
		/// used to determine table  constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="row_indices">The list of rows that were added to the 
		/// table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.INITIALLY_DEFERRED"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckAddConstraintViolations(
				 SimpleTransaction transaction,
				 ITableDataSource table, int[] row_indices, ConstraintDeferrability deferred) {

			String cur_schema = table.DataTableDef.Schema;
			IQueryContext context = new SystemQueryContext(transaction, cur_schema);

			// Quick exit case
			if (row_indices == null || row_indices.Length == 0) {
				return;
			}

			DataTableDef table_def = table.DataTableDef;
			TableName table_name = table_def.TableName;

			// ---- Constraint checking ----

			// Check any primary key constraint.
			Transaction.ColumnGroup primary_key =
					   Transaction.QueryTablePrimaryKeyGroup(transaction, table_name);
			if (primary_key != null &&
				(deferred == ConstraintDeferrability.INITIALLY_DEFERRED ||
				 primary_key.deferred == ConstraintDeferrability.INITIALLY_IMMEDIATE)) {

				// For each row added to this column
				for (int rn = 0; rn < row_indices.Length; ++rn) {
					if (!IsUniqueColumns(table, row_indices[rn],
										 primary_key.columns, false)) {
						throw new DatabaseConstraintViolationException(
						  DatabaseConstraintViolationException.PrimaryKeyViolation,
						  DeferredString(deferred) + " primary Key constraint violation (" +
						  primary_key.name + ") Columns = ( " +
						  StringColumnList(primary_key.columns) +
						  " ) Table = ( " + table_name.ToString() + " )");
					}
				} // For each row being added

			}

			// Check any unique constraints.
			Transaction.ColumnGroup[] unique_constraints =
						  Transaction.QueryTableUniqueGroups(transaction, table_name);
			for (int i = 0; i < unique_constraints.Length; ++i) {
				Transaction.ColumnGroup unique = unique_constraints[i];
				if (deferred == ConstraintDeferrability.INITIALLY_DEFERRED ||
					unique.deferred == ConstraintDeferrability.INITIALLY_IMMEDIATE) {

					// For each row added to this column
					for (int rn = 0; rn < row_indices.Length; ++rn) {
						if (!IsUniqueColumns(table, row_indices[rn], unique.columns, true)) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.UniqueViolation,
							  DeferredString(deferred) + " unique constraint violation (" +
							  unique.name + ") Columns = ( " +
							  StringColumnList(unique.columns) + " ) Table = ( " +
							  table_name.ToString() + " )");
						}
					} // For each row being added

				}
			}

			// Check any foreign key constraints.
			// This ensures all foreign references in the table are referenced
			// to valid records.
			Transaction.ColumnGroupReference[] foreign_constraints =
				  Transaction.QueryTableForeignKeyReferences(transaction, table_name);
			for (int i = 0; i < foreign_constraints.Length; ++i) {
				Transaction.ColumnGroupReference reference = foreign_constraints[i];
				if (deferred == ConstraintDeferrability.INITIALLY_DEFERRED ||
					reference.deferred == ConstraintDeferrability.INITIALLY_IMMEDIATE) {
					// For each row added to this column
					for (int rn = 0; rn < row_indices.Length; ++rn) {
						// Make sure the referenced record exists

						// Return the count of records where the given row of
						//   table_name(columns, ...) IN
						//                    ref_table_name(ref_columns, ...)
						int row_count = RowCountOfReferenceTable(transaction,
												   row_indices[rn],
												   reference.key_table_name, reference.key_columns,
												   reference.ref_table_name, reference.ref_columns,
												   false);
						if (row_count == -1) {
							// foreign key is NULL
						}
						if (row_count == 0) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.ForeignKeyViolation,
							  DeferredString(deferred) + " foreign key constraint violation (" +
							  reference.name + ") Columns = " +
							  reference.key_table_name.ToString() + "( " +
							  StringColumnList(reference.key_columns) + " ) -> " +
							  reference.ref_table_name.ToString() + "( " +
							  StringColumnList(reference.ref_columns) + " )");
						}
					} // For each row being added.
				}
			}

			// Any general checks of the inserted data
			Transaction.CheckExpression[] check_constraints =
					   Transaction.QueryTableCheckExpressions(transaction, table_name);

			// The TransactionSystem object
			TransactionSystem system = transaction.System;

			// For each check constraint, check that it evaluates to true.
			for (int i = 0; i < check_constraints.Length; ++i) {
				Transaction.CheckExpression check = check_constraints[i];
				if (deferred == ConstraintDeferrability.INITIALLY_DEFERRED ||
					check.deferred == ConstraintDeferrability.INITIALLY_IMMEDIATE) {

					check = system.PrepareTransactionCheckConstraint(table_def, check);
					Expression exp = check.expression;

					// For each row being added to this column
					for (int rn = 0; rn < row_indices.Length; ++rn) {
						TableRowVariableResolver resolver =
									  new TableRowVariableResolver(table, row_indices[rn]);
						TObject ob = exp.Evaluate(null, resolver, context);
						bool isNull;
						bool b = ob.ToBoolean(out isNull);

						if (!isNull) {
							if (!b) {
								// Evaluated to false so don't allow this row to be added.
								throw new DatabaseConstraintViolationException(
								   DatabaseConstraintViolationException.CheckViolation,
								   DeferredString(deferred) + " check constraint violation (" +
								   check.name + ") - '" + exp.Text +
								   "' evaluated to false for inserted/updated row.");
							}
						} else {
							// NOTE: This error will pass the row by default
							transaction.Debug.Write(DebugLevel.Error,
							            typeof (TableDataConglomerate),
							            DeferredString(deferred) + " check constraint violation (" +
							            check.name + ") - '" + exp.Text +
							            "' returned a non boolean or NULL result.");
						}
					} // For each row being added
				}
			}
		}

		/// <summary>
		/// Performs constraint violation checks on an addition of the given 
		/// set of row indices into the <see cref="ITableDataSource"/> in the 
		/// given transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance 
		/// used to determine table  constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="row_index">The row that was added to the table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.INITIALLY_DEFERRED"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckAddConstraintViolations(
				   SimpleTransaction transaction,
				   ITableDataSource table, int row_index, ConstraintDeferrability deferred) {
			CheckAddConstraintViolations(transaction, table,
										 new int[] { row_index }, deferred);
		}

		/// <summary>
		/// Performs constraint violation checks on a removal of the given set 
		/// of row indexes from the <see cref="ITableDataSource"/> in the given 
		/// transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance used 
		/// to determine table constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="row_indices">The list of rows that were removed from 
		/// the table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.INITIALLY_DEFERRED"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckRemoveConstraintViolations(
				 SimpleTransaction transaction, ITableDataSource table,
				 int[] row_indices, ConstraintDeferrability deferred) {

			// Quick exit case
			if (row_indices == null || row_indices.Length == 0) {
				return;
			}

			DataTableDef table_def = table.DataTableDef;
			TableName table_name = table_def.TableName;

			// Check any imported foreign key constraints.
			// This ensures that a referential reference can not be removed making
			// it invalid.
			Transaction.ColumnGroupReference[] foreign_constraints =
				   Transaction.QueryTableImportedForeignKeyReferences(
															 transaction, table_name);
			for (int i = 0; i < foreign_constraints.Length; ++i) {
				Transaction.ColumnGroupReference reference = foreign_constraints[i];
				if (deferred == ConstraintDeferrability.INITIALLY_DEFERRED ||
					reference.deferred == ConstraintDeferrability.INITIALLY_IMMEDIATE) {
					// For each row removed from this column
					for (int rn = 0; rn < row_indices.Length; ++rn) {
						// Make sure the referenced record exists

						// Return the count of records where the given row of
						//   ref_table_name(columns, ...) IN
						//                    table_name(ref_columns, ...)
						int row_count = RowCountOfReferenceTable(transaction,
												   row_indices[rn],
												   reference.ref_table_name, reference.ref_columns,
												   reference.key_table_name, reference.key_columns,
												   true);
						// There must be 0 references otherwise the delete isn't allowed to
						// happen.
						if (row_count > 0) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.ForeignKeyViolation,
							  DeferredString(deferred) + " foreign key constraint violation " +
							  "on delete (" +
							  reference.name + ") Columns = " +
							  reference.key_table_name.ToString() + "( " +
							  StringColumnList(reference.key_columns) + " ) -> " +
							  reference.ref_table_name.ToString() + "( " +
							  StringColumnList(reference.ref_columns) + " )");
						}
					} // For each row being added.
				}
			}
		}

		/// <summary>
		/// Performs constraint violation checks on a removal of the given 
		/// set of row indices into the <see cref="ITableDataSource"/> in the 
		/// given transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance 
		/// used to determine table  constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="row_index">The row that was removed from the table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.INITIALLY_DEFERRED"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		static void CheckRemoveConstraintViolations(
					SimpleTransaction transaction,
					ITableDataSource table, int row_index, ConstraintDeferrability deferred) {
			CheckRemoveConstraintViolations(transaction, table,
											new int[] { row_index }, deferred);
		}

		/// <summary>
		/// Performs constraint violation checks on all the rows in the given
		/// table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// This method is useful when the constraint schema of a table changes 
		/// and we need to check existing data in a table is conformant with 
		/// the new constraint changes.
		/// <para>
		/// If deferred is <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.INITIALLY_DEFERRED"/> all constraints 
		/// are tested.
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		static void CheckAllAddConstraintViolations(
					 SimpleTransaction transaction, ITableDataSource table,
					 ConstraintDeferrability deferred) {
			// Get all the rows in the table
			int[] rows = new int[table.RowCount];
			IRowEnumerator row_enum = table.GetRowEnumerator();
			int p = 0;
			while (row_enum.MoveNext()) {
				rows[p] = row_enum.RowIndex;
				++p;
			}
			// Check the constraints of all the rows in the table.
			CheckAddConstraintViolations(transaction, table,
										 rows, ConstraintDeferrability.INITIALLY_DEFERRED);
		}


		// ---------- IBlob store and object management ----------

		/// <summary>
		/// Creates and allocates storage for a new large object in the blob store.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// This is called to create a new large object before filling it with 
		/// data sent from the client.
		/// </remarks>
		/// <returns></returns>
		internal IRef CreateNewLargeObject(ReferenceType type, long size) {
			try {
				// If the conglomerate is Read-only, a blob can not be created.
				if (IsReadOnly) {
					throw new Exception(
						"A new large object can not be allocated " +
						"with a Read-only conglomerate");
				}
				// Allocate the large object from the store
				IRef reference = blob_store.AllocateLargeObject(type, size);
				// Return the large object reference
				return reference;
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error when creating blob: " +
										   e.Message);
			}
		}

		/// <summary>
		/// Called when one or more blobs has been completed.
		/// </summary>
		/// <remarks>
		/// This flushes the blob to the blob store and completes the blob 
		/// write procedure. It's important this is called otherwise the 
		/// <see cref="BlobStore"/> may not be correctly flushed to disk with 
		/// the changes and the data will not be recoverable if a crash occurs.
		/// </remarks>
		[Obsolete("Deprecated: no longer necessary", false)]
		internal void FlushBlobStore() {
		}


		// ---------- Conglomerate diagnosis and repair methods ----------

		/// <summary>
		/// Checks the conglomerate state file.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="terminal"></param>
		public void Fix(String name, IUserTerminal terminal) {
			this.name = name;

			try {

				String state_fn = (name + STATE_POST);
				bool state_exists = false;
				try {
					state_exists = Exists(name);
				} catch (IOException e) {
					terminal.WriteLine("IO Error when checking if state store exists: " +
									 e.Message);
					Console.Error.WriteLine(e.StackTrace);
				}

				if (!state_exists) {
					terminal.WriteLine("Couldn't find store: " + state_fn);
					return;
				}
				terminal.WriteLine("+ Found state store: " + state_fn);

				// Open the state store
				try {
					act_state_store = StoreSystem.OpenStore(name + STATE_POST);
					state_store = new StateStore(act_state_store);
					// Get the 64 byte fixed area
					IArea fixed_area = act_state_store.GetArea(-1);
					long head_p = fixed_area.ReadInt8();
					state_store.init(head_p);
					terminal.WriteLine("+ Initialized the state store: " + state_fn);
				} catch (IOException e) {
					// Couldn't initialize the state file.
					terminal.WriteLine("Couldn't initialize the state file: " + state_fn +
									 " Reason: " + e.Message);
					return;
				}

				// Initialize the blob store
				try {
					InitializeBlobStore();
				} catch (IOException e) {
					terminal.WriteLine("Error intializing BlobStore: " + e.Message);
					Console.Error.WriteLine(e.StackTrace);
					return;
				}
				// Setup internal
				SetupInternal();

				try {
					CheckVisibleTables(terminal);

					// Reset the sequence id's for the system tables
					terminal.WriteLine("+ RESETTING ALL SYSTEM TABLE UNIQUE ID VALUES.");
					ResetAllSystemTableID();

					// Some diagnostic information
					StringBuilder buf = new StringBuilder();
					MasterTableDataSource t;
					StateStore.StateResource[] committed_tables = state_store.GetVisibleList();
					StateStore.StateResource[] committed_dropped = state_store.GetDeleteList();
					for (int i = 0; i < committed_tables.Length; ++i) {
						terminal.WriteLine("+ COMMITTED TABLE: " +
										 committed_tables[i].name);
					}
					for (int i = 0; i < committed_dropped.Length; ++i) {
						terminal.WriteLine("+ COMMIT DROPPED TABLE: " +
										 committed_dropped[i].name);
					}

					return;

				} catch (IOException e) {
					terminal.WriteLine("IOException: " + e.Message);
					Console.Out.WriteLine(e.StackTrace);
				}

			} finally {
				try {
					Close();
				} catch (IOException) {
					terminal.WriteLine("Unable to close conglomerate after fix.");
				}
			}

		}


		// ---------- Conveniences for commit ----------

		/// <summary>
		/// A static container class for information collected about a table 
		/// during the commit cycle.
		/// </summary>
		private sealed class CommitTableInfo {
			// The master table
			internal MasterTableDataSource master;
			// The immutable index set
			internal IIndexSet index_set;
			// The journal describing the changes to this table by this
			// transaction.
			internal MasterTableJournal journal;
			// A list of journals describing changes since this transaction
			// started.
			internal MasterTableJournal[] changes_since_commit;
			// Break down of changes to the table
			// Normalized list of row ids that were added
			internal int[] norm_added_rows;
			// Normalized list of row ids that were removed
			internal int[] norm_removed_rows;
		}

		/// <summary>
		/// Returns true if the given List of <see cref="CommitTableInfo"/> objects 
		/// contains an entry for the given master table.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="master"></param>
		/// <returns></returns>
		private static bool CommitTableListContains(IList list,
													   MasterTableDataSource master) {
			int sz = list.Count;
			for (int i = 0; i < sz; ++i) {
				CommitTableInfo info = (CommitTableInfo)list[i];
				if (info.master.Equals(master)) {
					return true;
				}
			}
			return false;
		}




		// ---------- low level File IO level operations on a conglomerate ----------
		// These operations are low level IO operations on the contents of the
		// conglomerate.  How the rows and tables are organised is up to the
		// transaction managemenet.  These methods deal with the low level
		// operations of creating/dropping tables and adding, deleting and querying
		// row in tables.

		/// <summary>
		/// Tries to commit a transaction to the conglomerate.
		/// </summary>
		/// <param name="transaction">The transaction to commit from.</param>
		/// <param name="visible_tables">The list of visible tables at the end 
		/// of the commit (<see cref="MasterTableDataSource"/>)</param>
		/// <param name="selected_from_tables">The list of tables that this 
		/// transaction performed <i>select</i> like queries on (<see cref="MasterTableDataSource"/>)</param>
		/// <param name="touched_tables">The list of tables touched by the 
		/// transaction (<see cref="IMutableTableDataSource"/>)</param>
		/// <param name="journal">The journal that describes all the changes 
		/// within the transaction.</param>
		/// <remarks>
		/// This is called by the <see cref="Transaction.Commit"/> 
		/// method in <see cref="Transaction"/>. An overview of how this works 
		/// follows:
		/// <list type="bullet">
		///   <item>Determine if any transactions have been committed since 
		///   this transaction was created.</item>
		///   <item>If no transactions committed then commit this transaction 
		///   and exit.</item>
		///   <item>Otherwise, determine the tables that have been changed by 
		///   the committed transactions since this was created.</item>
		///   <item>If no tables changed in the tables changed by this transaction 
		///   then commit this transaction and exit.</item>
		///   <item>Determine if there are any rows that have been deleted that 
		///   this transaction read/deleted.</item>
		///   <item>If there are then rollback this transaction and throw an error.</item>
		///   <item>Determine if any rows have been added to the tables this 
		///   transaction read/changed.</item>
		///   <item>If there are then rollback this transaction and throw an error.</item>
		///   <item>Otherwise commit the transaction.</item>
		/// </list>
		/// </remarks>
		internal void ProcessCommit(Transaction transaction, ArrayList visible_tables,
						   ArrayList selected_from_tables,
						   ArrayList touched_tables, TransactionJournal journal) {

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal
			ArrayList journal_list = new ArrayList();
			for (int i = 0; i < touched_tables.Count; ++i) {
				MasterTableJournal table_journal =
					((IMutableTableDataSource)touched_tables[i]).Journal;
				if (table_journal.EntriesCount > 0) {
					// Check the journal has entries.
					journal_list.Add(table_journal);
				}
			}
			MasterTableJournal[] changed_tables =
				(MasterTableJournal[])journal_list.ToArray(typeof(MasterTableJournal));

			// The list of tables created by this journal.
			IntegerVector created_tables = journal.GetTablesCreated();
			// Ths list of tables dropped by this journal.
			IntegerVector dropped_tables = journal.GetTablesDropped();
			// The list of tables that constraints were alter by this journal
			IntegerVector constraint_altered_tables =
				journal.GetTablesConstraintAltered();

			// Exit early if nothing changed (this is a Read-only transaction)
			if (changed_tables.Length == 0 &&
				created_tables.Count == 0 && dropped_tables.Count == 0 &&
				constraint_altered_tables.Count == 0) {
				CloseTransaction(transaction);
				return;
			}

			// This flag is set to true when entries from the changes tables are
			// at a point of no return.  If this is false it is safe to rollback
			// changes if necessary.
			bool entries_committed = false;

			// The tables that were actually changed (MasterTableDataSource)
			ArrayList changed_tables_list = new ArrayList();

			// Grab the commit Lock.
			lock (commit_lock) {

				// Get the list of all database objects that were created in the
				// transaction.
				ArrayList database_objects_created = transaction.AllNamesCreated;
				// Get the list of all database objects that were dropped in the
				// transaction.
				ArrayList database_objects_dropped = transaction.AllNamesDropped;

				// This is a transaction that will represent the view of the database
				// at the end of the commit
				Transaction check_transaction = null;

				try {

					// ---- Commit check stage ----

					long tran_commit_id = transaction.CommitID;

					// We only perform this check if transaction error on dirty selects
					// are enabled.
					if (transaction.TransactionErrorOnDirtySelect) {

						// For each table that this transaction selected from, if there are
						// any committed changes then generate a transaction error.
						for (int i = 0; i < selected_from_tables.Count; ++i) {
							MasterTableDataSource selected_table =
								(MasterTableDataSource)selected_from_tables[i];
							// Find all committed journals equal to or greater than this
							// transaction's commit_id.
							MasterTableJournal[] journals_since =
								selected_table.FindAllJournalsSince(tran_commit_id);
							if (journals_since.Length > 0) {
								// Yes, there are changes so generate transaction error and
								// rollback.
								throw new TransactionException(
									TransactionException.DirtyTableSelect,
									"Concurrent Serializable Transaction Conflict(4): " +
									"Select from table that has committed changes: " +
									selected_table.Name);
							}
						}
					}

					// Check there isn't a namespace clash with database objects.
					// We need to create a list of all create and drop activity in the
					// conglomerate from when the transaction started.
					ArrayList all_dropped_obs = new ArrayList();
					ArrayList all_created_obs = new ArrayList();
					int nsj_sz = namespace_journal_list.Count;
					for (int i = 0; i < nsj_sz; ++i) {
						NameSpaceJournal ns_journal =
							(NameSpaceJournal)namespace_journal_list[i];
						if (ns_journal.commit_id >= tran_commit_id) {
							all_dropped_obs.AddRange(ns_journal.dropped_names);
							all_created_obs.AddRange(ns_journal.created_names);
						}
					}

					// The list of all dropped objects since this transaction
					// began.
					int ado_sz = all_dropped_obs.Count;
					bool conflict5 = false;
					Object conflict_name = null;
					String conflict_desc = "";
					for (int n = 0; n < ado_sz; ++n) {
						if (database_objects_dropped.Contains(all_dropped_obs[n])) {
							conflict5 = true;
							conflict_name = all_dropped_obs[n];
							conflict_desc = "Drop Clash";
						}
					}
					// The list of all created objects since this transaction
					// began.
					int aco_sz = all_created_obs.Count;
					for (int n = 0; n < aco_sz; ++n) {
						if (database_objects_created.Contains(all_created_obs[n])) {
							conflict5 = true;
							conflict_name = all_created_obs[n];
							conflict_desc = "Create Clash";
						}
					}
					if (conflict5) {
						// Namespace conflict...
						throw new TransactionException(
							TransactionException.DuplicateTable,
							"Concurrent Serializable Transaction Conflict(5): " +
							"Namespace conflict: " + conflict_name.ToString() + " " +
							conflict_desc);
					}

					// For each journal,
					for (int i = 0; i < changed_tables.Length; ++i) {
						MasterTableJournal change_journal = changed_tables[i];
						// The table the change was made to.
						int table_id = change_journal.TableId;
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(table_id);

						// True if the state contains a committed resource with the given name
						bool committed_resource =
							state_store.ContainsVisibleResource(table_id);

						// Check this table is still in the committed tables list.
						if (!created_tables.Contains(table_id) &&
							!committed_resource) {
							// This table is no longer a committed table, so rollback
							throw new TransactionException(
								TransactionException.TableDropped,
								"Concurrent Serializable Transaction Conflict(2): " +
								"Table altered/dropped: " + master.Name);
						}

						// Since this journal was created, check to see if any changes to the
						// tables have been committed since.
						// This will return all journals on the table with the same commit_id
						// or greater.
						MasterTableJournal[] journals_since =
							master.FindAllJournalsSince(tran_commit_id);

						// For each journal, determine if there's any clashes.
						for (int n = 0; n < journals_since.Length; ++n) {
							// This will thrown an exception if a commit classes.
							change_journal.TestCommitClash(master.DataTableDef,
														   journals_since[n]);
						}

					}

					// Look at the transaction journal, if a table is dropped that has
					// journal entries since the last commit then we have an exception
					// case.
					for (int i = 0; i < dropped_tables.Count; ++i) {
						int table_id = dropped_tables[i];
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(table_id);
						// Any journal entries made to this dropped table?
						if (master.FindAllJournalsSince(tran_commit_id).Length > 0) {
							// Oops, yes, rollback!
							throw new TransactionException(
								TransactionException.TableRemoveClash,
								"Concurrent Serializable Transaction Conflict(3): " +
								"Dropped table has modifications: " + master.Name);
						}
					}

					// Tests passed so go on to commit,

					// ---- Commit stage ----

					// Create a normalized list of MasterTableDataSource of all tables that
					// were either changed (and not dropped), and created (and not dropped).
					// This list represents all tables that are either new or changed in
					// this transaction.

					int created_tables_count = created_tables.Count;
					int changed_tables_count = changed_tables.Length;
					ArrayList normalized_changed_tables = new ArrayList(8);
					// Add all tables that were changed and not dropped in this transaction.
					for (int i = 0; i < changed_tables_count; ++i) {
						MasterTableJournal table_journal = changed_tables[i];
						// The table the changes were made to.
						int table_id = table_journal.TableId;
						// If this table is not dropped in this transaction and is not
						// already in the normalized list then add it.
						if (!dropped_tables.Contains(table_id)) {
							MasterTableDataSource master_table = GetMasterTable(table_id);

							CommitTableInfo table_info = new CommitTableInfo();
							table_info.master = master_table;
							table_info.journal = table_journal;
							table_info.changes_since_commit =
								master_table.FindAllJournalsSince(tran_commit_id);

							normalized_changed_tables.Add(table_info);
						}
					}

					// Add all tables that were created and not dropped in this transaction.
					for (int i = 0; i < created_tables_count; ++i) {
						int table_id = created_tables[i];
						// If this table is not dropped in this transaction then this is a
						// new table in this transaction.
						if (!dropped_tables.Contains(table_id)) {
							MasterTableDataSource master_table = GetMasterTable(table_id);
							if (!CommitTableListContains(normalized_changed_tables,
														 master_table)) {

								// This is for entries that are created but modified (no journal).
								CommitTableInfo table_info = new CommitTableInfo();
								table_info.master = master_table;

								normalized_changed_tables.Add(table_info);
							}
						}
					}

					// The final size of the normalized changed tables list
					int norm_changed_tables_count = normalized_changed_tables.Count;

					// Create a normalized list of MasterTableDataSource of all tables that
					// were dropped (and not created) in this transaction.  This list
					// represents tables that will be dropped if the transaction
					// successfully commits.

					int dropped_tables_count = dropped_tables.Count;
					ArrayList normalized_dropped_tables = new ArrayList(8);
					for (int i = 0; i < dropped_tables_count; ++i) {
						// The dropped table
						int table_id = dropped_tables[i];
						// Was this dropped table also created?  If it was created in this
						// transaction then we don't care about it.
						if (!created_tables.Contains(table_id)) {
							MasterTableDataSource master_table = GetMasterTable(table_id);
							normalized_dropped_tables.Add(master_table);
						}
					}

					// We now need to create a SimpleTransaction object that we
					// use to send to the triggering mechanism.  This
					// SimpleTransaction represents a very specific view of the
					// transaction.  This view contains the latest version of changed
					// tables in this transaction.  It also contains any tables that have
					// been created by this transaction and does not contain any tables
					// that have been dropped.  Any tables that have not been touched by
					// this transaction are shown in their current committed state.
					// To summarize - this view is the current view of the database plus
					// any modifications made by the transaction that is being committed.

					// How this works - All changed tables are merged with the current
					// committed table.  All created tables are added into check_transaction
					// and all dropped tables are removed from check_transaction.  If
					// there were no other changes to a table between the time the
					// transaction was created and now, the view of the table in the
					// transaction is used, otherwise the latest changes are merged.

					// Note that this view will be the view that the database will
					// ultimately become if this transaction successfully commits.  Also,
					// you should appreciate that this view is NOT exactly the same as
					// the current trasaction view because any changes that have been
					// committed by concurrent transactions will be reflected in this view.

					// Create a new transaction of the database which will represent the
					// committed view if this commit is successful.
					check_transaction = CreateTransaction();

					// Overwrite this view with tables from this transaction that have
					// changed or have been added or dropped.

					// (Note that order here is important).  First drop any tables from
					// this view.
					for (int i = 0; i < normalized_dropped_tables.Count; ++i) {
						// Get the table
						MasterTableDataSource master_table =
							(MasterTableDataSource)normalized_dropped_tables[i];
						// Drop this table in the current view
						check_transaction.RemoveVisibleTable(master_table);
					}

					// Now add any changed tables to the view.

					// Represents view of the changed tables
					ITableDataSource[] changed_table_source =
						new ITableDataSource[norm_changed_tables_count];
					// Set up the above arrays
					for (int i = 0; i < norm_changed_tables_count; ++i) {

						// Get the information for this changed table
						CommitTableInfo table_info =
							(CommitTableInfo)normalized_changed_tables[i];

						// Get the master table that changed from the normalized list.
						MasterTableDataSource master = table_info.master;
						// Did this table change since the transaction started?
						MasterTableJournal[] all_table_changes =
							table_info.changes_since_commit;

						if (all_table_changes == null || all_table_changes.Length == 0) {
							// No changes so we can pick the correct IIndexSet from the current
							// transaction.

							// Get the state of the changed tables from the Transaction
							IMutableTableDataSource mtable =
								transaction.GetTable(master.TableName);
							// Get the current index set of the changed table
							table_info.index_set = transaction.GetIndexSetForTable(master);
							// Flush all index changes in the table
							mtable.FlushIndexChanges();

							// Set the 'check_transaction' object with the latest version of the
							// table.
							check_transaction.UpdateVisibleTable(table_info.master,
																 table_info.index_set);

						} else {
							// There were changes so we need to merge the changes with the
							// current view of the table.

							// It's not immediately obvious how this merge update works, but
							// basically what happens is we WriteByte the table journal with all the
							// changes into a new IMutableTableDataSource of the current
							// committed state, and then we flush all the changes into the
							// index and then update the 'check_transaction' with this change.

							// Create the IMutableTableDataSource with the changes from this
							// journal.
							IMutableTableDataSource mtable =
								master.CreateTableDataSourceAtCommit(check_transaction,
																	 table_info.journal);
							// Get the current index set of the changed table
							table_info.index_set =
								check_transaction.GetIndexSetForTable(master);
							// Flush all index changes in the table
							mtable.FlushIndexChanges();

							// Dispose the table
							mtable.Dispose();

						}

						// And now refresh the 'changed_table_source' entry
						changed_table_source[i] =
							check_transaction.GetTable(master.TableName);

					}

					// The 'check_transaction' now represents the view the database will be
					// if the commit succeeds.  We Lock 'check_transaction' so it is
					// Read-only (the view is immutable).
					check_transaction.SetReadOnly();

					// Any tables that the constraints were altered for we need to check
					// if any rows in the table violate the new constraints.
					for (int i = 0; i < constraint_altered_tables.Count; ++i) {
						// We need to check there are no constraint violations for all the
						// rows in the table.
						int table_id = constraint_altered_tables[i];
						for (int n = 0; n < norm_changed_tables_count; ++n) {
							CommitTableInfo table_info =
								(CommitTableInfo)normalized_changed_tables[n];
							if (table_info.master.TableID == table_id) {
								CheckAllAddConstraintViolations(check_transaction,
																changed_table_source[n],
																ConstraintDeferrability.INITIALLY_DEFERRED);
							}
						}
					}

					// For each changed table we must determine the rows that
					// were deleted and perform the remove constraint checks on the
					// deleted rows.  Note that this happens after the records are
					// removed from the index.

					// For each changed table,
					for (int i = 0; i < norm_changed_tables_count; ++i) {
						CommitTableInfo table_info =
							(CommitTableInfo)normalized_changed_tables[i];
						// Get the journal that details the change to the table.
						MasterTableJournal change_journal = table_info.journal;
						if (change_journal != null) {
							// Find the normalized deleted rows.
							int[] normalized_removed_rows =
								change_journal.NormalizedRemovedRows();
							// Check removing any of the data doesn't cause a constraint
							// violation.
							CheckRemoveConstraintViolations(check_transaction,
															changed_table_source[i], normalized_removed_rows,
															ConstraintDeferrability.INITIALLY_DEFERRED);

							// Find the normalized added rows.
							int[] normalized_added_rows =
								change_journal.NormalizedAddedRows();
							// Check adding any of the data doesn't cause a constraint
							// violation.
							CheckAddConstraintViolations(check_transaction,
														 changed_table_source[i], normalized_added_rows,
														 ConstraintDeferrability.INITIALLY_DEFERRED);

							// Set up the list of added and removed rows
							table_info.norm_added_rows = normalized_added_rows;
							table_info.norm_removed_rows = normalized_removed_rows;

						}
					}

					// Deferred trigger events.
					// For each changed table.
					//n_loop:
					for (int i = 0; i < norm_changed_tables_count; ++i) {
						CommitTableInfo table_info =
							(CommitTableInfo)normalized_changed_tables[i];
						// Get the journal that details the change to the table.
						MasterTableJournal change_journal = table_info.journal;
						if (change_journal != null) {
							// Get the table name
							TableName table_name = table_info.master.TableName;
							// The list of listeners to dispatch this event to
							TransactionModificationListener[] listeners;
							// Are there any listeners listening for events on this table?
							lock (modification_listeners) {
								ArrayList list =
									(ArrayList)modification_listeners[table_name];
								if (list == null || list.Count == 0) {
									// If no listeners on this table, continue to the next
									// table that was changed.
									continue;
								}
								// Generate the list of listeners,
								listeners = (TransactionModificationListener[])list.ToArray(typeof(TransactionModificationListener));
							}
							// Generate the event
							TableCommitModificationEvent evt =
								new TableCommitModificationEvent(
									check_transaction, table_name,
									table_info.norm_added_rows,
									table_info.norm_removed_rows);
							// Fire this event on the listeners
							for (int n = 0; n < listeners.Length; ++n) {
								listeners[n].OnTableCommitChange(evt);
							}

						} // if (change_journal != null)
					} // for each changed table

					// NOTE: This isn't as fail safe as it could be.  We really need to
					//  do the commit in two phases.  The first writes updated indices to
					//  the index files.  The second updates the header pointer for the
					//  respective table.  Perhaps we can make the header update
					//  procedure just one file Write.

					// Finally, at this point all constraint checks have passed and the
					// changes are ready to finally be committed as permanent changes
					// to the conglomerate.  All that needs to be done is to commit our
					// IIndexSet indices for each changed table as final.
					// ISSUE: Should we separate the 'committing of indexes' changes and
					//   'committing of delete/add flags' to make the FS more robust?
					//   It would be more robust if all indexes are committed in one go,
					//   then all table flag data.

					// Set flag to indicate we have committed entries.
					entries_committed = true;

					// For each change to each table,
					for (int i = 0; i < norm_changed_tables_count; ++i) {
						CommitTableInfo table_info =
							(CommitTableInfo)normalized_changed_tables[i];
						// Get the journal that details the change to the table.
						MasterTableJournal change_journal = table_info.journal;
						if (change_journal != null) {
							// Get the master table with this table id.
							MasterTableDataSource master = table_info.master;
							// Commit the changes to the table.
							// We use 'this.commit_id' which is the current commit level we are
							// at.
							master.CommitTransactionChange(this.commit_id, change_journal,
														   table_info.index_set);
							// Add to 'changed_tables_list'
							changed_tables_list.Add(master);
						}
					}

					// Only do this if we've created or dropped tables.
					if (created_tables.Count > 0 || dropped_tables.Count > 0) {
						// Update the committed tables in the conglomerate state.
						// This will update and synchronize the headers in this conglomerate.
						CommitToTables(created_tables, dropped_tables);
					}

					// Update the namespace clash list
					if (database_objects_created.Count > 0 ||
						database_objects_dropped.Count > 0) {
						NameSpaceJournal namespace_journal =
							new NameSpaceJournal(tran_commit_id,
												 database_objects_created,
												 database_objects_dropped);
						namespace_journal_list.Add(namespace_journal);
					}

				} finally {

					try {

						// If entries_committed == false it means we didn't get to a point
						// where any changed tables were committed.  Attempt to rollback the
						// changes in this transaction if they haven't been committed yet.
						if (entries_committed == false) {
							// For each change to each table,
							for (int i = 0; i < changed_tables.Length; ++i) {
								// Get the journal that details the change to the table.
								MasterTableJournal change_journal = changed_tables[i];
								// The table the changes were made to.
								int table_id = change_journal.TableId;
								// Get the master table with this table id.
								MasterTableDataSource master = GetMasterTable(table_id);
								// Commit the rollback on the table.
								master.RollbackTransactionChange(change_journal);
							}
							if (Debug.IsInterestedIn(DebugLevel.Information)) {
								Debug.Write(DebugLevel.Information, this,
											  "Rolled back transaction changes in a commit.");
							}
						}

					} finally {
						try {
							// Dispose the 'check_transaction'
							if (check_transaction != null) {
								check_transaction.dispose();
								CloseTransaction(check_transaction);
							}
							// Always ensure a transaction close, even if we have an exception.
							// Notify the conglomerate that this transaction has closed.
							CloseTransaction(transaction);
						} catch (Exception e) {
							Debug.WriteException(e);
						}
					}

				}

				// Flush the journals up to the minimum commit id for all the tables
				// that this transaction changed.
				long min_commit_id = open_transactions.MinimumCommitID(null);
				int chsz = changed_tables_list.Count;
				for (int i = 0; i < chsz; ++i) {
					MasterTableDataSource master =
						(MasterTableDataSource)changed_tables_list[i];
					master.MergeJournalChanges(min_commit_id);
				}
				int nsjsz = namespace_journal_list.Count;
				for (int i = nsjsz - 1; i >= 0; --i) {
					NameSpaceJournal namespace_journal =
						(NameSpaceJournal)namespace_journal_list[i];
					// Remove if the commit id for the journal is less than the minimum
					// commit id
					if (namespace_journal.commit_id < min_commit_id) {
						namespace_journal_list.RemoveAt(i);
					}
				}

				// Set a check point in the store system.  This means that the
				// persistance state is now stable.
				store_system.SetCheckPoint();

			} // lock (commit_lock)

		}

		/// <summary>
		/// Rollbacks a transaction and invalidates any changes that the 
		/// transaction made to the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="touched_tables"></param>
		/// <param name="journal"></param>
		/// <remarks>
		/// The rows that this transaction changed are given up as freely 
		/// available rows. This is called by the <see cref="Transaction.Rollback"/> 
		/// method in <see cref="Transaction"/>.
		/// </remarks>
		internal void ProcessRollback(Transaction transaction,
							 ArrayList touched_tables, TransactionJournal journal) {

			// Go through the journal.  Any rows added should be marked as deleted
			// in the respective master table.

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal
			ArrayList journal_list = new ArrayList();
			for (int i = 0; i < touched_tables.Count; ++i) {
				MasterTableJournal table_journal =
						   ((IMutableTableDataSource)touched_tables[i]).Journal;
				if (table_journal.EntriesCount > 0) {  // Check the journal has entries.
					journal_list.Add(table_journal);
				}
			}
			MasterTableJournal[] changed_tables =
						(MasterTableJournal[])journal_list.ToArray(typeof(MasterTableJournal));

			// The list of tables created by this journal.
			IntegerVector created_tables = journal.GetTablesCreated();

			lock (commit_lock) {

				try {

					// For each change to each table,
					for (int i = 0; i < changed_tables.Length; ++i) {
						// Get the journal that details the change to the table.
						MasterTableJournal change_journal = changed_tables[i];
						// The table the changes were made to.
						int table_id = change_journal.TableId;
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(table_id);
						// Commit the rollback on the table.
						master.RollbackTransactionChange(change_journal);
					}

				} finally {
					// Notify the conglomerate that this transaction has closed.
					CloseTransaction(transaction);
				}
			}
		}

		// -----

		/// <summary>
		/// Sets the <see cref="MasterTableDataSource"/> objects pointed by the
		/// given <see cref="IntegerVector"/> to the currently committed list of 
		/// tables in this conglomerate.
		/// </summary>
		/// <param name="created_tables"></param>
		/// <param name="dropped_tables"></param>
		/// <remarks>
		/// This will make the change permanent by updating the state file also.
		/// <para>
		/// This should be called as part of a transaction commit.
		/// </para>
		/// </remarks>
		private void CommitToTables(
						IntegerVector created_tables, IntegerVector dropped_tables) {

			// Add created tables to the committed tables list.
			for (int i = 0; i < created_tables.Count; ++i) {
				// For all created tables, add to the visible list and remove from the
				// delete list in the state store.
				MasterTableDataSource t = GetMasterTable(created_tables[i]);
				StateStore.StateResource resource =
						   new StateStore.StateResource(t.TableID, CreateEncodedTableFile(t));
				state_store.AddVisibleResource(resource);
				state_store.RemoveDeleteResource(resource.name);
			}

			// Remove dropped tables from the committed tables list.
			for (int i = 0; i < dropped_tables.Count; ++i) {
				// For all dropped tables, add to the delete list and remove from the
				// visible list in the state store.
				MasterTableDataSource t = GetMasterTable(dropped_tables[i]);
				StateStore.StateResource resource =
						   new StateStore.StateResource(t.TableID, CreateEncodedTableFile(t));
				state_store.AddDeleteResource(resource);
				state_store.RemoveVisibleResource(resource.name);
			}

			try {
				state_store.Commit();
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Returns the <see cref="MasterTableDataSource"/> in this conglomerate 
		/// with the given table id.
		/// </summary>
		/// <param name="table_id"></param>
		/// <returns></returns>
		MasterTableDataSource GetMasterTable(int table_id) {
			lock (commit_lock) {
				// Find the table with this table id.
				for (int i = 0; i < table_list.Count; ++i) {
					MasterTableDataSource t = (MasterTableDataSource)table_list[i];
					if (t.TableID == table_id) {
						return t;
					}
				}
				throw new ApplicationException("Unable to find an open table with id: " + table_id);
			}
		}

		/// <summary>
		/// Creates a table store in this conglomerate with the given name and 
		/// returns a reference to the table.
		/// </summary>
		/// <param name="table_def">The table meta definition.</param>
		/// <param name="data_sector_size">The size of the data sectors 
		/// (affects performance and size of the file).</param>
		/// <param name="index_sector_size">The size of the index sectors.</param>
		/// <remarks>
		/// Note that this table is not a commited change to the system. It 
		/// is a free standing blank table store. The table returned here is 
		/// uncommitted and will be deleted unless it is committed.
		/// <para>
		/// Note that two tables may exist within a conglomerate with the same 
		/// name, however each <b>committed</b> table must have a unique name.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal MasterTableDataSource CreateMasterTable(DataTableDef table_def,
									  int data_sector_size, int index_sector_size) {
			lock (commit_lock) {
				try {

					// EFFICIENCY: Currently this writes to the conglomerate state file
					//   twice.  Once in 'NextUniqueTableID' and once in
					//   'state_store.commit'.

					// The unique id that identifies this table,
					int table_id = NextUniqueTableID();

					// Create the object.
					V2MasterTableDataSource master_table = new V2MasterTableDataSource(System, StoreSystem, open_transactions,
					                                                                   blob_store);
					master_table.Create(table_id, table_def);

					// Add to the list of all tables.
					table_list.Add(master_table);

					// Add this to the list of deleted tables,
					// (This should really be renamed to uncommitted tables).
					MarkAsCommittedDropped(table_id);

					// Commit this
					state_store.Commit();

					// And return it.
					return master_table;

				} catch (IOException e) {
					Debug.WriteException(e);
					throw new ApplicationException("Unable to create master table '" +
									table_def.Name + "' - " + e.Message);
				}
			}

		}

		internal MasterTableDataSource CreateTemporaryDataSource(DataTableDef table_def) {
			lock (commit_lock) {
				try {
					// The unique id that identifies this table,
					int table_id = NextUniqueTableID();

					V2MasterTableDataSource temporary = new V2MasterTableDataSource(System, new V1HeapStoreSystem(), open_transactions, blob_store);

					temporary.Create(table_id, table_def);

					table_list.Add(temporary);

					return temporary;
				} catch(Exception e) {
					Debug.WriteException(e);
					throw new ApplicationException("Unable to create temporary table '" + table_def.Name + "' - " + e.Message);
				}
			}
		}

		/// <summary>
		/// Creates a table store in this conglomerate that is an exact copy 
		/// of the given <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="src_master_table">The source master table to copy.</param>
		/// <param name="index_set">The view of the table index to copy.</param>
		/// <remarks>
		/// Note that this table is not a commited change to the system. It is 
		/// a free standing blank table store. The table returned here is 
		/// uncommitted and will be deleted unless it is committed.
		/// <para>
		/// Note that two tables may exist within a conglomerate with the same 
		/// name, however each <b>committed</b> table must have a unique name.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the <see cref="MasterTableDataSource"/> with the copied 
		/// information.
		/// </returns>
		internal MasterTableDataSource CopyMasterTable(MasterTableDataSource src_master_table, IIndexSet index_set) {
			lock (commit_lock) {
				try {

					// EFFICIENCY: Currently this writes to the conglomerate state file
					//   twice.  Once in 'NextUniqueTableID' and once in
					//   'state_store.commit'.

					// The unique id that identifies this table,
					int table_id = NextUniqueTableID();

					// Create the object.
					V2MasterTableDataSource master_table =
						new V2MasterTableDataSource(System,
							 StoreSystem, open_transactions, blob_store);

					master_table.Copy(table_id, src_master_table, index_set);

					// Add to the list of all tables.
					table_list.Add(master_table);

					// Add this to the list of deleted tables,
					// (This should really be renamed to uncommitted tables).
					MarkAsCommittedDropped(table_id);

					// Commit this
					state_store.Commit();

					// And return it.
					return master_table;

				} catch (IOException e) {
					Debug.WriteException(e);
					throw new Exception("Unable to copy master table '" +
									src_master_table.DataTableDef.Name +
									"' - " + e.Message);
				}
			}

		}

		// ---------- Inner classes ----------

		/// <summary>
		/// A journal for handling namespace clashes between transactions.
		/// </summary>
		/// <remarks>
		/// For example, we would need to generate a conflict if two concurrent
		/// transactions were to drop the same table, or if a procedure and a
		/// table with the same name were generated in concurrent transactions.
		/// </remarks>
		private sealed class NameSpaceJournal {

			/// <summary>
			/// The commit_id of this journal entry.
			/// </summary>
			internal readonly long commit_id;

			/// <summary>
			/// The list of names created in this journal.
			/// </summary>
			internal readonly ArrayList created_names;

			/// <summary>
			/// The list of names dropped in this journal.
			/// </summary>
			internal readonly ArrayList dropped_names;

			internal NameSpaceJournal(long commit_id,
							 ArrayList created_names, ArrayList dropped_names) {
				this.commit_id = commit_id;
				this.created_names = created_names;
				this.dropped_names = dropped_names;
			}

		}


		#region Implementation of IDisposable

		public void Dispose() {
			//    removeShutdownHook();
		}

		#endregion
	}
}