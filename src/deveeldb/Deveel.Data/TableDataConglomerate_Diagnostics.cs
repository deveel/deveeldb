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

using Deveel.Data.Collections;
using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data {
	public sealed partial class TableDataConglomerate {
		/// <summary>
		/// Given a table with a 'id' field, this will check that the sequence
		/// value for the table is at least greater than the maximum id in the column.
		/// </summary>
		/// <param name="tname"></param>
		private void ResetTableID(TableName tname) {
			// Create the transaction
			Transaction transaction = CreateTransaction();
			// Get the table
			IMutableTableDataSource table = transaction.GetTable(tname);
			// Find the index of the column name called 'id'
			DataTableDef tableDef = table.TableInfo;
			int colIndex = tableDef.FindColumnName("id");
			if (colIndex == -1)
				throw new ApplicationException("Column name 'id' not found.");

			// Find the maximum 'id' value.
			SelectableScheme scheme = table.GetColumnScheme(colIndex);
			IntegerVector ivec = scheme.SelectLast();
			if (ivec.Count > 0) {
				TObject value = table.GetCellContents(colIndex, ivec[0]);
				BigNumber bNum = value.ToBigNumber();
				if (bNum != null) {
					// Set the unique id to +1 the maximum id value in the column
					transaction.SetUniqueID(tname, bNum.ToInt64() + 1L);
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
		private void ResetAllSystemTableID() {
			ResetTableID(PrimaryInfoTable);
			ResetTableID(ForeignInfoTable);
			ResetTableID(UniqueInfoTable);
			ResetTableID(CheckInfoTable);
			ResetTableID(SchemaInfoTable);
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
			StateStore.StateResource[] tables = stateStore.GetVisibleList();
			// For each visible table
			for (int i = 0; i < tables.Length; ++i) {
				StateStore.StateResource resource = tables[i];

				int masterTableId = (int)resource.table_id;
				string fileName = resource.name;

				// Parse the file name string and determine the table type.
				int tableType = 1;
				if (fileName.StartsWith(":")) {
					if (fileName[1] == '1')
						throw new NotSupportedException();

					if (fileName[1] != '2')
						throw new Exception("Table type is not known.");

					tableType = 2;
					fileName = fileName.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master = LoadMasterTable(masterTableId, fileName, tableType);

				if (!(master is V2MasterTableDataSource))
					throw new ApplicationException("Unknown master table type: " + master.GetType());

				V2MasterTableDataSource v2Master = (V2MasterTableDataSource)master;
				v2Master.SourceIdentity = fileName;
				v2Master.Repair(terminal);

				// Add the table to the table list
				tableList.Add(master);

				// Set a check point
				storeSystem.SetCheckPoint();
			}
		}


		/// <summary>
		/// Checks the conglomerate state file.
		/// </summary>
		/// <param name="terminal"></param>
		public void Fix(IUserTerminal terminal) {
			try {
				string stateFn = (name + StatePost);
				bool stateExists = false;
				try {
					stateExists = Exists(name);
				} catch (IOException e) {
					terminal.WriteLine("IO Error when checking if state store exists: " + e.Message);
					Console.Error.WriteLine(e.StackTrace);
				}

				if (!stateExists) {
					terminal.WriteLine("Couldn't find store: " + stateFn);
					return;
				}
				terminal.WriteLine("+ Found state store: " + stateFn);

				// Open the state store
				try {
					actStateStore = StoreSystem.OpenStore(name + StatePost);
					stateStore = new StateStore(actStateStore);
					// Get the 64 byte fixed area
					IArea fixed_area = actStateStore.GetArea(-1);
					long head_p = fixed_area.ReadInt8();
					stateStore.init(head_p);
					terminal.WriteLine("+ Initialized the state store: " + stateFn);
				} catch (IOException e) {
					// Couldn't initialize the state file.
					terminal.WriteLine("Couldn't initialize the state file: " + stateFn +
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
					StateStore.StateResource[] committedTables = stateStore.GetVisibleList();
					StateStore.StateResource[] committed_dropped = stateStore.GetDeleteList();
					for (int i = 0; i < committedTables.Length; ++i) {
						terminal.WriteLine("+ COMMITTED TABLE: " + committedTables[i].name);
					}
					for (int i = 0; i < committed_dropped.Length; ++i) {
						terminal.WriteLine("+ COMMIT DROPPED TABLE: " + committed_dropped[i].name);
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

		/// <summary>
		/// Returns a RawDiagnosticTable object that is used for diagnostics of 
		/// the table with the given file name.
		/// </summary>
		/// <param name="tableFileName"></param>
		/// <returns></returns>
		public IRawDiagnosticTable GetDiagnosticTable(string tableFileName) {
			lock (CommitLock) {
				foreach (MasterTableDataSource master in tableList) {
					if (master.SourceIdentity.Equals(tableFileName)) {
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
			lock (CommitLock) {
				String[] list = new String[tableList.Count];
				for (int i = 0; i < tableList.Count; ++i) {
					MasterTableDataSource master = tableList[i];
					list[i] = master.SourceIdentity;
				}
				return list;
			}
		}

	}
}