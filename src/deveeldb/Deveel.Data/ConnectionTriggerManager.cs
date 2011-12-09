// 
//  Copyright 2010  Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

using Deveel.Data.Procedures;

namespace Deveel.Data {
	/// <summary>
	/// A trigger manager on a <see cref="DatabaseConnection"/> that maintains 
	/// a list of all triggers set in the database, and the types of triggers 
	/// they are.
	/// </summary>
	/// <remarks>
	/// The trigger manager actually uses a trigger itself to maintain a list of
	/// tables that have triggers, and the action to perform on the trigger.
	/// </remarks>
	public sealed class ConnectionTriggerManager {
		/// <summary>
		/// The DatabaseConnection.
		/// </summary>
		private readonly DatabaseConnection connection;

		/// <summary>
		/// The list of triggers currently in view. (TriggerInfo)
		/// </summary>
		private readonly ArrayList triggers_active;

		/// <summary>
		/// If this is false then the list is not validated and must be refreshed
		/// when we next access trigger information.
		/// </summary>
		private bool list_validated;

		/// <summary>
		/// True if the trigger table was modified during the last transaction.
		/// </summary>
		private bool trigger_modified;

		internal ConnectionTriggerManager(DatabaseConnection connection) {
			this.connection = connection;
			triggers_active = new ArrayList();
			list_validated = false;
			trigger_modified = false;
			// Attach a commit trigger listener
			connection.AttachTableBackedCache(new CTMBackedCache(this));
		}

		/// <summary>
		/// Returns a Table object that contains the trigger information with the
		/// given name.  Returns an empty table if no trigger found.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="schema"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		private static Table FindTrigger(IQueryContext context, Table table, String schema, String name) {
			// Find all the trigger entries with this name
			Operator EQUALS = Operator.Get("=");

			VariableName schemav = table.GetResolvedVariable(0);
			VariableName namev = table.GetResolvedVariable(1);

			Table t = table.SimpleSelect(context, namev, EQUALS,
										 new Expression(TObject.CreateString(name)));
			return t.ExhaustiveSelect(context, Expression.Simple(
												schemav, EQUALS, TObject.CreateString(schema)));
		}

		/// <summary>
		/// Creates a new trigger action on a stored procedure and makes the change
		/// to the transaction of the underlying <see cref="DatabaseConnection"/>.
		/// </summary>
		/// <param name="schema">The schema name of the trigger.</param>
		/// <param name="name">The name of the trigger.</param>
		/// <param name="type">The type of trigger.</param>
		/// <param name="on_table">The table on which the trigger will be executed.</param>
		/// <param name="procedure_name">The name of the procedure to execute.</param>
		/// <param name="parameters">Any constant parameters for the triggering procedure.</param>
		/// <remarks>
		/// If the session is committed then the trigger is made a 
		/// permanent change to the database.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a trigger with the given name already exists within the 
		/// underlying session.
		/// </exception>
		/// <exception cref="IOException">
		/// If any error occurred while serializing trigger parameters.
		/// </exception>
		public void CreateTableTrigger(String schema, String name,
									   TriggerEventType type, TableName on_table,
									   String procedure_name, TObject[] parameters) {
			TableName trigger_table_name = new TableName(schema, name);

			// Check this name is not reserved
			DatabaseConnection.CheckAllowCreate(trigger_table_name);

			// Before adding the trigger, make sure this name doesn't already Resolve
			// to an object in the database with this schema/name.
			if (!connection.TableExists(trigger_table_name)) {
				// Encode the parameters
				MemoryStream bout = new MemoryStream();
				try {
					BinaryWriter ob_out = new BinaryWriter(bout);
					ob_out.Write(1); // version
					MemoryStream obj_stream = new MemoryStream();
					BinaryFormatter formatter = new BinaryFormatter();
					formatter.Serialize(obj_stream, parameters);
					obj_stream.Flush();
					byte[] buf = obj_stream.ToArray();
					ob_out.Write(buf.Length);
					ob_out.Write(buf);
					ob_out.Flush();
				} catch (IOException e) {
					throw new Exception("IO Error: " + e.Message);
				}
				byte[] encoded_params = bout.ToArray();

				// Insert the entry into the trigger table,
				DataTable table = connection.GetTable(Database.SysDataTrigger);
				DataRow row = new DataRow(table);
				row.SetValue(0, TObject.CreateString(schema));
				row.SetValue(1, TObject.CreateString(name));
				row.SetValue(2, TObject.CreateInt4((int)type));
				row.SetValue(3, TObject.CreateString("T:" + on_table));
				row.SetValue(4, TObject.CreateString(procedure_name));
				row.SetValue(5, TObject.CreateObject(encoded_params));
				row.SetValue(6, TObject.CreateString(connection.User.UserName));
				table.Add(row);

				// Invalidate the list
				InvalidateTriggerList();

				// Notify that this database object has been successfully created.
				connection.DatabaseObjectCreated(trigger_table_name);

				// Flag that this transaction modified the trigger table.
				trigger_modified = true;
			} else {
				throw new Exception("Trigger name '" + schema + "." + name +
									"' already in use.");
			}
		}

		/// <summary>
		/// Drops a previously defined trigger.
		/// </summary>
		/// <param name="schema">The schema name of the trigger</param>
		/// <param name="name">The name of the trigger to drop.</param>
		/// <exception cref="StatementException">
		/// If none or more than one trigger was found for the given 
		/// <paramref name="name"/> in the given <paramref name="schema"/>.
		/// </exception>
		public void DropTrigger(String schema, String name) {
			IQueryContext context = new DatabaseQueryContext(connection);
			DataTable table = connection.GetTable(Database.SysDataTrigger);

			// Find the trigger
			Table t = FindTrigger(context, table, schema, name);

			if (t.RowCount == 0)
				throw new StatementException("Trigger '" + schema + "." + name +
				                             "' not found.");
			if (t.RowCount > 1)
				throw new Exception("Assertion failed: multiple entries for the same trigger name.");

			// Drop this trigger,
			table.Delete(t);

			// Notify that this database object has been successfully dropped.
			connection.DatabaseObjectDropped(new TableName(schema, name));

			// Flag that this transaction modified the trigger table.
			trigger_modified = true;
		}

		/// <summary>
		/// Checks the existence of a trigger for the given name.
		/// </summary>
		/// <param name="schema">The schema name of the trigger.</param>
		/// <param name="name">The name of the trigger to check.</param>
		/// <returns>
		/// Returns <b>true</b> if the trigger exists, otherwise <b>false</b>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If multiple triggers were found for the given <paramref name="name"/> 
		/// in the given <paramref name="schema"/>.
		/// </exception>
		public bool TriggerExists(String schema, String name) {
			IQueryContext context = new DatabaseQueryContext(connection);
			DataTable table = connection.GetTable(Database.SysDataTrigger);

			// Find the trigger
			Table t = FindTrigger(context, table, schema, name);

			if (t.RowCount == 0)
				// Trigger wasn't found
				return false;

			if (t.RowCount > 1)
				throw new Exception("Assertion failed: multiple entries for the same trigger name.");

			// Trigger found
			return true;
		}

		/// <summary>
		/// Invalidates the trigger list causing the list to rebuild when a 
		/// potential triggering event next occurs.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> must only be called from the thread that owns the
		/// underlying database session.
		/// </remarks>
		private void InvalidateTriggerList() {
			list_validated = false;
			triggers_active.Clear();
		}

		/// <summary>
		/// Build the trigger list if it is not validated.
		/// </summary>
		private void BuildTriggerList() {
			if (!list_validated) {
				// Cache the trigger table
				DataTable table = connection.GetTable(Database.SysDataTrigger);
				IRowEnumerator e = table.GetRowEnumerator();

				// For each row
				while (e.MoveNext()) {
					int row_index = e.RowIndex;

					TObject trig_schem = table.GetCellContents(0, row_index);
					TObject trig_name = table.GetCellContents(1, row_index);
					TObject type = table.GetCellContents(2, row_index);
					TObject on_object = table.GetCellContents(3, row_index);
					TObject action = table.GetCellContents(4, row_index);
					TObject misc = table.GetCellContents(5, row_index);

					TriggerInfo trigger_info = new TriggerInfo();
					trigger_info.schema = trig_schem.Object.ToString();
					trigger_info.name = trig_name.Object.ToString();
					trigger_info.type = (TriggerEventType) type.ToBigNumber().ToInt32();
					trigger_info.on_object = on_object.Object.ToString();
					trigger_info.action = action.Object.ToString();
					trigger_info.misc = misc;

					// Add to the list
					triggers_active.Add(trigger_info);
				}

				list_validated = true;
			}
		}

		/// <summary>
		/// Performs any trigger action for this event.
		/// </summary>
		/// <param name="evt"></param>
		/// <remarks>
		/// For example, if we have it setup so a trigger fires when there is an 
		/// <c>INSERT</c> event on table x then we perform the triggering procedure right here.
		/// </remarks>
		internal void PerformTriggerAction(TableModificationEvent evt) {
			// REINFORCED NOTE: The 'TableExists' call is REALLY important.  First it
			//   makes sure the transaction on the connection is established (it should
			//   be anyway if a trigger is firing), and it also makes sure the trigger
			//   table exists - which it may not be during database init.
			if (connection.TableExists(Database.SysDataTrigger)) {
				// If the trigger list isn't built, then do so now
				BuildTriggerList();

				// On object value to test for,
				TableName table_name = evt.TableName;
				String on_ob_test = "T:" + table_name;

				// Search the triggers list for an event that matches this event
				int sz = triggers_active.Count;
				for (int i = 0; i < sz; ++i) {
					TriggerInfo t_info = (TriggerInfo)triggers_active[i];
					if (t_info.on_object.Equals(on_ob_test)) {
						// Table name matches
						// Do the types match?  eg. before/after match, and
						// insert/delete/update is being listened to.
						if (evt.IsListenedBy(t_info.type)) {
							// Type matches this trigger, so we need to fire it
							// Parse the action string
							String action = t_info.action;
							// Get the procedure name to fire (qualify it against the schema
							// of the table being fired).
							ProcedureName procedure_name =
								ProcedureName.Qualify(table_name.Schema, action);
							// Set up OLD and NEW tables

							// Record the old table state
							DatabaseConnection.OldNewTableState current_state =
								connection.GetOldNewTableState();

							// Set the new table state
							// If an INSERT event then we setup NEW to be the row being inserted
							// If an DELETE event then we setup OLD to be the row being deleted
							// If an UPDATE event then we setup NEW to be the row after the
							// update, and OLD to be the row before the update.
							connection.SetOldNewTableState(
								new DatabaseConnection.OldNewTableState(table_name,
																		evt.RowIndex, evt.DataRow, evt.IsBefore));

							try {
								// Invoke the procedure (no arguments)
								connection.ProcedureManager.InvokeProcedure(
									procedure_name, new TObject[0]);
							} finally {
								// Reset the OLD and NEW tables to previous values
								connection.SetOldNewTableState(current_state);
							}
						}
					}
				} // for each trigger
			}
		}

		/// <summary>
		/// Returns an <see cref="IInternalTableInfo"/> object used to model the 
		/// list of triggers that are accessible within the given <see cref="Transaction"/> 
		/// object.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is used to model all triggers that have been defined as tables.
		/// </remarks>
		/// <returns></returns>
		internal static IInternalTableInfo CreateInternalTableInfo(Transaction transaction) {
			return new TriggerInternalTableInfo(transaction);
		}

		// ---------- Inner classes ----------

		#region Nested type: CTMBackedCache

		/// <summary>
		/// A TableBackedCache that manages the list of session level triggers that
		/// are currently active on this session.
		/// </summary>
		private class CTMBackedCache : TableBackedCache {
			private ConnectionTriggerManager ctm;

			/**
			 * Constructor.
			 */

			public CTMBackedCache(ConnectionTriggerManager ctm)
				: base(Database.SysDataTrigger) {
				this.ctm = ctm;
			}

			protected override void PurgeCache(IList<int> addedRows, IList<int> removedRows) {
				// Note that this is called when a transaction is started or stopped.

				// If the trigger table was modified, we need to invalidate the trigger
				// list.  This covers the case when we rollback a trigger table change
				if (ctm.trigger_modified) {
					ctm.InvalidateTriggerList();
					ctm.trigger_modified = false;
				}
					// If any data has been committed removed then completely flush the
					// cache.
				else if ((removedRows != null && removedRows.Count > 0) ||
						 (addedRows != null && addedRows.Count > 0)) {
					ctm.InvalidateTriggerList();
				}
			}
		}

		#endregion

		#region Nested type: TriggerInfo

		/// <summary>
		/// Container class for all trigger actions defined on the database.
		/// </summary>
		private class TriggerInfo {
			internal String action;
			internal TObject misc;
			internal String name;
			internal String on_object;
			internal String schema;
			internal TriggerEventType type;
		}

		#endregion

		#region Nested type: TriggerInternalTableInfo

		/// <summary>
		/// An object that models the list of triggers as table objects in a
		/// transaction.
		/// </summary>
		private sealed class TriggerInternalTableInfo
			: InternalTableInfo2 {
			internal TriggerInternalTableInfo(Transaction transaction)
				: base(transaction, Database.SysDataTrigger) {
			}

			private static DataTableInfo CreateTableInfo(String schema, String name) {
				// Create the DataTableInfo that describes this entry
				DataTableInfo info = new DataTableInfo();
				info.TableName = new TableName(schema, name);

				// Add column definitions
				info.AddColumn(DataTableColumnInfo.CreateNumericColumn("type"));
				info.AddColumn(DataTableColumnInfo.CreateStringColumn("on_object"));
				info.AddColumn(DataTableColumnInfo.CreateStringColumn("procedure_name"));
				info.AddColumn(DataTableColumnInfo.CreateStringColumn("param_args"));
				info.AddColumn(DataTableColumnInfo.CreateStringColumn("owner"));

				// Set to immutable
				info.SetImmutable();

				// Return the data table info
				return info;
			}


			public override String GetTableType(int i) {
				return "TRIGGER";
			}

			public override DataTableInfo GetTableInfo(int i) {
				TableName table_name = GetTableName(i);
				return CreateTableInfo(table_name.Schema, table_name.Name);
			}

			public override IMutableTableDataSource CreateInternalTable(int index) {
				IMutableTableDataSource table =
					transaction.GetTable(Database.SysDataTrigger);
				IRowEnumerator row_e = table.GetRowEnumerator();
				int p = 0;
				int i;
				int row_i = -1;
				while (row_e.MoveNext()) {
					i = row_e.RowIndex;
					if (p == index) {
						row_i = i;
					} else {
						++p;
					}
				}
				if (p == index) {
					String schema = table.GetCellContents(0, row_i).Object.ToString();
					String name = table.GetCellContents(1, row_i).Object.ToString();

					DataTableInfo tableInfo = CreateTableInfo(schema, name);
					TObject type = table.GetCellContents(2, row_i);
					TObject on_object = table.GetCellContents(3, row_i);
					TObject procedure_name = table.GetCellContents(4, row_i);
					TObject param_args = table.GetCellContents(5, row_i);
					TObject owner = table.GetCellContents(6, row_i);

					// Implementation of IMutableTableDataSource that describes this
					// trigger.
					GTDataSourceImpl int_table = new GTDataSourceImpl(transaction.System, tableInfo);
					int_table.type = type;
					int_table.on_object = on_object;
					int_table.procedure_name = procedure_name;
					int_table.param_args = param_args;
					int_table.owner = owner;
					return int_table;
				} else {
					throw new Exception("Index out of bounds.");
				}
			}

			#region Nested type: GTDataSourceImpl

			private class GTDataSourceImpl : GTDataSource {
				internal TObject on_object;
				internal TObject owner;
				internal TObject param_args;
				internal TObject procedure_name;
				private readonly DataTableInfo tableInfo;
				internal TObject type;

				public GTDataSourceImpl(TransactionSystem system, DataTableInfo tableInfo)
					: base(system) {
					this.tableInfo = tableInfo;
				}

				public override DataTableInfo TableInfo {
					get { return tableInfo; }
				}

				public override int RowCount {
					get { return 1; }
				}

				public override TObject GetCellContents(int col, int row) {
					switch (col) {
						case 0:
							return type;
						case 1:
							return on_object;
						case 2:
							return procedure_name;
						case 3:
							return param_args;
						case 4:
							return owner;
						default:
							throw new Exception("Column out of bounds.");
					}
				}
			}

			#endregion
		}

		#endregion
	}
}