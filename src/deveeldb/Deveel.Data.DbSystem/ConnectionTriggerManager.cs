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
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

using Deveel.Data.Routines;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
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
		/// The list of triggers currently in view. (ProcedureTriggerInfo)
		/// </summary>
		private readonly List<ProcedureTriggerInfo> triggersActive;

		/// <summary>
		/// Maps from the user session (User) to the list of TriggerAction 
		/// objects for this user.
		/// </summary>
		private readonly Dictionary<DatabaseConnection, IList<TriggerAction>> listenerMap;

		/// <summary>
		/// If this is false then the list is not validated and must be refreshed
		/// when we next access trigger information.
		/// </summary>
		private bool listValidated;

		/// <summary>
		/// True if the trigger table was modified during the last transaction.
		/// </summary>
		private bool triggerModified;

		internal ConnectionTriggerManager(DatabaseConnection connection) {
			this.connection = connection;
			triggersActive = new List<ProcedureTriggerInfo>();
			listValidated = false;
			triggerModified = false;
			listenerMap = new Dictionary<DatabaseConnection, IList<TriggerAction>>();
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
		/// Notifies all the handlers on a triggerSource (ie. a table) that a
		/// specific type of event has happened, as denoted by the type.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="args"></param>
		private void FireTrigger(DatabaseConnection database, TriggerEventArgs args) {
			List<TriggerAction> trigList;
			lock (this) {
				IList<TriggerAction> list;
				if (!listenerMap.TryGetValue(database, out list))
					return;

				if (list.Count == 0)
					return;

				trigList = new List<TriggerAction>();
				foreach (TriggerAction action in list) {
					if (action.TriggerSource == args.Source)
						trigList.Add(action);
				}
			}

			// Post an event that fires the triggers for each listener.
			//FireTriggersDelegate d = new FireTriggersDelegate(args, trig_list);

			// Post the event to go off approx 3ms from now.
			connection.Context.PostEvent(3, connection.Context.CreateEvent(delegate {
				foreach (TriggerAction action in trigList) {
					if ((args.EventType & action.TriggerEvent) != 0)
						action.Handler(action.Connection,
									   new TriggerEventArgs(action.TriggerName, args.Source,
															args.EventType, args.FireCount));
				}

			}));
		}

		/// <summary>
		/// Flushes the list of <see cref="TriggerEventArgs"/> objects and 
		/// dispatches them to the users that are listening.
		/// </summary>
		/// <param name="eventList"></param>
		/// <remarks>
		/// This is called after the given connection has successfully 
		/// committed and closed.
		/// </remarks>
		internal void FlushTriggerEvents(IEnumerable<TriggerEventArgs> eventList) {
			foreach (TriggerEventArgs args in eventList) {
				FireTrigger(connection, args);
			}
		}

		/// <summary>
		/// Clears all the user triggers that have been defined.
		/// </summary>
		internal void ClearCallbackTriggers() {
			lock (this) {
				listenerMap.Remove(connection);
			}
		}

		/// <summary>
		/// Creates a new trigger action on a stored procedure and makes the change
		/// to the transaction of the underlying <see cref="DatabaseConnection"/>.
		/// </summary>
		/// <param name="schema">The schema name of the trigger.</param>
		/// <param name="name">The name of the trigger.</param>
		/// <param name="type">The type of trigger.</param>
		/// <param name="onTable">The table on which the trigger will be executed.</param>
		/// <param name="procedureName">The name of the procedure to execute.</param>
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
		public void CreateTableTrigger(string schema, string name, TriggerEventType type, TableName onTable, string procedureName, TObject[] parameters) {
			TableName triggerTableName = new TableName(schema, name);

			// Check this name is not reserved
			DatabaseConnection.CheckAllowCreate(triggerTableName);

			// Before adding the trigger, make sure this name doesn't already Resolve
			// to an object in the database with this schema/name.
			if (!connection.TableExists(triggerTableName)) {
				// Encode the parameters
				MemoryStream output = new MemoryStream();
				try {
					BinaryWriter writer = new BinaryWriter(output);
					writer.Write(1); // version

					MemoryStream objStream = new MemoryStream();
					BinaryFormatter formatter = new BinaryFormatter();
					formatter.Serialize(objStream, parameters);
					objStream.Flush();
					byte[] buf = objStream.ToArray();

					writer.Write(buf.Length);
					writer.Write(buf);
					writer.Flush();
				} catch (IOException e) {
					throw new Exception("IO Error: " + e.Message);
				}

				byte[] encodedParams = output.ToArray();

				// Insert the entry into the trigger table,
				DataTable table = connection.GetTable(SystemSchema.DataTrigger);
				DataRow row = new DataRow(table);
				row.SetValue(0, TObject.CreateString(schema));
				row.SetValue(1, TObject.CreateString(name));
				row.SetValue(2, TObject.CreateInt4((int)type));
				row.SetValue(3, TObject.CreateString("T:" + onTable));
				row.SetValue(4, TObject.CreateString(procedureName));
				row.SetValue(5, TObject.CreateObject(encodedParams));
				row.SetValue(6, TObject.CreateString(connection.User.UserName));
				table.Add(row);

				// Invalidate the list
				InvalidateTriggerList();

				// Notify that this database object has been successfully created.
				connection.DatabaseObjectCreated(triggerTableName);

				// Flag that this transaction modified the trigger table.
				triggerModified = true;
			} else {
				throw new Exception("Trigger name '" + schema + "." + name + "' already in use.");
			}
		}

		/// <summary>
		/// Adds a listener for an event with the given 'id' for the user session.
		/// </summary>
		/// <param name="triggerName"></param>
		/// <param name="eventType"></param>
		/// <param name="triggerSource"></param>
		/// <param name="handler"></param>
		/// <example>
		/// In the following example the handler is notified of all update 
		/// events on the 'Part' table:
		/// <code>
		/// AddTriggerHandler(user, "my_trigger", TriggerEventType.Update, "Part", my_handler);
		/// </code>
		/// </example>
		public void CreateCallbackTrigger(string triggerName, TriggerEventType eventType, TableName triggerSource, TriggerEventHandler handler) {
			lock (this) {
				// Has this trigger name already been defined for this user?
				IList<TriggerAction> list;
				if (listenerMap.TryGetValue(connection, out list)) {
					foreach (TriggerAction action in list) {
						if (action.TriggerName.Equals(triggerName))
							throw new ApplicationException("Duplicate trigger name '" + triggerName + "'");
					}
				} else {
					list = new List<TriggerAction>();
					listenerMap[connection] = list;
				}

				list.Add(new TriggerAction(connection, triggerName, eventType, triggerSource, handler));
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
		public void DropTrigger(string schema, string name) {
			IQueryContext context = new DatabaseQueryContext(connection);
			DataTable table = connection.GetTable(SystemSchema.DataTrigger);

			// Find the trigger
			Table t = FindTrigger(context, table, schema, name);

			if (t.RowCount == 0)
				throw new StatementException("Trigger '" + schema + "." + name + "' not found.");
			if (t.RowCount > 1)
				throw new Exception("Assertion failed: multiple entries for the same trigger name.");

			// Drop this trigger,
			table.Delete(t);

			// Notify that this database object has been successfully dropped.
			connection.DatabaseObjectDropped(new TableName(schema, name));

			// Flag that this transaction modified the trigger table.
			triggerModified = true;
		}

		/// <summary>
		/// Removes a trigger for the given user session.
		/// </summary>
		/// <param name="triggerName"></param>
		public void DropCallbackTrigger(string triggerName) {
			lock (this) {
				IList<TriggerAction> list;
				if (listenerMap.TryGetValue(connection, out  list)) {
					for (int i = list.Count - 1; i >= 0; i--) {
						TriggerAction action = list[i];
						if (action.TriggerName.Equals(triggerName)) {
							list.RemoveAt(i);
							if (list.Count == 0)
								listenerMap.Remove(connection);
							return;
						}
					}
				}
				throw new ApplicationException("Trigger name '" + triggerName + "' not found.");
			}
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
		public bool TriggerExists(string schema, string name) {
			IQueryContext context = new DatabaseQueryContext(connection);
			DataTable table = connection.GetTable(SystemSchema.DataTrigger);

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
			listValidated = false;
			triggersActive.Clear();
		}

		/// <summary>
		/// Build the trigger list if it is not validated.
		/// </summary>
		private void BuildTriggerList() {
			if (!listValidated) {
				// Cache the trigger table
				DataTable table = connection.GetTable(SystemSchema.DataTrigger);
				IRowEnumerator e = table.GetRowEnumerator();

				// For each row
				while (e.MoveNext()) {
					int rowIndex = e.RowIndex;

					TObject triggerSchema = table.GetCell(0, rowIndex);
					TObject triggerName = table.GetCell(1, rowIndex);
					TObject type = table.GetCell(2, rowIndex);
					TObject onObject = table.GetCell(3, rowIndex);
					TObject action = table.GetCell(4, rowIndex);
					TObject misc = table.GetCell(5, rowIndex);

					ProcedureTriggerInfo triggerInfo = new ProcedureTriggerInfo();
					triggerInfo.schema = triggerSchema.Object.ToString();
					triggerInfo.name = triggerName.Object.ToString();
					triggerInfo.type = (TriggerEventType) type.ToBigNumber().ToInt32();
					triggerInfo.on_object = onObject.Object.ToString();
					triggerInfo.action = action.Object.ToString();
					triggerInfo.misc = misc;

					// Add to the list
					triggersActive.Add(triggerInfo);
				}

				listValidated = true;
			}
		}

		/// <summary>
		/// Performs any trigger action for this event.
		/// </summary>
		/// <param name="args"></param>
		/// <remarks>
		/// For example, if we have it setup so a trigger fires when there is an 
		/// <c>INSERT</c> event on table x then we perform the triggering procedure right here.
		/// </remarks>
		internal void PerformTriggerAction(TriggerEventArgs args) {
			// REINFORCED NOTE: The 'TableExists' call is REALLY important.  First it
			//   makes sure the transaction on the connection is established (it should
			//   be anyway if a trigger is firing), and it also makes sure the trigger
			//   table exists - which it may not be during database init.
			if (connection.TableExists(SystemSchema.DataTrigger)) {
				// If the trigger list isn't built, then do so now
				BuildTriggerList();

				// On object value to test for,
				TableName tableName = args.Source;
				String on_ob_test = "T:" + tableName;

				// Search the triggers list for an event that matches this event
				foreach (ProcedureTriggerInfo tInfo in triggersActive) {
					if (tInfo.on_object.Equals(on_ob_test)) {
						// Table name matches
						// Do the types match?  eg. before/after match, and
						// insert/delete/update is being listened to.
						if (args.MatchesEventType(tInfo.type)) {
							// Type matches this trigger, so we need to fire it
							// Parse the action string
							string action = tInfo.action;

							// Get the procedure name to fire (qualify it against the schema
							// of the table being fired).
							RoutineName routineName = RoutineName.Qualify(tableName.Schema, action);

							// Set up OLD and NEW tables

							// Record the old table state
							DatabaseConnection.OldNewTableState currentState = connection.GetOldNewTableState();

							// Set the new table state
							// If an INSERT event then we setup NEW to be the row being inserted
							// If an DELETE event then we setup OLD to be the row being deleted
							// If an UPDATE event then we setup NEW to be the row after the
							// update, and OLD to be the row before the update.
							connection.SetOldNewTableState(new DatabaseConnection.OldNewTableState(tableName, args.OldRowIndex, args.NewDataRow,
							                                                                       args.IsBefore));

							try {
								// Invoke the procedure (no arguments)
								connection.RoutinesManager.InvokeRoutine(routineName, new TObject[0]);
							} finally {
								// Reset the OLD and NEW tables to previous values
								connection.SetOldNewTableState(currentState);
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
			private readonly ConnectionTriggerManager ctm;

			public CTMBackedCache(ConnectionTriggerManager ctm)
				: base(SystemSchema.DataTrigger) {
				this.ctm = ctm;
			}

			protected override void PurgeCache(IList<int> addedRows, IList<int> removedRows) {
				// Note that this is called when a transaction is started or stopped.

				// If the trigger table was modified, we need to invalidate the trigger
				// list.  This covers the case when we rollback a trigger table change
				if (ctm.triggerModified) {
					ctm.InvalidateTriggerList();
					ctm.triggerModified = false;
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

		#region ProcedureTriggerInfo

		/// <summary>
		/// Container class for all trigger actions defined on the database.
		/// </summary>
		private class ProcedureTriggerInfo {
			internal string action;
			internal TObject misc;
			internal String name;
			internal String on_object;
			internal String schema;
			internal TriggerEventType type;
		}

		#endregion

		#region TriggerAction
		
		/// <summary>
		/// Encapsulates the informations of a trigger event handler for a specific 
		/// event for a user.
		/// </summary>
		private sealed class TriggerAction {
			public readonly DatabaseConnection Connection;
			public readonly string TriggerName;   // The name of the trigger.
			public readonly TriggerEventHandler Handler;       // The trigger listener.
			public readonly TableName TriggerSource; // The source of the trigger.
			public readonly TriggerEventType TriggerEvent;  // Event we are to listen for.

			public TriggerAction(DatabaseConnection connection, string name, TriggerEventType type, TableName triggerSource, TriggerEventHandler handler) {
				Connection = connection;
				TriggerName = name;
				TriggerEvent = type;
				Handler = handler;
				TriggerSource = triggerSource;
			}
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
				: base(transaction, SystemSchema.DataTrigger) {
			}

			private static DataTableInfo CreateTableInfo(String schema, String name) {
				// Create the DataTableInfo that describes this entry
				DataTableInfo info = new DataTableInfo(new TableName(schema, name));

				// Add column definitions
				info.AddColumn("type", PrimitiveTypes.Numeric);
				info.AddColumn("on_object", PrimitiveTypes.VarString);
				info.AddColumn("procedure_name", PrimitiveTypes.VarString);
				info.AddColumn("param_args", PrimitiveTypes.VarString);
				info.AddColumn("owner", PrimitiveTypes.VarString);

				// Set to immutable
				info.IsReadOnly = true;

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

			public override ITableDataSource CreateInternalTable(int index) {
				ITableDataSource table = transaction.GetTable(SystemSchema.DataTrigger);
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
					String schema = table.GetCell(0, row_i).Object.ToString();
					String name = table.GetCell(1, row_i).Object.ToString();

					DataTableInfo tableInfo = CreateTableInfo(schema, name);
					TObject type = table.GetCell(2, row_i);
					TObject on_object = table.GetCell(3, row_i);
					TObject procedure_name = table.GetCell(4, row_i);
					TObject param_args = table.GetCell(5, row_i);
					TObject owner = table.GetCell(6, row_i);

					// Implementation of IMutableTableDataSource that describes this
					// trigger.
					GTDataSourceImpl int_table = new GTDataSourceImpl(transaction.Context, tableInfo);
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

				public GTDataSourceImpl(SystemContext context, DataTableInfo tableInfo)
					: base(context) {
					this.tableInfo = tableInfo;
				}

				public override DataTableInfo TableInfo {
					get { return tableInfo; }
				}

				public override int RowCount {
					get { return 1; }
				}

				public override TObject GetCell(int col, int row) {
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