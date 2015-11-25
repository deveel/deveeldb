// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Index;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Types;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The system implementation of a transaction model that handles
	/// isolated operations within a database context.
	/// </summary>
	/// <seealso cref="ITransaction"/>
	public sealed class Transaction : ITransaction, ICallbackHandler, ITableStateHandler {
		private List<TableCommitCallback> callbacks;

		private Action<TableCommitInfo> commitActions; 

		private static readonly TableInfo[] IntTableInfo;

		private readonly bool dbReadOnly;

		internal Transaction(ITransactionContext context, Database database, int commitId, IsolationLevel isolation, IEnumerable<TableSource> committedTables, IEnumerable<IIndexSet> indexSets) {
			CommitId = commitId;
			Database = database;
			Isolation = isolation;
		    TransactionContext = context;

			context.RegisterInstance<ITransaction>(this);

			Registry = new TransactionRegistry(this);
			TableManager.AddVisibleTables(committedTables, indexSets);

			AddInternalTables();

			TableState = new OldNewTableState();

			IsClosed = false;

			Database.TransactionFactory.OpenTransactions.AddTransaction(this);

			this.CurrentSchema(database.Context.DefaultSchema());
			this.ReadOnly(database.Context.ReadOnly());
			this.AutoCommit(database.Context.AutoCommit());
			this.IgnoreIdentifiersCase(database.Context.IgnoreIdentifiersCase());
			this.ParameterStyle(QueryParameterStyle.Marker);
		}

		internal Transaction(ITransactionContext context, Database database, int commitId, IsolationLevel isolation)
			: this(context, database, commitId, isolation, new TableSource[0], new IIndexSet[0]) {
		}

		~Transaction() {
			Dispose(false);
		}

		static Transaction() {
			IntTableInfo = new TableInfo[9];
			IntTableInfo[0] = SystemSchema.TableInfoTableInfo;
			IntTableInfo[1] = SystemSchema.TableColumnsTableInfo;
			IntTableInfo[2] = SystemSchema.ProductInfoTableInfo;
			IntTableInfo[3] = SystemSchema.VariablesTableInfo;
			IntTableInfo[4] = SystemSchema.StatisticsTableInfo;
			IntTableInfo[5] = SystemSchema.SessionInfoTableInfo;
			IntTableInfo[6] = SystemSchema.OpenSessionsTableInfo;
			IntTableInfo[7] = SystemSchema.SqlTypesTableInfo;
			IntTableInfo[8] = SystemSchema.PrivilegesTableInfo;
		}

		public int CommitId { get; private set; }

		public IsolationLevel Isolation { get; private set; }

		private bool IsClosed { get; set; }

		public OldNewTableState TableState { get; private set; }

        public ITransactionContext TransactionContext { get; private set; }

		public void SetTableState(OldNewTableState tableState) {
			TableState = tableState;
		}

		IDatabase ITransaction.Database {
			get { return Database; }
		}

		public Database Database { get; private set; }

		IEventSource IEventSource.ParentSource {
			get { return Database; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return new KeyValuePair<string, object>[0];}
		}
			
		IContext IEventSource.Context {
			get { return TransactionContext; }
		}

		public IDatabaseContext DatabaseContext {
			get { return Database.Context; }
		}

		private TableSourceComposite TableComposite {
			get { return Database.TableComposite; }
		}

		private SequenceManager SequenceManager {
			get { return (SequenceManager) this.GetObjectManager(DbObjectType.Sequence); }
		}

		private ViewManager ViewManager {
			get { return (ViewManager) this.GetObjectManager(DbObjectType.View); }
		}

		private TriggerManager TriggerManager {
			get { return (TriggerManager) this.GetObjectManager(DbObjectType.Trigger); }
		}

		public TransactionRegistry Registry { get; private set; }

		private TableManager TableManager {
			get { return this.GetTableManager(); }
		}

		private void AddInternalTables() {
			TableManager.AddInternalTables(new TransactionTableContainer(this, IntTableInfo));

			// OLD and NEW system tables (if applicable)
			TableManager.AddInternalTables(new OldAndNewTableContainer(this));

			// Model views as tables (obviously)
			TableManager.AddInternalTables(ViewManager.CreateInternalTableInfo());

			//// Model procedures as tables
			//tableManager.AddInternalTables(routineManager.CreateInternalTableInfo());

			// Model sequences as tables
			TableManager.AddInternalTables(SequenceManager.TableContainer);

			// Model triggers as tables
			TableManager.AddInternalTables(TriggerManager.CreateTriggersTableContainer());
		}

		private void AssertNotReadOnly() {
			if (this.ReadOnly())
				throw new TransactionException(TransactionErrorCodes.ReadOnly, "The transaction is in read-only mode.");
		}

		public void Commit() {
			if (!IsClosed) {
				try {
					var touchedTables = TableManager.AccessedTables.ToList();
					var visibleTables = TableManager.GetVisibleTables().ToList();
					var selected = TableManager.SelectedTables.ToArray();
					TableComposite.Commit(this, visibleTables, selected, touchedTables, Registry, commitActions);
				} finally {
					Finish();
				}
			}
		}

		public void RegisterOnCommit(Action<TableCommitInfo> action) {
			if (commitActions == null) {
				commitActions = action;
			} else {
				commitActions = Delegate.Combine(commitActions, action) as Action<TableCommitInfo>;
			}
		}

		public void UnregisterOnCommit(Action<TableCommitInfo> action) {
			if (commitActions != null)
				commitActions = Delegate.Remove(commitActions, action) as Action<TableCommitInfo>;
		}


		private void Finish() {
			try {
				// Dispose all the table we touched
				try {
					TableManager.Dispose();

					if (TransactionContext != null)
						TransactionContext.Dispose();

				} catch (Exception) {
					// TODO: report the error
				}

				Registry = null;

				if (callbacks != null) {
					foreach (var callback in callbacks) {
						callback.OnTransactionEnd();
						callback.DetachFrom(this);
					}
				}

				callbacks = null;
				TransactionContext = null;

				// Dispose all the objects in the transaction
			} finally {
				IsClosed = true;
			}
		}

		public void Rollback() {
			if (!IsClosed) {
				try {
					var touchedTables = TableManager.AccessedTables.ToList();
					TableComposite.Rollback(this, touchedTables, Registry);
				} finally {
					IsClosed = true;
					Finish();
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				//if (!IsClosed)
				//	Rollback();
				// TODO: review this ...
				Finish();
			}
		}

		#region TransactionTableContainer

		class TransactionTableContainer : ITableContainer {
			private readonly Transaction transaction;
			private readonly TableInfo[] tableInfos;

			public TransactionTableContainer(Transaction transaction, TableInfo[] tableInfos) {
				this.transaction = transaction;
				this.tableInfos = tableInfos;
			}

			public int TableCount {
				get { return tableInfos.Length; }
			}

			public int FindByName(ObjectName name) {
				var ignoreCase = transaction.IgnoreIdentifiersCase();
				for (int i = 0; i < tableInfos.Length; i++) {
					var info = tableInfos[i];
					if (info != null && 
						info.TableName.Equals(name, ignoreCase))
						return i;
				}

				return -1;
			}

			public ObjectName GetTableName(int offset) {
				if (offset < 0 || offset >= tableInfos.Length)
					throw new ArgumentOutOfRangeException("offset");

				return tableInfos[offset].TableName;
			}

			public TableInfo GetTableInfo(int offset) {
				if (offset < 0 || offset >= tableInfos.Length)
					throw new ArgumentOutOfRangeException("offset");

				return tableInfos[offset];
			}

			public string GetTableType(int offset) {
				return TableTypes.SystemTable;
			}

			public bool ContainsTable(ObjectName name) {
				return FindByName(name) >= 0;
			}

			public ITable GetTable(int offset) {
				if (offset == 0)
					return SystemSchema.GetTableInfoTable(transaction);
				if (offset == 1)
					return SystemSchema.GetTableColumnsTable(transaction);
				if (offset == 2)
					return SystemSchema.GetProductInfoTable(transaction);
				if (offset == 3)
					return SystemSchema.GetVariablesTable(transaction);
				if (offset == 4)
					return SystemSchema.GetStatisticsTable(transaction);
				/*
				TODO:
				if (offset == 5)
					return SystemSchema.GetSessionInfoTable(transaction);
				*/
				if (offset == 6)
					return SystemSchema.GetOpenSessionsTable(transaction);
				if (offset == 7)
					return SystemSchema.GetSqlTypesTable(transaction);
				if (offset == 8)
					return SystemSchema.GetPrivilegesTable(transaction);

				throw new ArgumentOutOfRangeException("offset");
			}
		}

		#endregion

		#region OldAndNewTableContainer

		class OldAndNewTableContainer : ITableContainer {
			private readonly Transaction transaction;

			public OldAndNewTableContainer(Transaction transaction) {
				this.transaction = transaction;
			}

			private bool HasOldTable {
				get { return transaction.TableState.OldRowIndex != -1; }
			}

			private bool HasNewTable {
				get { return transaction.TableState.NewDataRow != null; }
			}


			public int TableCount {
				get {
					int count = 0;
					if (HasOldTable)
						++count;
					if (HasNewTable)
						++count;
					return count;
				}
			}

			public int FindByName(ObjectName name) {
				if (HasOldTable &&
				    name.Equals(SystemSchema.OldTriggerTableName, transaction.IgnoreIdentifiersCase()))
					return 0;
				if (HasNewTable &&
				    name.Equals(SystemSchema.NewTriggerTableName, transaction.IgnoreIdentifiersCase()))
					return HasOldTable ? 1 : 0;
				return -1;
			}

			public ObjectName GetTableName(int offset) {
				if (HasOldTable && offset == 0)
					return SystemSchema.OldTriggerTableName;

				return SystemSchema.NewTriggerTableName;
			}

			public TableInfo GetTableInfo(int offset) {
				var tableInfo = transaction.GetTableInfo(transaction.TableState.TableSource);
				return tableInfo.Alias(GetTableName(offset));
			}

			public string GetTableType(int offset) {
				return TableTypes.SystemTable;
			}

			public bool ContainsTable(ObjectName name) {
				return FindByName(name) > 0;
			}

			public ITable GetTable(int offset) {
				var tableInfo = GetTableInfo(offset);

				var table = new TriggeredOldNew(transaction.DatabaseContext, tableInfo);

				if (HasOldTable) {
					if (offset == 0) {
						// Copy data from the table to the new table
						var dtable = transaction.GetTable(transaction.TableState.TableSource);
						var oldRow = new Row(table);
						int rowIndex = transaction.TableState.OldRowIndex;
						for (int i = 0; i < tableInfo.ColumnCount; ++i) {
							oldRow.SetValue(i, dtable.GetValue(rowIndex, i));
						}

						// All OLD tables are immutable
						table.SetReadOnly(true);
						table.SetData(oldRow);

						return table;
					}
				}

				table.SetReadOnly(!transaction.TableState.IsNewMutable);
				table.SetData(transaction.TableState.NewDataRow);

				return table;
			}

			#region TriggeredOldNew

			class TriggeredOldNew : GeneratedTable, IMutableTable {
				private readonly TableInfo tableInfo;
				private Row data;
				private bool readOnly;

				public TriggeredOldNew(IDatabaseContext dbContext, TableInfo tableInfo) 
					: base(dbContext) {
					this.tableInfo = tableInfo;
				}

				public override TableInfo TableInfo {
					get { return tableInfo; }
				}

				public override int RowCount {
					get { return 1; }
				}

				public void SetData(Row row) {
					data = row;
				}

				public void SetReadOnly(bool flag) {
					readOnly = flag;
				}

				public override DataObject GetValue(long rowNumber, int columnOffset) {
					if (rowNumber < 0 || rowNumber >= 1)
						throw new ArgumentOutOfRangeException("rowNumber");

					return data.GetValue(columnOffset);
				}

				public TableEventRegistry EventRegistry {
					get { throw new InvalidOperationException(); }
				}

				public RowId AddRow(Row row) {
					throw new NotSupportedException(String.Format("Inserting data into '{0}' is not allowed.", tableInfo.TableName));
				}

				public void UpdateRow(Row row) {
					if (row.RowId.RowNumber < 0 ||
						row.RowId.RowNumber >= 1)
						throw new ArgumentOutOfRangeException();
					if (readOnly)
						throw new NotSupportedException(String.Format("Updating '{0}' is not permitted.", tableInfo.TableName));

					int sz = TableInfo.ColumnCount;
					for (int i = 0; i < sz; ++i) {
						data.SetValue(i, row.GetValue(i));
					}
				}

				public bool RemoveRow(RowId rowId) {
					throw new NotSupportedException(String.Format("Deleting data from '{0}' is not allowed.", tableInfo.TableName));
				}

				public void FlushIndexes() {
				}

				public void AssertConstraints() {
				}

				public void AddLock() {
				}

				public void RemoveLock() {
				}
			}

			#endregion
		}

		#endregion

		//#region ObjectManagersResolver

		//class ObjectManagersResolver : IObjectManagerResolver {
		//	private readonly Transaction transaction;

		//	public ObjectManagersResolver(Transaction transaction) {
		//		this.transaction = transaction;
		//	}

		//	public IEnumerable<IObjectManager> ResolveAll() {
		//		return new IObjectManager[] {
		//			transaction.schemaManager,
		//			transaction.tableManager,
		//			transaction.sequenceManager,
		//			transaction.variableManager,
		//			transaction.viewManager,
		//			transaction.triggerManager
		//		};
		//	}

		//	public IObjectManager ResolveForType(DbObjectType objType) {
		//		if (objType == DbObjectType.Schema)
		//			return transaction.schemaManager;
		//		if (objType == DbObjectType.Table)
		//			return transaction.tableManager;
		//		if (objType == DbObjectType.Sequence)
		//			return transaction.sequenceManager;
		//		if (objType == DbObjectType.Variable)
		//			return transaction.variableManager;
		//		if (objType == DbObjectType.View)
		//			return transaction.viewManager;
		//		if (objType == DbObjectType.Trigger)
		//			return transaction.triggerManager;

		//		return null;
		//	}
		//}

		//#endregion

		#region Variables
		/*
		void IVariableScope.OnVariableDefined(Variable variable) {
			if (variable.Name.Equals(TransactionSettingKeys.CurrentSchema, StringComparison.OrdinalIgnoreCase)) {
				currentSchema = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.ReadOnly, StringComparison.OrdinalIgnoreCase)) {
				if (dbReadOnly)
					throw new InvalidOperationException("The database is read-only: cannot change access of the transaction.");

				// TODO: handle special cases like "ON", "OFF", "ENABLE" and "DISABLE"
				readOnly = ParseBoolean(variable.Value);
			} else if (variable.Name.Equals(TransactionSettingKeys.IgnoreIdentifiersCase, StringComparison.OrdinalIgnoreCase)) {
				ignoreCase = ParseBoolean(variable.Value);
			} else if (variable.Name.Equals(TransactionSettingKeys.AutoCommit, StringComparison.OrdinalIgnoreCase)) {
				autoCommit = ParseBoolean(variable.Value);
			} else if (variable.Name.Equals(TransactionSettingKeys.ParameterStyle, StringComparison.OrdinalIgnoreCase)) {
				parameterStyle = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.IsolationLevel, StringComparison.OrdinalIgnoreCase)) {
				var isolation = ParseIsolationLevel(variable.Value);
				//TODO: support multiple isolations!
				if (isolation != IsolationLevel.Serializable)
					throw new NotSupportedException();
			}
		}

		private static IsolationLevel ParseIsolationLevel(DataObject value) {
			var s = value.Value.ToString();
			if (String.Equals(s, "serializable", StringComparison.OrdinalIgnoreCase))
				return IsolationLevel.Serializable;
			if (String.Equals(s, "read committed", StringComparison.OrdinalIgnoreCase))
				return IsolationLevel.ReadCommitted;
			if (String.Equals(s, "read uncommitted", StringComparison.OrdinalIgnoreCase))
				return IsolationLevel.ReadUncommitted;
			if (String.Equals(s, "snapshot", StringComparison.OrdinalIgnoreCase))
				return IsolationLevel.Snapshot;

			return IsolationLevel.Unspecified;
		}

		private static bool ParseBoolean(DataObject value) {
			if (value.Type is BooleanType)
				return value;
			if (value.Type is StringType) {
				var s = value.Value.ToString();
				if (String.Equals(s, "true", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals(s, "on", StringComparison.OrdinalIgnoreCase))
					return true;
				if (String.Equals(s, "false", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals(s, "off", StringComparison.OrdinalIgnoreCase))
					return false;
			} else if (value.Type is NumericType) {
				int i = value;
				if (i == 0)
					return false;
				if (i == 1)
					return true;
			}

			throw new NotSupportedException();
		}

		void IVariableScope.OnVariableDropped(Variable variable) {
			if (variable.Name.Equals(TransactionSettingKeys.CurrentSchema, StringComparison.OrdinalIgnoreCase)) {
				currentSchema = Database.DatabaseContext.DefaultSchema();
			} else if (variable.Name.Equals(TransactionSettingKeys.ReadOnly, StringComparison.OrdinalIgnoreCase)) {
				readOnly = dbReadOnly;
			} else if (variable.Name.Equals(TransactionSettingKeys.IgnoreIdentifiersCase, StringComparison.OrdinalIgnoreCase)) {
				ignoreCase = Database.DatabaseContext.IgnoreIdentifiersCase();
			} else if (variable.Name.Equals(TransactionSettingKeys.AutoCommit, StringComparison.OrdinalIgnoreCase)) {
				autoCommit = Database.DatabaseContext.AutoCommit();
			} else if (variable.Name.Equals(TransactionSettingKeys.ParameterStyle, StringComparison.OrdinalIgnoreCase)) {
				// TODO: Get it from the configuration...
				parameterStyle = null;
			}
		}
		
		private Variable MakeStringVariable(string name, string value) {
			var variable = new Variable(new VariableInfo(name, PrimitiveTypes.String(), false));
			variable.SetValue(DataObject.String(value));
			return variable;
		}

		private Variable MakeBooleanVariable(string name, bool value) {
			var variable = new Variable(new VariableInfo(name, PrimitiveTypes.Boolean(), false));
			variable.SetValue(DataObject.Boolean(value));
			return variable;
		}

		Variable IVariableScope.OnVariableGet(string name) {
			if (name.Equals(TransactionSettingKeys.CurrentSchema, StringComparison.OrdinalIgnoreCase))
				return MakeStringVariable(TransactionSettingKeys.CurrentSchema, currentSchema);
			if (name.Equals(TransactionSettingKeys.ReadOnly, StringComparison.OrdinalIgnoreCase))
				return MakeBooleanVariable(TransactionSettingKeys.ReadOnly, readOnly);
			if (name.Equals(TransactionSettingKeys.IgnoreIdentifiersCase, StringComparison.OrdinalIgnoreCase))
				return MakeBooleanVariable(TransactionSettingKeys.IgnoreIdentifiersCase, ignoreCase);
			if (name.Equals(TransactionSettingKeys.AutoCommit, StringComparison.OrdinalIgnoreCase))
				return MakeBooleanVariable(TransactionSettingKeys.AutoCommit, autoCommit);
			if (name.Equals(TransactionSettingKeys.ParameterStyle, StringComparison.OrdinalIgnoreCase))
				return MakeStringVariable(TransactionSettingKeys.ParameterStyle, parameterStyle);

			return null;
		}
		*/

		#endregion

		void ICallbackHandler.OnCallbackAttached(TableCommitCallback callback) {
			if (callbacks == null)
				callbacks = new List<TableCommitCallback>();

			callbacks.Add(callback);
		}

		void ICallbackHandler.OnCallbackDetached(TableCommitCallback callback) {
			if (callbacks == null)
				return;

			for (int i = callbacks.Count - 1; i >= 0; i--) {
				var other = callbacks[i];
				if (other.TableName.Equals(callback.TableName))
					callbacks.RemoveAt(i);
			}
		}
	}
}
