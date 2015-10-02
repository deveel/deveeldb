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

using Deveel.Data;
using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Types;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The system implementation of a transaction model that handles
	/// isolated operations within a database context.
	/// </summary>
	/// <seealso cref="ITransaction"/>
	public sealed class Transaction : ITransaction, ICallbackHandler {
		private TableManager tableManager;
		private SequenceManager sequenceManager;
		private ViewManager viewManager;
		private VariableManager variableManager;
		private SchemaManager schemaManager;
		private List<TableCommitCallback> callbacks;

		private Action<TableCommitInfo> commitActions; 

		private static readonly TableInfo[] IntTableInfo;

		private string currentSchema;
		private bool readOnly;
		private readonly bool dbReadOnly;
		private bool ignoreCase;
		private bool autoCommit;
		private string parameterStyle;

		internal Transaction(Database database, int commitId, TransactionIsolation isolation, IEnumerable<TableSource> committedTables, IEnumerable<IIndexSet> indexSets) {
			CommitId = commitId;
			Database = database;
			Isolation = isolation;

			InitManagers();

			Registry = new TransactionRegistry(this);
			tableManager.AddVisibleTables(committedTables, indexSets);

			AddInternalTables();

			OldNewTableState = new OldNewTableState();

			IsClosed = false;

			Database.TransactionFactory.OpenTransactions.AddTransaction(this);

			currentSchema = database.DatabaseContext.DefaultSchema();
			readOnly = dbReadOnly = database.DatabaseContext.ReadOnly();
			autoCommit = database.DatabaseContext.AutoCommit();
			ignoreCase = database.DatabaseContext.IgnoreIdentifiersCase();
		}

		internal Transaction(Database database, int commitId, TransactionIsolation isolation)
			: this(database, commitId, isolation, new TableSource[0], new IIndexSet[0]) {
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

		object ILockable.RefId {
			get { return CommitId; }
		}

		public TransactionIsolation Isolation { get; private set; }

		private bool IsClosed { get; set; }

		public OldNewTableState OldNewTableState { get; private set; }

		IDatabase ITransaction.Database {
			get { return Database; }
		}

		public Database Database { get; private set; }

		public IDatabaseContext DatabaseContext {
			get { return Database.DatabaseContext; }
		}

		private TableSourceComposite TableComposite {
			get { return Database.TableComposite; }
		}

		public IObjectManagerResolver Managers { get; private set; }

		public TransactionRegistry Registry { get; private set; }

		private void InitManagers() {
			schemaManager = new SchemaManager(this);
			tableManager = new TableManager(this, TableComposite);
			sequenceManager = new SequenceManager(this);
			viewManager = new ViewManager(this);
			variableManager = new VariableManager(this);

			Managers = new ObjectManagersResolver(this);
		}

		private void AddInternalTables() {
			tableManager.AddInternalTables(new TransactionTableContainer(this, IntTableInfo));

			// OLD and NEW system tables (if applicable)
			tableManager.AddInternalTables(new OldAndNewTableContainer(this));

			// Model views as tables (obviously)
			tableManager.AddInternalTables(viewManager.CreateInternalTableInfo());

			//// Model procedures as tables
			//tableManager.AddInternalTables(routineManager.CreateInternalTableInfo());

			// Model sequences as tables
			tableManager.AddInternalTables(sequenceManager.TableContainer);

			// Model triggers as tables
			//tableManager.AddInternalTables(triggerManager.CreateInternalTableInfo());
		}

		private void AssertNotReadOnly() {
			if (this.ReadOnly())
				throw new TransactionException(TransactionErrorCodes.ReadOnly, "The transaction is in read-only mode.");
		}

		void ILockable.Acquired(Lock @lock) {
		}

		void ILockable.Released(Lock @lock) {
		}

		public void Commit() {
			if (!IsClosed) {
				try {
					var touchedTables = tableManager.AccessedTables.ToList();
					var visibleTables = tableManager.GetVisibleTables().ToList();
					var selected = tableManager.SelectedTables.ToArray();
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
					tableManager.Dispose();
				} catch (Exception e) {
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

				// Dispose all the objects in the transaction
			} finally {
				IsClosed = true;
			}
		}

		public void Rollback() {
			if (!IsClosed) {
				try {
					var touchedTables = tableManager.AccessedTables.ToList();
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
				return FindByName(name) > 0;
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
				get { return transaction.OldNewTableState.OldRowIndex != -1; }
			}

			private bool HasNewTable {
				get { return transaction.OldNewTableState.NewDataRow != null; }
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
				var tableInfo = transaction.GetTableInfo(transaction.OldNewTableState.TableSource);
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
						var dtable = transaction.GetTable(transaction.OldNewTableState.TableSource);
						var oldRow = new Row(table);
						int rowIndex = transaction.OldNewTableState.OldRowIndex;
						for (int i = 0; i < tableInfo.ColumnCount; ++i) {
							oldRow.SetValue(i, dtable.GetValue(rowIndex, i));
						}

						// All OLD tables are immutable
						table.SetReadOnly(true);
						table.SetData(oldRow);

						return table;
					}
				}

				table.SetReadOnly(!transaction.OldNewTableState.IsNewMutable);
				table.SetData(transaction.OldNewTableState.NewDataRow);

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

		#region ObjectManagersResolver

		class ObjectManagersResolver : IObjectManagerResolver {
			private readonly Transaction transaction;

			public ObjectManagersResolver(Transaction transaction) {
				this.transaction = transaction;
			}

			public IEnumerable<IObjectManager> ResolveAll() {
				return new IObjectManager[] {
					transaction.schemaManager,
					transaction.tableManager,
					transaction.sequenceManager,
					transaction.variableManager,
					transaction.viewManager
				};
			}

			public IObjectManager ResolveForType(DbObjectType objType) {
				if (objType == DbObjectType.Schema)
					return transaction.schemaManager;
				if (objType == DbObjectType.Table)
					return transaction.tableManager;
				if (objType == DbObjectType.Sequence)
					return transaction.sequenceManager;
				if (objType == DbObjectType.Variable)
					return transaction.variableManager;
				if (objType == DbObjectType.View)
					return transaction.viewManager;

				return null;
			}
		}

		#endregion

		DataObject IVariableResolver.Resolve(ObjectName variable) {
			throw new NotImplementedException();
		}

		SqlType IVariableResolver.ReturnType(ObjectName variable) {
			throw new NotImplementedException();
		}

		void IVariableScope.OnVariableDefined(Variable variable) {
			if (variable.Name.Equals(TransactionSettingKeys.CurrentSchema, StringComparison.OrdinalIgnoreCase)) {
				currentSchema = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.ReadOnly, StringComparison.OrdinalIgnoreCase)) {
				if (dbReadOnly)
					throw new InvalidOperationException("The database is read-only: cannot change access of the transaction.");

				// TODO: handle special cases like "ON", "OFF", "ENABLE" and "DISABLE"
				readOnly = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.IgnoreIdentifiersCase, StringComparison.OrdinalIgnoreCase)) {
				ignoreCase = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.AutoCommit, StringComparison.OrdinalIgnoreCase)) {
				autoCommit = variable.Value;
			} else if (variable.Name.Equals(TransactionSettingKeys.ParameterStyle, StringComparison.OrdinalIgnoreCase)) {
				parameterStyle = variable.Value;
			}
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
