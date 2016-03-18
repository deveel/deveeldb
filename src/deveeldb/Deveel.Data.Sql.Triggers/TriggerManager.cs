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
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerManager : IObjectManager, ITriggerManager {
		private ITransaction transaction;
		private bool tableModified;
		private bool cacheValid;
		private List<Trigger> triggerCache; 

		public TriggerManager(ITransaction transaction) {
			this.transaction = transaction;
			triggerCache = new List<Trigger>(24);
			this.transaction.RegisterOnCommit(OnCommit);
		}

		~TriggerManager() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (triggerCache != null)
					triggerCache.Clear();
			}

			triggerCache = null;
			transaction = null;
			cacheValid = false;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Trigger; }
		}

		private void OnTableCommit(TableCommitEvent commitEvent) {
			if (tableModified) {
				InvalidateTriggerCache();
				tableModified = false;
			} else if ((commitEvent.AddedRows != null &&
			            commitEvent.AddedRows.Any()) ||
			           (commitEvent.RemovedRows != null &&
			            commitEvent.RemovedRows.Any())) {
				InvalidateTriggerCache();
			}
		}

		private void OnCommit(TableCommitInfo commitInfo) {
			if (tableModified) {
				InvalidateTriggerCache();
				tableModified = false;
			} else if ((commitInfo.AddedRows != null &&
			     commitInfo.AddedRows.Any()) ||
			    (commitInfo.RemovedRows != null &&
			     commitInfo.RemovedRows.Any())) {
				InvalidateTriggerCache();
			}
		}

		private void BuildTriggerCache() {
			if (!cacheValid) {
				var table = transaction.GetTable(SystemSchema.TriggerTableName);

				var list =  new List<Trigger>();
				foreach (var row in table) {
					var triggerInfo = FormTrigger(row);
					list.Add(new Trigger(triggerInfo));
				}

				triggerCache = new List<Trigger>(list);
				cacheValid = true;
			}
		}

		private void InvalidateTriggerCache() {
			cacheValid = false;
		}

		public void Create() {
			var tableInfo = new TableInfo(SystemSchema.TriggerTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Integer());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("action", PrimitiveTypes.Integer());
			tableInfo.AddColumn("procedure_name", PrimitiveTypes.String());
			tableInfo.AddColumn("args", PrimitiveTypes.Binary());
			transaction.CreateTable(tableInfo);
		}

		private ITable FindTrigger(ITable table, string schema, string name) {
			// Find all the trigger entries with this name
			var schemaColumn = table.GetResolvedColumnName(0);
			var nameColumn = table.GetResolvedColumnName(1);

			using (var session = new SystemSession(transaction, SystemSchema.Name)) {
				using (var context = session.CreateQuery()) {
					var t = table.SimpleSelect(context, nameColumn, SqlExpressionType.Equal,
						SqlExpression.Constant(Field.String(name)));
					return t.ExhaustiveSelect(context,
						SqlExpression.Equal(SqlExpression.Reference(schemaColumn), SqlExpression.Constant(Field.String(schema))));
				}
			}
		}

		private IEnumerable<TriggerInfo> FindTriggers(ObjectName tableName, TriggerEventType eventType) {
			var fullTableName = tableName.FullName;
			var eventTypeCode = (int)eventType;

			var table = transaction.GetTable(SystemSchema.TriggerTableName);
			if (table == null)
				return new TriggerInfo[0];

			var tableColumn = table.GetResolvedColumnName(3);
			var eventTypeColumn = table.GetResolvedColumnName(4);

			ITable result;
			using (var session = new SystemSession(transaction, SystemSchema.Name)) {
				using (var context = session.CreateQuery()) {
					var t = table.SimpleSelect(context, tableColumn, SqlExpressionType.Equal,
						SqlExpression.Constant(Field.String(fullTableName)));

					result = t.ExhaustiveSelect(context,
						SqlExpression.Equal(SqlExpression.Reference(eventTypeColumn), SqlExpression.Constant(eventTypeCode)));
				}
			}

			if (result.RowCount == 0)
				return new TriggerInfo[0];

			var list = new List<TriggerInfo>();

			foreach (var row in result) {
				var triggerInfo = FormTrigger(row);

				//TODO: get the other information such has the body, the external method or the procedure
				//      if this is a non-callback

				list.Add(triggerInfo);
			}

			return list.AsEnumerable();
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var triggerInfo = objInfo as TriggerInfo;
			if (triggerInfo == null)
				throw new ArgumentException();

			CreateTrigger(triggerInfo);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return TriggerExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return TriggerExists(objName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetTrigger(objName);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			var triggerInfo = objInfo as TriggerInfo;
			if (triggerInfo == null)
				throw new ArgumentException();

			return AlterTrigger(triggerInfo);
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropTrigger(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			// TODO: implement!!!
			return null;
		}

		[Serializable]
		class TriggerArgument : ISerializable {
			public TriggerArgument(SqlExpression[] args) {
				Arguments = args;
			}

			private TriggerArgument(SerializationInfo info, StreamingContext context) {
				Arguments = (SqlExpression[]) info.GetValue("Arguments", typeof (SqlExpression[]));
			}

			public SqlExpression[] Arguments { get; private set; }

			void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
				info.AddValue("Arguments", Arguments);
			}
		}

		private static byte[] SerializeArguments(TriggerArgument args) {
			using (var stream = new MemoryStream()) {
				var serializer = new BinarySerializer();
				serializer.Serialize(stream, args);

				stream.Flush();

				return stream.ToArray();
			}
		}

		public void CreateTrigger(TriggerInfo triggerInfo) {
			if (!transaction.TableExists(SystemSchema.TriggerTableName))
				return;

			try {
				var args = new TriggerArgument(triggerInfo.Arguments.ToArray());
				var binArgs = SerializeArguments(args);

				var schema = triggerInfo.TriggerName.ParentName;
				var name = triggerInfo.TriggerName.Name;
				var type = (int) triggerInfo.TriggerType;
				var onTable = triggerInfo.TableName == null ? null : triggerInfo.TableName.FullName;
				var procedureName = triggerInfo.ProcedureName != null ? triggerInfo.ProcedureName.FullName : null;

				var action = (int) triggerInfo.EventType;
				
				// TODO: if the trigger has a body, create a special procedure and set the name

				// Insert the entry into the trigger table,
				var table = transaction.GetMutableTable(SystemSchema.TriggerTableName);
				var row = table.NewRow();
				row.SetValue(0, Field.String(schema));
				row.SetValue(1, Field.String(name));
				row.SetValue(2, Field.Integer(type));
				row.SetValue(3, Field.String(onTable));
				row.SetValue(4, Field.Integer(action));
				row.SetValue(5, Field.String(procedureName));
				row.SetValue(6, Field.Binary(binArgs));
				table.AddRow(row);

				InvalidateTriggerCache();

				transaction.Registry.RegisterEvent(new ObjectCreatedEvent(triggerInfo.TriggerName, DbObjectType.Trigger));

				tableModified = true;
			} catch (Exception) {
				// TODO: use a specialized exception
				throw;
			}
		}

		public bool DropTrigger(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public bool TriggerExists(ObjectName triggerName) {
			var table = transaction.GetTable(SystemSchema.TriggerTableName);
			var result = FindTrigger(table, triggerName.ParentName, triggerName.Name);
			if (result.RowCount == 0)
				return false;

			if (result.RowCount > 1)
				throw new InvalidOperationException(String.Format("More than one trigger found with name '{0}'.", triggerName));

			return true;
		}

		public Trigger GetTrigger(ObjectName triggerName) {
			var table = transaction.GetTable(SystemSchema.TriggerTableName);
			var result = FindTrigger(table, triggerName.ParentName, triggerName.Name);
			if (result.RowCount == 0)
				return null;

			if (result.RowCount > 1)
				throw new InvalidOperationException(String.Format("More than one trigger found with name '{0}'.", triggerName));

			var triggerInfo = FormTrigger(result.First());
			return new Trigger(triggerInfo);
		}

		private TriggerInfo FormTrigger(Row row) {
			var schema = row.GetValue(0).Value.ToString();
			var name = row.GetValue(1).Value.ToString();
			var triggerName = new ObjectName(new ObjectName(schema), name);

			var triggerType = (TriggerType)((SqlNumber)row.GetValue(2).Value).ToInt32();

			// TODO: In case it's  a procedural trigger, take the reference to the procedure
			if (triggerType == TriggerType.Procedure)
				throw new NotImplementedException();

			var tableName = ObjectName.Parse(((SqlString) row.GetValue(3).Value).ToString());
			var eventType = (TriggerEventType) ((SqlNumber) row.GetValue(4).Value).ToInt32();
			return  new TriggerInfo(triggerName, triggerType, eventType, tableName);
		}

		public bool AlterTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public IEnumerable<Trigger> FindTriggers(TriggerEventInfo eventInfo) {
			var triggers = FindTriggers(eventInfo.TableName, eventInfo.EventType);
			return triggers.Select(x => new Trigger(x));
		}

		public void FireTriggers(IRequest context, TableEvent tableEvent) {
			if (!transaction.TableExists(SystemSchema.TriggerTableName))
				return;

			BuildTriggerCache();

			foreach (var trigger in triggerCache) {
				if (trigger.CanInvoke(tableEvent))
					trigger.Invoke(context, tableEvent);
			}
		}

		public ITableContainer CreateTriggersTableContainer() {
			return new TriggersTableContainer(transaction);
		}

		#region TriggersTableContainer

		class TriggersTableContainer : SystemTableContainer {
			public TriggersTableContainer(ITransaction transaction) 
				: base(transaction, SystemSchema.TriggerTableName) {
			}

			public override TableInfo GetTableInfo(int offset) {
				var triggerName = GetTableName(offset);
				return CreateTableInfo(triggerName.ParentName, triggerName.Name);
			}

			public override string GetTableType(int offset) {
				return TableTypes.Trigger;
			}

			private static TableInfo CreateTableInfo(string schema, string name) {
				var tableInfo = new TableInfo(new ObjectName(new ObjectName(schema), name));

				tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
				tableInfo.AddColumn("on_object", PrimitiveTypes.String());
				tableInfo.AddColumn("routine_name", PrimitiveTypes.String());
				tableInfo.AddColumn("param_args", PrimitiveTypes.String());
				tableInfo.AddColumn("owner", PrimitiveTypes.String());

				return tableInfo.AsReadOnly();
			}


			public override ITable GetTable(int offset) {
				var table = Transaction.GetTable(SystemSchema.TriggerTableName);
				var enumerator = table.GetEnumerator();
				int p = 0;
				int i;
				int rowIndex = -1;
				while (enumerator.MoveNext()) {
					i = enumerator.Current.RowId.RowNumber;
					if (p == offset) {
						rowIndex = i;
					} else {
						++p;
					}
				}

				if (p != offset)
					throw new ArgumentOutOfRangeException("offset");

				var schema = table.GetValue(rowIndex, 0).Value.ToString();
				var name = table.GetValue(rowIndex, 1).Value.ToString();

				var tableInfo = CreateTableInfo(schema, name);

				var type = table.GetValue(rowIndex, 2);
				var tableName = table.GetValue(rowIndex, 3);
				var routine = table.GetValue(rowIndex, 4);
				var args = table.GetValue(rowIndex, 5);
				var owner = table.GetValue(rowIndex, 6);

				return new TriggerTable(Transaction, tableInfo) {
					Type = type,
					TableName = tableName,
					Routine = routine,
					Arguments = args,
					Owner = owner
				};
			}

			#region TriggerTable

			class TriggerTable : GeneratedTable {
				private TableInfo tableInfo;

				public TriggerTable(ITransaction transaction, TableInfo tableInfo) 
					: base(transaction.Database.Context) {
					this.tableInfo = tableInfo;
				}

				public override TableInfo TableInfo {
					get { return tableInfo; }
				}

				public Field Type { get; set; }

				public Field TableName { get; set; }

				public Field Routine { get; set; }

				public Field Arguments { get; set; }

				public Field Owner { get; set; }

				public override int RowCount {
					get { return 1; }
				}

				public override Field GetValue(long rowNumber, int columnOffset) {
					if (rowNumber > 0)
						throw new ArgumentOutOfRangeException("rowNumber");

					switch (columnOffset) {
						case 0:
							return Type;
						case 1:
							return TableName;
						case 2:
							return Routine;
						case 3:
							return Arguments;
						case 4:
							return Owner;
						default:
							throw new ArgumentOutOfRangeException("columnOffset");
					}
				}
			}

			#endregion
		}

		#endregion
	}
}
