// 
//  Copyright 2010-2016 Deveel
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
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerManager : IObjectManager, ITriggerManager {
		private ITransaction transaction;
		private bool tableModified;
		private bool cacheValid;
		private List<Trigger> triggerCache;

		private const int PlSqlType = 1;
		private const int ProcedureType = 2;

		public TriggerManager(ITransaction transaction) {
			this.transaction = transaction;
			triggerCache = new List<Trigger>(24);
			this.transaction.RegisterOnCommit(OnCommit);
		}

		~TriggerManager() {
			Dispose(false);
		}

		public static readonly ObjectName TriggerTableName = new ObjectName(SystemSchema.SchemaName, "trigger");

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
				var table = transaction.GetTable(TriggerTableName);

				var list =  new List<Trigger>();
				foreach (var row in table) {
					var triggerInfo = FormTrigger(row);
					if (triggerInfo is PlSqlTriggerInfo) {
						list.Add(new PlSqlTrigger((PlSqlTriggerInfo)triggerInfo));
					} else if (triggerInfo is ProcedureTriggerInfo) {
						list.Add(new ProcedureTrigger((ProcedureTriggerInfo)triggerInfo));
					}
				}

				triggerCache = new List<Trigger>(list);
				cacheValid = true;
			}
		}

		private void InvalidateTriggerCache() {
			cacheValid = false;
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

			var table = transaction.GetTable(TriggerTableName);
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
			var table = transaction.GetTable(TriggerTableName);

			var schemaName = objName.ParentName;
			var name = objName.Name;
			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

			foreach (var row in table) {
				var schemaValue = row.GetValue(0).Value.ToString();
				var nameValue = row.GetValue(1).Value.ToString();

				if (!String.Equals(name, nameValue, comparison) ||
					!String.Equals(schemaName, schemaValue, comparison))
					continue;

				return new ObjectName(ObjectName.Parse(schemaValue), nameValue);
			}

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

		private static SqlExpression[] DeserializeArguments(byte[] bytes) {
			using (var stream = new MemoryStream(bytes)) {
				var serializer = new BinarySerializer();
				var args = (TriggerArgument) serializer.Deserialize(stream);
				return args.Arguments;
			}
		}

		void ITriggerManager.CreateTrigger(TriggerInfo triggerInfo) {
			CreateTrigger(triggerInfo);
		}

		public void CreateTrigger(TriggerInfo triggerInfo) {
			if (!transaction.TableExists(TriggerTableName))
				return;

			var schema = triggerInfo.TriggerName.ParentName;
			var name = triggerInfo.TriggerName.Name;
			var onTable = triggerInfo.TableName.FullName;

			var action = (int) triggerInfo.EventTypes;

			int type;
			if (triggerInfo is ProcedureTriggerInfo) {
				type = ProcedureType;
			} else if (triggerInfo is PlSqlTriggerInfo) {
				type = PlSqlType;
			} else {
				throw new ArgumentException("The specified trigger info is invalid.");
			}

			// Insert the entry into the trigger table,
			var table = transaction.GetMutableTable(TriggerTableName);
			var row = table.NewRow();
			row.SetValue(0, Field.String(schema));
			row.SetValue(1, Field.String(name));
			row.SetValue(2, Field.Integer(type));
			row.SetValue(3, Field.String(onTable));
			row.SetValue(4, Field.Integer(action));

			if (type == ProcedureType) {
				var procInfo = (ProcedureTriggerInfo) triggerInfo;

				var args = new TriggerArgument(procInfo.Arguments);
				var binArgs = SerializeArguments(args);

				var procedureName = procInfo.ProcedureName.FullName;
				row.SetValue(5, Field.String(procedureName));
				row.SetValue(6, Field.Binary(binArgs));
			} else if (type == PlSqlType) {
				var plsqlInfo = (PlSqlTriggerInfo) triggerInfo;
				var body = Field.Binary(SqlBinary.ToBinary(plsqlInfo.Body));
				row.SetValue(7, body);
			}

			table.AddRow(row);

			InvalidateTriggerCache();

			transaction.Registry.RegisterEvent(new ObjectCreatedEvent(triggerInfo.TriggerName, DbObjectType.Trigger));

			tableModified = true;
		}

		public bool DropTrigger(ObjectName triggerName) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");

			var table = transaction.GetMutableTable(TriggerTableName);

			var schemaName = triggerName.ParentName;
			var name = triggerName.Name;

			var schemaCol = table.GetResolvedColumnName(0);
			var nameCol = table.GetResolvedColumnName(1);

			using (var session = new SystemSession(transaction)) {
				using (var query = session.CreateQuery()) {
					var t = table.SimpleSelect(query, nameCol, SqlExpressionType.Equal, SqlExpression.Constant(name));
					t = t.ExhaustiveSelect(query,
						SqlExpression.Equal(SqlExpression.Reference(schemaCol), SqlExpression.Constant(schemaName)));

					if (t.RowCount == 0)
						return false;

					table.Delete(t);

					transaction.Registry.RegisterEvent(new ObjectDroppedEvent(DbObjectType.Trigger, triggerName));
					return true;
				}
			}
		}

		public bool TriggerExists(ObjectName triggerName) {
			var table = transaction.GetTable(TriggerTableName);
			var result = FindTrigger(table, triggerName.ParentName, triggerName.Name);
			if (result.RowCount == 0)
				return false;

			if (result.RowCount > 1)
				throw new InvalidOperationException(String.Format("More than one trigger found with name '{0}'.", triggerName));

			return true;
		}

		public Trigger GetTrigger(ObjectName triggerName) {
			var table = transaction.GetTable(TriggerTableName);
			var result = FindTrigger(table, triggerName.ParentName, triggerName.Name);
			if (result.RowCount == 0)
				return null;

			if (result.RowCount > 1)
				throw new InvalidOperationException(String.Format("More than one trigger found with name '{0}'.", triggerName));

			var triggerInfo = FormTrigger(result.First());
			if (triggerInfo is PlSqlTriggerInfo)
				return new PlSqlTrigger((PlSqlTriggerInfo)triggerInfo);
			if (triggerInfo is ProcedureTriggerInfo)
				return new ProcedureTrigger((ProcedureTriggerInfo)triggerInfo);

			throw new InvalidOperationException();
		}

		private TriggerInfo FormTrigger(Row row) {
			var schema = row.GetValue(0).Value.ToString();
			var name = row.GetValue(1).Value.ToString();
			var triggerName = new ObjectName(new ObjectName(schema), name);

			var triggerType = ((SqlNumber)row.GetValue(2).Value).ToInt32();

			var tableName = ObjectName.Parse(((SqlString) row.GetValue(3).Value).ToString());
			var eventType = (TriggerEventType) ((SqlNumber) row.GetValue(4).Value).ToInt32();

			TriggerInfo triggerInfo;

			if (triggerType == ProcedureType) {
				var procNameString = row.GetValue(5).Value.ToString();
				var procName = ObjectName.Parse(procNameString);
				var argsBinary = (SqlBinary)row.GetValue(6).Value;
				var args = DeserializeArguments(argsBinary.ToByteArray());

				triggerInfo = new ProcedureTriggerInfo(procName, tableName, eventType, procName);

				if (args != null && args.Length > 0) {
					foreach (var expression in args) {
						((ProcedureTriggerInfo) triggerInfo).Arguments = args;
					}
				}
			} else if (triggerType == PlSqlType) {
				var binary = (SqlBinary) row.GetValue(7).Value;
				var body = binary.ToObject<PlSqlBlockStatement>();

				triggerInfo = new PlSqlTriggerInfo(triggerName, tableName, eventType, body);
			} else {
				throw new InvalidOperationException();
			}

			return triggerInfo;
		}

		public bool AlterTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public void FireTriggers(IRequest context, TableEvent tableEvent) {
			if (!transaction.TableExists(TriggerTableName))
				return;

			BuildTriggerCache();

			foreach (var trigger in triggerCache) {
				try {
					if (trigger.CanFire(tableEvent))
						trigger.Fire(tableEvent, context);
				} catch(TriggerException) {
					throw;
				} catch (Exception ex) {
					throw new TriggerException(trigger, ex);
				}
			}
		}
	}
}
