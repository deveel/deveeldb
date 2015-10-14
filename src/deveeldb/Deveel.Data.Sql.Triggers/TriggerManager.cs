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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerManager : IObjectManager {
		private ITransaction transaction;

		public TriggerManager(ITransaction transaction) {
			this.transaction = transaction;
		}

		public void Dispose() {
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Trigger; }
		}

		public void Create() {
			var tableInfo = new TableInfo(SystemSchema.TriggerTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Integer());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("action", PrimitiveTypes.Integer());
			tableInfo.AddColumn("misc", PrimitiveTypes.Binary());
			tableInfo.AddColumn("username", PrimitiveTypes.String());
			transaction.CreateTable(tableInfo);
		}

		private ITable FindTrigger(ITable table, string schema, string name) {
			// Find all the trigger entries with this name
			var schemaColumn = table.GetResolvedColumnName(0);
			var nameColumn = table.GetResolvedColumnName(1);

			using (var context = new SystemQueryContext(transaction, SystemSchema.Name)) {
				var t = table.SimpleSelect(context, nameColumn, SqlExpressionType.Equal,
					SqlExpression.Constant(DataObject.String(name)));
				return t.ExhaustiveSelect(context,
					SqlExpression.Equal(SqlExpression.Reference(schemaColumn), SqlExpression.Constant(DataObject.String(schema))));
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
			using (var context = new SystemQueryContext(transaction, SystemSchema.Name)) {
				var t = table.SimpleSelect(context, tableColumn, SqlExpressionType.Equal,
					SqlExpression.Constant(DataObject.String(fullTableName)));

				result = t.ExhaustiveSelect(context,
					SqlExpression.Equal(SqlExpression.Reference(eventTypeColumn), SqlExpression.Constant(eventTypeCode)));
			}

			if (result.RowCount == 0)
				return new TriggerInfo[0];

			var list = new List<TriggerInfo>();

			foreach (var row in result) {
				var schema = row.GetValue(0).Value.ToString();
				var name = row.GetValue(1).Value.ToString();
				var triggerName = new ObjectName(new ObjectName(schema), name);

				var triggerType = (TriggerType) ((SqlNumber) row.GetValue(2).Value).ToInt32();
				var triggerInfo = new TriggerInfo(triggerName, triggerType, eventType, tableName);

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
			throw new NotImplementedException();
		}

		public void CreateTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public bool DropTrigger(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public bool TriggerExists(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public Trigger GetTrigger(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public bool AlterTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public IEnumerable<Trigger> FindTriggers(TriggerEventInfo eventInfo) {
			var triggers = FindTriggers(eventInfo.TableName, eventInfo.EventType);
			return triggers.Select(x => new Trigger(x));
		}
	}
}
