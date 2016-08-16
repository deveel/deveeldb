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

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class RequestAccess : SystemAccess {
		public RequestAccess(IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			Request = request;
		}

		private IRequest Request { get; set; }

		protected override ISession Session {
			get { return Request.Query.Session; }
		}

		public override bool ObjectExists(ObjectName objectName) {
			if (Request.Context.VariableExists(objectName.Name))
				return true;
			if (Request.Context.VariableExists(objectName.Name))
				return true;
			if (Request.Context.TriggerExists(objectName.Name))
				return true;

			return base.ObjectExists(objectName);
		}

		public override bool ObjectExists(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor &&
				Request.Context.CursorExists(objectName.Name))
				return true;
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return true;
			if (objectType == DbObjectType.Trigger &&
			    Request.Context.TriggerExists(objectName.Name))
				return true;

			return base.ObjectExists(objectType, objectName);
		}

		public override IDbObject GetObject(DbObjectType objType, ObjectName objName) {
			return GetObject(objType, objName, AccessType.ReadWrite);
		}

		public override IDbObject GetObject(DbObjectType objType, ObjectName objName, AccessType accessType) {
			if (objType == DbObjectType.Cursor)
				return Request.Context.FindCursor(objName.Name);
			if (objType == DbObjectType.Variable)
				return Request.Context.FindVariable(objName.Name);

			if (objType == DbObjectType.Table)
				return GetTable(objName);

			if (objType == DbObjectType.Trigger &&
			    Request.Context.TriggerExists(objName.Name))
				return Request.Context.FindTrigger(objName.Name);

			return base.GetObject(objType, objName, accessType);
		}

		public override void CreateObject(IObjectInfo objectInfo) {
			if (objectInfo.ObjectType == DbObjectType.Cursor) {
				var cursorInfo = (CursorInfo) objectInfo;
				Request.Context.DeclareCursor(cursorInfo, Request);
			} else if (objectInfo.ObjectType == DbObjectType.Variable) {
				var varInfo = (VariableInfo) objectInfo;
				Request.Context.DeclareVariable(varInfo);
			} else if (objectInfo is CallbackTriggerInfo) {
				var triggerInfo = (CallbackTriggerInfo) objectInfo;
				Request.Context.DeclareTrigger(triggerInfo);
			} else {
				base.CreateObject(objectInfo);
			}
		}

		public override bool DropObject(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor)
				return Request.Context.DropCursor(objectName.Name);
			if (objectType == DbObjectType.Variable)
				return Request.Context.DropVariable(objectName.Name);
			if (objectType == DbObjectType.Trigger &&
			    Request.Context.DropTrigger(objectName.Name))
				return true;

			return base.DropObject(objectType, objectName);
		}

		public override bool AlterObject(IObjectInfo objectInfo) {
			if (objectInfo == null)
				throw new ArgumentNullException("objectInfo");

			if (objectInfo.ObjectType == DbObjectType.Cursor ||
				objectInfo.ObjectType == DbObjectType.Variable) {
				throw new NotSupportedException();
			}

			if (objectInfo.ObjectType == DbObjectType.Table) {
				return AlterTable((TableInfo) objectInfo);
			}

			if (objectInfo.ObjectType == DbObjectType.Trigger &&
			    Request.Context.TriggerExists(objectInfo.FullName.Name)) {
				// TODO:
			}

			return base.AlterObject(objectInfo);
		}

		private bool AlterTable(TableInfo tableInfo) {
			try {
				return base.AlterObject(tableInfo);
			} finally {
				if (TableCache != null)
					TableCache.Remove(tableInfo.TableName.FullName);
			}
		}

		public override ObjectName ResolveObjectName(string name) {
			if (Request.Context.CursorExists(name))
				return new ObjectName(name);
			if (Request.Context.VariableExists(name))
				return new ObjectName(name);
			if (Request.Context.TriggerExists(name))
				return new ObjectName(name);

			return base.ResolveObjectName(name);
		}

		public override ObjectName ResolveObjectName(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Cursor &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Trigger &&
				Request.Context.TriggerExists(objectName.Name))
				return new ObjectName(objectName.Name);

			return base.ResolveObjectName(objectType, objectName);
		}

		public override IDbObject FindObject(ObjectName objectName) {
			if (Request.Context.CursorExists(objectName.Name))
				return Request.Context.FindCursor(objectName.Name);
			if (Request.Context.VariableExists(objectName.Name))
				return Request.Context.FindVariable(objectName.Name);

			return base.FindObject(objectName);
		}

		private ICache TableCache {
			get { return Request.Context.ResolveService<ICache>("TableCache"); }
		}

		public override ITable GetTable(ObjectName tableName) {
			var table = GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = Session.Access().GetTable(tableName);
				if (table != null) {
					table = new UserContextTable(Request, table);
					CacheTable(tableName.FullName, table);
				}
			}

			return table;
		}

		public override void CacheTable(string cacheKey, ITable table) {
			var tableCache = TableCache;
			if (tableCache == null)
				return;

			tableCache.Set(cacheKey, table);
		}

		public override void ClearCachedTables() {
			var tableCache = TableCache;
			if (tableCache == null)
				return;

			tableCache.Clear();
		}

		public override ITable GetCachedTable(string cacheKey) {
			var tableCache = TableCache;
			if (tableCache == null)
				return null;

			object obj;
			if (!tableCache.TryGet(cacheKey, out obj))
				return null;

			return obj as ITable;
		}
	}
}
