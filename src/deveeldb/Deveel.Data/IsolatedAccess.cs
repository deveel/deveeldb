using System;

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class IsolatedAccess : IDisposable {
		public IsolatedAccess(IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			Request = request;
		}

		public IRequest Request { get; private set; }

		public ISession Session {
			get { return Request.Query.Session; }
		}

		public SystemAccess SystemAccess {
			get { return Session.SystemAccess; }
		}

		public bool ObjectExists(ObjectName objectName) {
			if (Request.Context.VariableExists(objectName.Name))
				return true;
			if (Request.Context.VariableExists(objectName.Name))
				return true;

			return Session.ObjectExists(objectName);
		}

		public bool ObjectExists(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor &&
				Request.Context.CursorExists(objectName.Name))
				return true;
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return true;

			return Session.ObjectExists(objectType, objectName);
		}

		public IDbObject GetObject(DbObjectType objType, ObjectName objName) {
			return GetObject(objType, objName, AccessType.ReadWrite);
		}

		public IDbObject GetObject(DbObjectType objType, ObjectName objName, AccessType accessType) {
			if (objType == DbObjectType.Cursor)
				return Request.Context.FindCursor(objName.Name);
			if (objType == DbObjectType.Variable)
				return Request.Context.FindVariable(objName.Name);

			if (objType == DbObjectType.Table)
				return GetTable(objName);

			// TODO: throw a specialized exception
			if (!Session.SystemAccess.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return Session.GetObject(objType, objName, accessType);
		}

		public void CreateObject(IObjectInfo objectInfo) {
			if (objectInfo.ObjectType == DbObjectType.Cursor) {
				var cursorInfo = (CursorInfo) objectInfo;
				Request.Context.DeclareCursor(cursorInfo);
			} else if (objectInfo.ObjectType == DbObjectType.Variable) {
				var varInfo = (VariableInfo) objectInfo;
				Request.Context.DeclareVariable(varInfo);
			} else {
				Session.SystemAccess.CreateObject(objectInfo);
			}
		}

		public bool DropObject(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor)
				return Request.Context.DropCursor(objectName.Name);
			if (objectType == DbObjectType.Variable)
				return Request.Context.DropVariable(objectName.Name);

			Session.SystemAccess.DropObject(objectType, objectName);
			return true;
		}

		public void AlterObject(IObjectInfo objectInfo) {
			if (objectInfo == null)
				throw new ArgumentNullException("objectInfo");

			if (objectInfo.ObjectType == DbObjectType.Cursor ||
				objectInfo.ObjectType == DbObjectType.Variable) {
				throw new NotSupportedException();
			}

			Session.SystemAccess.AlterObject(objectInfo);
		}

		public ObjectName ResolveObjectName(string name) {
			if (Request.Context.CursorExists(name))
				return new ObjectName(name);
			if (Request.Context.VariableExists(name))
				return new ObjectName(name);

			return Session.SystemAccess.ResolveObjectName(name);
		}

		public ObjectName ResolveObjectName(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Cursor &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);

			return Session.SystemAccess.ResolveObjectName(objectType, objectName);
		}

		public IDbObject FindObject(ObjectName objectName) {
			if (Request.Context.CursorExists(objectName.Name))
				return Request.Context.FindCursor(objectName.Name);
			if (Request.Context.VariableExists(objectName.Name))
				return Request.Context.FindVariable(objectName.Name);

			return Session.SystemAccess.FindObject(objectName);
		}

		private ICache TableCache {
			get { return Request.Context.ResolveService<ICache>("TableCache"); }
		}

		public ITable GetTable(ObjectName tableName) {
			var table = GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = Session.GetTable(tableName);
				if (table != null) {
					table = new UserContextTable(Request, table);
					CacheTable(tableName.FullName, table);
				}
			}

			return table;
		}

		public void CacheTable(string cacheKey, ITable table) {
			var tableCache = TableCache;
			if (tableCache == null)
				return;

			tableCache.Set(cacheKey, table);
		}

		public void ClearCachedTables() {
			var tableCache = TableCache;
			if (tableCache == null)
				return;

			tableCache.Clear();
		}

		public IMutableTable GetMutableTable(ObjectName tableName) {
			return GetTable(tableName) as IMutableTable;
		}

		public ITable GetCachedTable(string cacheKey) {
			var tableCache = TableCache;
			if (tableCache == null)
				return null;

			object obj;
			if (!tableCache.TryGet(cacheKey, out obj))
				return null;

			return obj as ITable;
		}

		public bool IsAggregateFunction(Invoke invoke) {
			return Session.SystemAccess.IsAggregateFunction(invoke, Request);
		}

		public IRoutine ResolveRoutine(Invoke invoke) {
			return Session.SystemAccess.ResolveRoutine(invoke, Request);
		}

		public void FireTriggers(TableEvent tableEvent) {
			Session.SystemAccess.FireTriggers(Request, tableEvent);
		}


		public void Dispose() {
			Request = null;
		}
	}
}
