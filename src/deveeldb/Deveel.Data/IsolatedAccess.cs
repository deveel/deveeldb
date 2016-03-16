using System;

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
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
			get { return Session.Access; }
		}

		public bool ObjectExists(ObjectName objectName) {
			if (Request.Context.VariableExists(objectName.Name))
				return true;
			if (Request.Context.VariableExists(objectName.Name))
				return true;

			return Session.Access.ObjectExists(objectName);
		}

		public bool ObjectExists(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor &&
				Request.Context.CursorExists(objectName.Name))
				return true;
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return true;

			return Session.Access.ObjectExists(objectType, objectName);
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
			if (!Session.Access.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return Session.Access.GetObject(objType, objName, accessType);
		}

		public void CreateObject(IObjectInfo objectInfo) {
			if (objectInfo.ObjectType == DbObjectType.Cursor) {
				var cursorInfo = (CursorInfo) objectInfo;
				Request.Context.DeclareCursor(cursorInfo);
			} else if (objectInfo.ObjectType == DbObjectType.Variable) {
				var varInfo = (VariableInfo) objectInfo;
				Request.Context.DeclareVariable(varInfo);
			} else {
				Session.Access.CreateObject(objectInfo);
			}
		}

		public bool DropObject(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor)
				return Request.Context.DropCursor(objectName.Name);
			if (objectType == DbObjectType.Variable)
				return Request.Context.DropVariable(objectName.Name);

			Session.Access.DropObject(objectType, objectName);
			return true;
		}

		public void AlterObject(IObjectInfo objectInfo) {
			if (objectInfo == null)
				throw new ArgumentNullException("objectInfo");

			if (objectInfo.ObjectType == DbObjectType.Cursor ||
				objectInfo.ObjectType == DbObjectType.Variable) {
				throw new NotSupportedException();
			}

			if (objectInfo.ObjectType == DbObjectType.Table) {
				AlterTable((TableInfo) objectInfo);
				return;
			}

			Session.Access.AlterObject(objectInfo);
		}

		public void AlterTable(TableInfo tableInfo) {
			try {
				Session.Access.AlterTable(tableInfo);
			} finally {
				if (TableCache != null)
					TableCache.Remove(tableInfo.TableName.FullName);
			}
		}

		public ObjectName ResolveObjectName(string name) {
			if (Request.Context.CursorExists(name))
				return new ObjectName(name);
			if (Request.Context.VariableExists(name))
				return new ObjectName(name);

			return Session.Access.ResolveObjectName(name);
		}

		public ObjectName ResolveObjectName(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Cursor &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);

			return Session.Access.ResolveObjectName(objectType, objectName);
		}

		public IDbObject FindObject(ObjectName objectName) {
			if (Request.Context.CursorExists(objectName.Name))
				return Request.Context.FindCursor(objectName.Name);
			if (Request.Context.VariableExists(objectName.Name))
				return Request.Context.FindVariable(objectName.Name);

			return Session.Access.FindObject(objectName);
		}

		private ICache TableCache {
			get { return Request.Context.ResolveService<ICache>("TableCache"); }
		}

		public ITable GetTable(ObjectName tableName) {
			var table = GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = Session.Access.GetTable(tableName);
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

		#region Cursors

		public void DeclareCursor(CursorInfo cursorInfo) {
			var queryPlan = Request.Context.QueryPlanner().PlanQuery(new QueryInfo(Request, cursorInfo.QueryExpression));
			var selectedTables = queryPlan.DiscoverTableNames();
			foreach (var tableName in selectedTables) {
				if (!SystemAccess.UserCanSelectFromTable(tableName))
					throw new MissingPrivilegesException(Request.Query.UserName(), tableName, Privileges.Select);
			}

			Request.Context.DeclareCursor(cursorInfo);
		}

		public void DeclareCursor(string cursorName, SqlQueryExpression query) {
			DeclareCursor(cursorName, (CursorFlags)0, query);
		}

		public void DeclareCursor(string cursorName, CursorFlags flags, SqlQueryExpression query) {
			DeclareCursor(new CursorInfo(cursorName, flags, query));
		}

		public void DeclareInsensitiveCursor(string cursorName, SqlQueryExpression query) {
			DeclareInsensitiveCursor(cursorName, query, false);
		}

		public void DeclareInsensitiveCursor(string cursorName, SqlQueryExpression query, bool withScroll) {
			var flags = CursorFlags.Insensitive;
			if (withScroll)
				flags |= CursorFlags.Scroll;

			DeclareCursor(cursorName, flags, query);
		}

		public bool CursorExists(string cursorName) {
			return Request.Context.CursorExists(cursorName);
		}

		public bool DropCursor(string cursorName) {
			return Request.Context.DropCursor(cursorName);
		}

		public Cursor FindCursor(string cursorName) {
			return Request.Context.FindCursor(cursorName);
		}

		public bool OpenCursor(string cursorName, params SqlExpression[] args) {
			return Request.Context.OpenCursor(Request, cursorName, args);
		}

		public bool CloseCursor(string cursorName) {
			return Request.Context.CloseCursor(Request, cursorName);
		}


		#endregion

		public bool VariableExists(string variableName) {
			return ObjectExists(DbObjectType.Variable, new ObjectName(variableName));
		}

		public bool IsAggregateFunction(Invoke invoke) {
			return Session.Access.IsAggregateFunction(invoke, Request);
		}

		public IRoutine ResolveRoutine(Invoke invoke) {
			return Session.Access.ResolveRoutine(invoke, Request);
		}

		public Field InvokeSystemFunction(string functionName, params SqlExpression[] args) {
			var resolvedName = new ObjectName(SystemSchema.SchemaName, functionName);
			var invoke = new Invoke(resolvedName, args);
			return Session.Access.InvokeFunction(Request, invoke);
		}

		public Field InvokeFunction(Invoke invoke) {
			return Session.Access.InvokeFunction(Request, invoke);
		}

		public Field InvokeFunction(ObjectName functionName, params SqlExpression[] args) {
			return SystemAccess.InvokeFunction(Request, new Invoke(functionName, args));
		}


		public void FireTriggers(TableEvent tableEvent) {
			Session.Access.FireTriggers(Request, tableEvent);
		}


		public void Dispose() {
			Request = null;
		}
	}
}
