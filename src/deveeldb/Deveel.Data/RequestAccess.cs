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

		private SessionAccess SessionAccess {
			get { return Session.Access; }
		}

		public override bool ObjectExists(ObjectName objectName) {
			if (Request.Context.VariableExists(objectName.Name))
				return true;
			if (Request.Context.VariableExists(objectName.Name))
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

			// TODO: throw a specialized exception
			if (!Session.Access.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return base.GetObject(objType, objName, accessType);
		}

		public override void CreateObject(IObjectInfo objectInfo) {
			if (objectInfo.ObjectType == DbObjectType.Cursor) {
				var cursorInfo = (CursorInfo) objectInfo;
				Request.Context.DeclareCursor(cursorInfo);
			} else if (objectInfo.ObjectType == DbObjectType.Variable) {
				var varInfo = (VariableInfo) objectInfo;
				Request.Context.DeclareVariable(varInfo);
			} else {
				base.CreateObject(objectInfo);
			}
		}

		public override bool DropObject(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor)
				return Request.Context.DropCursor(objectName.Name);
			if (objectType == DbObjectType.Variable)
				return Request.Context.DropVariable(objectName.Name);

			return base.DropObject(objectType, objectName);
		}

		public override void AlterObject(IObjectInfo objectInfo) {
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

			base.AlterObject(objectInfo);
		}

		public void AlterTable(TableInfo tableInfo) {
			try {
				base.AlterObject(tableInfo);
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

			return base.ResolveObjectName(name);
		}

		public override ObjectName ResolveObjectName(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Variable &&
				Request.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Cursor &&
				Request.Context.VariableExists(objectName.Name))
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
				if (!UserCanSelectFromTable(tableName))
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

		public void FireTriggers(TableEvent tableEvent) {
			Session.Access.FireTriggers(Request, tableEvent);
		}
	}
}
