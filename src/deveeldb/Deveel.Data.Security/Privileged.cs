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
using System.Diagnostics;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Security {
	[DebuggerDisplay("{Name}")]
	public abstract class Privileged {
		internal Privileged(ISession session, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Session = session;
			Name = name;
		}

		/// <summary>
		/// Gets the name that uniquely identify a privileged entity within a database system.
		/// </summary>
		public string Name { get; private set; }

		internal ISession Session { get; private set; }

		internal void AssertInContext() {
			if (Session == null)
				throw new InvalidOperationException("The user is not in context");
		}

		public virtual bool HasPrivileges(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			AssertInContext();

			if (Session.Access.UserHasPrivilege(Name, objectType, objectName, privileges))
				return true;

			var obj = Session.Access.GetObject(objectType, objectName);
			if (obj != null &&
			    String.Equals(Name, obj.ObjectInfo.Owner))
				return true;

			if (objectName.Parent != null)
				return HasPrivileges(DbObjectType.Schema, objectName.Parent, privileges);

			return false;
		}

		public virtual bool HasGrantOption(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			AssertInContext();
			return Session.Access.HasGrantOption(Name, objectType, objectName, privileges);
		}

		public virtual bool CanManageUsers() {
			return false;
		}

		public virtual bool CanManageSchema() {
			return false;
		}

		public bool CanCreate(DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Schema)
				return CanManageSchema();

			return HasSchemaPrivileges(objectName.ParentName, Privileges.Create);
		}

		public bool CanCreateTable(ObjectName tableName) {
			return CanCreate(DbObjectType.Table, tableName);
		}

		public bool CanSelectFrom(DbObjectType objectType, ObjectName objectName) {
			return HasPrivileges(objectType, objectName, Privileges.Select);
		}

		public bool CanSelectFromTable(ObjectName tableName) {
			return CanSelectFrom(DbObjectType.Table, tableName);
		}

		public bool CanSelectFrom(IQueryPlanNode queryPlan) {
			var references = queryPlan.DiscoverTableNames();
			return references.All(CanSelectFromTable);
		}

		public bool CanAccessObject(DbObjectType objectType, ObjectName objectName) {
			var privileges = Privileges.Select;
			if (objectType == DbObjectType.Routine)
				privileges = Privileges.Execute;

			return HasPrivileges(objectType, objectName, privileges);
		}

		public bool CanInsertIntoTable(ObjectName tableName) {
			return HasPrivileges(DbObjectType.Table, tableName, Privileges.Insert);
		}

		public bool CanUpdateTable(ObjectName tableName) {
			return HasPrivileges(DbObjectType.Table, tableName, Privileges.Update);
		}

		public bool CanDeleteFromTable(ObjectName tableName) {
			return HasPrivileges(DbObjectType.Table, tableName, Privileges.Delete);
		}

		public bool HasSchemaPrivileges(string schemaName, Privileges privileges) {
			return HasPrivileges(DbObjectType.Schema, new ObjectName(schemaName), privileges);
		}

		public bool CanCreateInSchema(string schemaName) {
			return HasSchemaPrivileges(schemaName, Privileges.Create);
		}

		public bool CanDrop(DbObjectType objectType, ObjectName objectName) {
			return HasPrivileges(objectType, objectName, Privileges.Drop) ||
			       HasSchemaPrivileges(objectName.ParentName, Privileges.Drop);
		}

		public bool CanDropSchema(string schemaName) {
			return HasPrivileges(DbObjectType.Schema, new ObjectName(schemaName), Privileges.Drop) ||
			       CanManageSchema();
		}

		public bool CanAlter(DbObjectType objectType, ObjectName objectName) {
			return HasPrivileges(objectType, objectName, Privileges.Alter) ||
			       HasSchemaPrivileges(objectName.ParentName, Privileges.Alter);
		}

		public bool CanAlterTable(ObjectName tableName) {
			return CanAlter(DbObjectType.Table, tableName);
		}

		public bool CanExecute(RoutineType routineType, Invoke invoke, IRequest request) {
			AssertInContext();

			if (routineType == RoutineType.Function &&
				Session.Access.IsSystemFunction(invoke, request))
				return true;

			return HasPrivileges(DbObjectType.Routine, invoke.RoutineName, Privileges.Execute);
		}

		public bool CanExecuteFunction(Invoke invoke, IRequest request) {
			return CanExecute(RoutineType.Function, invoke, request);
		}

		public bool CanExecuteProcedure(Invoke invoke, IRequest request) {
			return CanExecute(RoutineType.Procedure, invoke, request);
		}

		public override string ToString() {
			return Name;
		}
	}
}
