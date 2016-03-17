using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
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
			return Session.Access.UserHasPrivilege(Name, objectType, objectName, privileges);
		}

		public virtual bool HasGrantOption(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			AssertInContext();
			return Session.Access.HasGrantOption(Name, objectType, objectName, privileges);
		}

		public bool CanSelectFrom(DbObjectType objectType, ObjectName objectName) {
			return HasPrivileges(objectType, objectName, Privileges.Select);
		}

		public bool CanSelectFromTable(ObjectName tableName) {
			return CanSelectFrom(DbObjectType.Table, tableName);
		}

		public bool CanInsertIntoTable(ObjectName tableName) {
			return HasPrivileges(DbObjectType.Table, tableName, Privileges.Insert);
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

		public bool CanAlter(DbObjectType objectType, ObjectName objectName) {
			return HasPrivileges(objectType, objectName, Privileges.Alter) ||
			       HasSchemaPrivileges(objectName.ParentName, Privileges.Alter);
		}
	}
}
