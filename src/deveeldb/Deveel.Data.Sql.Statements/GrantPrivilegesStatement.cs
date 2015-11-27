using System;
using System.Collections.Generic;

using Deveel.Data.Security;
using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class GrantPrivilegesStatement : SqlPreparedStatement, IPreparableStatement {
		public GrantPrivilegesStatement(string grantee, Privileges privilege, ObjectName objName) 
			: this(grantee, privilege, false, objName) {
		}

		public GrantPrivilegesStatement(string grantee, Privileges privilege, bool withGrant, ObjectName objName) 
			: this(grantee, privilege, withGrant, objName, null) {
		}

		public GrantPrivilegesStatement(string grantee, Privileges privilege, ObjectName objName, IEnumerable<string> columns) 
			: this(grantee, privilege, false, objName, columns) {
		}

		public GrantPrivilegesStatement(string grantee, Privileges privilege, bool withGrant, ObjectName objName, IEnumerable<string> columns) {
			Grantee = grantee;
			Privilege = privilege;
			Columns = columns;
			ObjectName = objName;
			WithGrant = withGrant;
		}

		private GrantPrivilegesStatement(ObjectData data) {
			ObjectName = data.GetValue<ObjectName>("ObjectName");
			Grantee = data.GetString("Grantee");
			Privilege = (Privileges) data.GetInt32("Privilege");
			Columns = data.GetValue<string[]>("Columns");
			WithGrant = data.GetBoolean("WithGrant");
		}

		public IEnumerable<string> Columns { get; private set; }

		public string Grantee { get; private set; }

		public Privileges Privilege { get; private set; }

		public ObjectName ObjectName { get; private set; }

		public bool WithGrant { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("ObjectName", ObjectName);
			data.SetValue("Grantee", Grantee);
			data.SetValue("Privilege", (int)Privilege);
			data.SetValue("Columns", Columns);
			data.SetValue("WithGrant", WithGrant);
		}

		IPreparedStatement IPreparableStatement.Prepare(IRequest context) {
			var objName = context.Query.ResolveObjectName(ObjectName.FullName);
			return new GrantPrivilegesStatement(Grantee, Privilege, WithGrant, objName, Columns);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var obj = context.Request.Query.FindObject(ObjectName);
			if (obj == null)
				throw new InvalidOperationException(String.Format("Object '{0}' was not found in the system.", ObjectName));

			context.Request.Query.GrantTo(Grantee, obj.ObjectType, obj.FullName, Privilege, WithGrant);
		}
	}
}
