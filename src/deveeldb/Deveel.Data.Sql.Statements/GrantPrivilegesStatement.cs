using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class GrantPrivilegesStatement : SqlStatement {
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

		public IEnumerable<string> Columns { get; private set; }

		public string Grantee { get; private set; }

		public Privileges Privilege { get; private set; }

		public ObjectName ObjectName { get; private set; }

		public bool WithGrant { get; private set; }

		protected override SqlStatement PrepareStatement(IQueryContext context) {
			var objName = context.ResolveObjectName(ObjectName.FullName);
			return new GrantPrivilegesStatement(Grantee, Privilege, WithGrant, objName, Columns);
		}

		protected override ITable ExecuteStatement(IQueryContext context) {
			var obj = context.FindObject(ObjectName);
			if (obj == null)
				throw new InvalidOperationException(String.Format("Object '{0}' was not found in the system.", ObjectName));

			context.GrantTo(Grantee, obj.ObjectType, obj.FullName, Privilege, WithGrant);
			return FunctionTable.ResultTable(context, 0);
		}
	}
}
