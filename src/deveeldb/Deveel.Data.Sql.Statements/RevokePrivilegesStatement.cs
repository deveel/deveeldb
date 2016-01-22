using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class RevokePrivilegesStatement : SqlStatement, IPreparableStatement {
		public RevokePrivilegesStatement(string grantee, Privileges privileges, bool grantOption, ObjectName objectName, IEnumerable<string> columns) {
			if (String.IsNullOrEmpty(grantee))
				throw new ArgumentNullException("grantee");
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			Grantee = grantee;
			Privileges = privileges;
			ObjectName = objectName;
			Columns = columns;
			GrantOption = grantOption;
		}

		public string Grantee { get; private set; }

		public Privileges Privileges { get; private set; }
		
		public ObjectName ObjectName { get; private set; }

		public IEnumerable<string> Columns { get; private set; }

		public bool GrantOption { get; set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			var objectName = request.Query.ResolveTableName(ObjectName);

			if (objectName == null)
				throw new ObjectNotFoundException(ObjectName);

			var columns = (Columns != null ? Columns.ToArray() : null);
			return new Prepared(Grantee, Privileges, columns, objectName, GrantOption);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(string grantee, Privileges privileges, string[] columns, ObjectName objectName, bool grantOption) {
				Grantee = grantee;
				Privileges = privileges;
				Columns = columns;
				ObjectName = objectName;
				GrantOption = grantOption;
			}

			private Prepared(ObjectData data) {
				Grantee = data.GetString("Grantee");
				Privileges = (Privileges) data.GetInt32("Privileges");
				ObjectName = data.GetValue<ObjectName>("ObjectName");
				GrantOption = data.GetBoolean("GrantOption");
			}

			public string Grantee { get; private set; }

			public Privileges Privileges { get; private set; }

			public string[] Columns { get; private set; }

			public ObjectName ObjectName { get; private set; }

			public bool GrantOption { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("Grantee", Grantee);
				data.SetValue("Privileges", (int)Privileges);
				data.SetValue("ObjectName", ObjectName);
				data.SetValue("GrantOption", GrantOption);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
