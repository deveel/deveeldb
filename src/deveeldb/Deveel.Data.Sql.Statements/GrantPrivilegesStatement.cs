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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
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
			if (String.IsNullOrEmpty(grantee))
				throw new ArgumentNullException("grantee");
			if (objName == null)
				throw new ArgumentNullException("objName");

			Grantee = grantee;
			Privilege = privilege;
			Columns = columns;
			ObjectName = objName;
			WithGrant = withGrant;
		}

		private GrantPrivilegesStatement(SerializationInfo info, StreamingContext context) {
			ObjectName = (ObjectName) info.GetValue("ObjectName", typeof(ObjectName));
			Grantee = info.GetString("Grantee");
			Privilege = (Privileges) info.GetInt32("Privilege");
			Columns = (string[]) info.GetValue("Columns", typeof(string[]));
			WithGrant = info.GetBoolean("WithGrant");
		}

		public IEnumerable<string> Columns { get; private set; }

		public string Grantee { get; private set; }

		public Privileges Privilege { get; private set; }

		public ObjectName ObjectName { get; private set; }

		public bool WithGrant { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("ObjectName", ObjectName);
			info.AddValue("Grantee", Grantee);
			info.AddValue("Privilege", (int)Privilege);
			info.AddValue("Columns", Columns);
			info.AddValue("WithGrant", WithGrant);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var objName = context.Access().ResolveObjectName(ObjectName.FullName);
			return new GrantPrivilegesStatement(Grantee, Privilege, WithGrant, objName, Columns);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var obj = context.Request.Access().FindObject(ObjectName);
			if (obj == null)
				throw new InvalidOperationException(String.Format("Object '{0}' was not found in the system.", ObjectName));

			if (!context.User.HasGrantOption(DbObjectType.Table, ObjectName, Privilege))
				throw new SecurityException(String.Format("User '{0}' has not the option to grant '{1}' to '{2}' on {3}",
					context.User.Name, Privilege, Grantee, ObjectName));

			// TODO: Veirfy the current user has grant option
			context.Request.Access().GrantTo(Grantee, obj.ObjectInfo.ObjectType, obj.ObjectInfo.FullName, Privilege, WithGrant);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			// TODO: Make it SQL string
			var privs = Privilege.ToString().ToUpperInvariant();
			builder.AppendFormat("GRANT {0} TO {1} ON {2}", privs, Grantee, ObjectName);

			if (Columns != null) {
				var columns = Columns.ToArray();
				if (columns.Length > 0) {
					builder.AppendFormat("({0})", String.Join(", ", columns));
				}
			}

			if (WithGrant)
				builder.Append(" WITH GRANT OPTION");
		}
	}
}
