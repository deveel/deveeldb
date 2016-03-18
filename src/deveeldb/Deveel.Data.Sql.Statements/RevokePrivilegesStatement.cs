// 
//  Copyright 2010-2015 Deveel
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
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class RevokePrivilegesStatement : SqlStatement {
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

		protected override SqlStatement PrepareStatement(IRequest context) {
			var objectName = context.Access.ResolveTableName(ObjectName);

			if (objectName == null)
				throw new ObjectNotFoundException(ObjectName);

			var columns = (Columns != null ? Columns.ToArray() : null);
			return new RevokePrivilegesStatement(Grantee, Privileges, GrantOption, objectName, columns);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var obj = context.DirectAccess.FindObject(ObjectName);
			if (obj == null)
				throw new ObjectNotFoundException(ObjectName);

			if (!context.User.HasGrantOption(obj.ObjectType, obj.FullName, Privileges))
				throw new SecurityException(String.Format("User '{0}' cannot revoke '{1}' privilege from '{2}' on '{3}'.",
					context.User.Name, Privileges, Grantee, ObjectName));

			context.DirectAccess.Revoke(obj.ObjectType, obj.FullName, Grantee, Privileges, GrantOption);
		}
	}
}
