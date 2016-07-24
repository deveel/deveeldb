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
using System.Runtime.Serialization;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTypeStatement : SqlStatement {
		public DropTypeStatement(ObjectName typeName) 
			: this(typeName, false) {
		}

		public DropTypeStatement(ObjectName typeName, bool ifExists) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
			IfExists = ifExists;
		}

		private DropTypeStatement(SerializationInfo info, StreamingContext context) {
			TypeName = (ObjectName) info.GetValue("TypeName", typeof(ObjectName));
			IfExists = info.GetBoolean("IfExists");
		}

		public ObjectName TypeName { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var typeName = context.Access().ResolveObjectName(DbObjectType.Type, TypeName);
			return new DropTypeStatement(typeName, IfExists);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.TypeExists(TypeName)) {
				if (!IfExists)
					throw new ObjectNotFoundException(TypeName);

				return;
			}

			if (!context.User.CanDrop(DbObjectType.Type, TypeName))
				throw new MissingPrivilegesException(context.User.Name, TypeName, Privileges.Drop);

			var children = context.DirectAccess.GetChildTypes(TypeName);
			foreach (var child in children) {
				DropType(context, child);
			}

			DropType(context, TypeName);
		}

		private void DropType(ExecutionContext context, ObjectName typeName) {
			if (!context.DirectAccess.DropObject(DbObjectType.Type, typeName))
				throw new StatementException(String.Format("Could not drop type '{0}' for an unknown reason.", typeName));

			context.DirectAccess.RevokeAllGrantsOn(DbObjectType.Type, typeName);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TypeName", TypeName);
			info.AddValue("IfExists", IfExists);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			string ifExists = IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP TYPE {0}{1}", ifExists, TypeName);
		}
	}
}