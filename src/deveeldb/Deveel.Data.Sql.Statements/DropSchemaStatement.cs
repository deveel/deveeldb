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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropSchemaStatement : SqlStatement {
		public DropSchemaStatement(string schemaName) {
			if (String.IsNullOrEmpty(schemaName))
				throw new ArgumentNullException("schemaName");
			if (String.Equals(InformationSchema.SchemaName, schemaName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(SystemSchema.Name, schemaName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("The schema name '{0}' is reserved and cannot be dropped.", schemaName));

			SchemaName = schemaName;
		}

		private DropSchemaStatement(SerializationInfo info, StreamingContext context) {
			SchemaName = info.GetString("SchemaName");
		}

		public string SchemaName { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("SchemaName", SchemaName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Access.SchemaExists(SchemaName))
				throw new InvalidOperationException(String.Format("The schema '{0}' does not exist.", SchemaName));

			if (!context.Request.Access.UserCanDropSchema(SchemaName))
				throw new MissingPrivilegesException(context.User.Name, new ObjectName(SchemaName), Privileges.Drop);

			// TODO: Check if the schema is empty before deleting it

			context.Request.Access.DropSchema(SchemaName);

			// TODO: Remove all the grants on this schema...
		}
	}
}
