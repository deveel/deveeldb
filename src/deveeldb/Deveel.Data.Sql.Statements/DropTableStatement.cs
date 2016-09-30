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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTableStatement : SqlStatement {
		public DropTableStatement(ObjectName tableName) 
			: this(tableName, false) {
		}

		public DropTableStatement(ObjectName tableName, bool ifExists) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			IfExists = ifExists;
		}

		private DropTableStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TableName = (ObjectName) info.GetValue("TableName", typeof (ObjectName));
			IfExists = info.GetBoolean("IfExists");
		}

		public ObjectName TableName { get; private set; }

		public bool IfExists { get; set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TableName", TableName);
			info.AddValue("IfExists", IfExists);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access().ResolveTableName(TableName);
			if (!context.Access().TableExists(tableName) &&
			    !IfExists)
				throw new ObjectNotFoundException(TableName);

			return new DropTableStatement(tableName, IfExists);
		}

		protected override void OnBeforeExecute(ExecutionContext context) {
			RequestDrop(TableName, DbObjectType.Table);

			base.OnBeforeExecute(context);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanDrop(DbObjectType.Table, TableName))
				throw new MissingPrivilegesException(context.User.Name, TableName, Privileges.Drop);

			// Check there are no referential links to any tables being dropped
			var refs = context.Request.Access().QueryTableImportedForeignKeys(TableName);
			if (refs.Length > 0) {
				var reference = refs[0];
				throw new DropTableViolationException(TableName, reference.ConstraintName, reference.TableName);
			}

			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!IfExists) {
				// If table doesn't exist, throw an error
				if (!context.Request.Access().TableExists(TableName)) {
					throw new InvalidOperationException(String.Format("The table '{0}' does not exist and cannot be dropped.",
						TableName));
				}
			}

			// Does the table already exist?
			if (context.Request.Access().TableExists(TableName)) {
				// Drop table in the transaction
				context.Request.Access().DropObject(DbObjectType.Table, TableName);

				// Revoke all the grants on the table
				context.Request.Access().RevokeAllGrantsOnTable(TableName);

				// Drop all constraints from the schema
				context.Request.Access().DropAllTableConstraints(TableName);
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			string ifExists = IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP TABLE {0}{1}", ifExists, TableName);
		}
	}
}
