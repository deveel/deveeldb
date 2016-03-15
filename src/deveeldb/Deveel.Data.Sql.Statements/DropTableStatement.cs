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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropTableStatement : SqlStatement {
		public DropTableStatement(ObjectName tableName) 
			: this(tableName, false) {
		}

		public DropTableStatement(ObjectName tableName, bool ifExists) {
			if (tableName == null)
				throw new ArgumentNullException("tableNames");

			TableName = tableName;
			IfExists = ifExists;
		}

		public ObjectName TableName { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Query.Session.Access.ResolveTableName(TableName);
			if (!context.Query.Session.Access.TableExists(tableName) &&
			    !IfExists)
				throw new ObjectNotFoundException(TableName);

			return new Prepared(tableName, IfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, bool ifExists) {
				TableName = tableName;
				IfExists = ifExists;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				TableName = (ObjectName) info.GetValue("TableName", typeof (ObjectName));
				IfExists = info.GetBoolean("IfExists");
			}

			public ObjectName TableName { get; private set; }

			public bool IfExists { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("TableName", TableName);
				info.AddValue("IfExists", IfExists);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				if (!context.Query.Session.Access.UserCanDropObject(DbObjectType.Table, TableName))
					throw new MissingPrivilegesException(context.Query.UserName(), TableName, Privileges.Drop);

				// Check there are no referential links to any tables being dropped
				var refs = context.Query.Session.Access.QueryTableImportedForeignKeys(TableName);
				if (refs.Length > 0) {
					var reference = refs[0];
					throw new ConstraintViolationException(SqlModelErrorCodes.DropTableViolation,
						String.Format("Constraint violation ({0}) dropping table '{1}' because of referential link from '{2}'",
							reference.ConstraintName, TableName, reference.TableName));
				}

				// If the 'only if exists' flag is false, we need to check tables to drop
				// exist first.
				if (!IfExists) {
					// If table doesn't exist, throw an error
					if (!context.Query.Session.Access.TableExists(TableName)) {
						throw new InvalidOperationException(String.Format("The table '{0}' does not exist and cannot be dropped.",
							TableName));
					}
				}

				// Does the table already exist?
				if (context.Query.Session.Access.TableExists(TableName)) {
					// Drop table in the transaction
					context.Query.Access.DropObject(DbObjectType.Table, TableName);

					// Revoke all the grants on the table
					context.Query.Session.Access.RevokeAllGrantsOnTable(TableName);

					// Drop all constraints from the schema
					context.Query.Session.Access.DropAllTableConstraints(TableName);
				}
			}
		}

		#endregion
	}
}
