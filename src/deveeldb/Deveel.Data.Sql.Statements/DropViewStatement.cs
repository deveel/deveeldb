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
using Deveel.Data.Sql.Views;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropViewStatement : SqlStatement {
		public DropViewStatement(ObjectName viewName) 
			: this(viewName, false) {
		}

		public DropViewStatement(ObjectName viewName, bool ifExists) {
			if (viewName == null)
				throw new ArgumentNullException("viewName");

			ViewName = viewName;
			IfExists = ifExists;
		}

		public ObjectName ViewName { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var viewName = context.Query.ResolveObjectName(DbObjectType.View, ViewName);

			if (!IfExists &&
				!context.Query.ViewExists(viewName))
				throw new ObjectNotFoundException(ViewName);

			return new Prepared(viewName, IfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public ObjectName ViewName { get; set; }

			public bool IfExists { get; set; }

			public Prepared(ObjectName viewName, bool ifExists) {
				ViewName = viewName;
				IfExists = ifExists;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				ViewName = (ObjectName) info.GetValue("ViewName", typeof(ObjectName));
				IfExists = info.GetBoolean("IfExists");
			}

			protected override void GetData(SerializationInfo info) {
				info.AddValue("ViewName", ViewName);
				info.AddValue("IfExists", IfExists);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				if (!context.Request.Query.UserCanDropObject(DbObjectType.View, ViewName))
					throw new MissingPrivilegesException(context.Request.Query.UserName(), ViewName, Privileges.Drop);

				// If the 'only if exists' flag is false, we need to check tables to drop
				// exist first.
				if (!IfExists) {
					// If view doesn't exist, throw an error
					if (!context.Request.Query.ViewExists(ViewName)) {
						throw new ObjectNotFoundException(ViewName,
							String.Format("The view '{0}' does not exist and cannot be dropped.", ViewName));
					}
				}

				// Does the table already exist?
				if (context.Request.Query.ViewExists(ViewName)) {
					// Drop table in the transaction
					context.Request.Query.DropObject(DbObjectType.View, ViewName);

					// Revoke all the grants on the table
					context.Request.Query.RevokeAllGrantsOnView(ViewName);
				}
			}
		}

		#endregion
	}
}
