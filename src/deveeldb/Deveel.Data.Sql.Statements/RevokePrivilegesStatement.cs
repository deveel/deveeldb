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
			var objectName = context.Query.ResolveTableName(ObjectName);

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

			private Prepared(SerializationInfo info, StreamingContext context) {
				Grantee = info.GetString("Grantee");
				Privileges = (Privileges) info.GetInt32("Privileges");
				ObjectName = (ObjectName) info.GetValue("ObjectName", typeof(ObjectName));
				GrantOption = info.GetBoolean("GrantOption");
			}

			public string Grantee { get; private set; }

			public Privileges Privileges { get; private set; }

			public string[] Columns { get; private set; }

			public ObjectName ObjectName { get; private set; }

			public bool GrantOption { get; private set; }

			protected override void GetData(SerializationInfo info, StreamingContext context) {
				info.AddValue("Grantee", Grantee);
				info.AddValue("Privileges", (int)Privileges);
				info.AddValue("ObjectName", ObjectName);
				info.AddValue("GrantOption", GrantOption);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
