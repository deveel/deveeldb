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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class ShowStatement : SqlStatement, IPreparableStatement {
		public ShowStatement(ShowTarget target) {
			Target = target;
		}

		public ShowTarget Target { get; private set; }

		public ObjectName TableName { get; set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		IStatement IPreparableStatement.Prepare(IRequest request) {
			ObjectName tableName = null;

			if (Target == ShowTarget.Table &&
			    TableName != null) {
				tableName = request.Query.ResolveTableName(TableName);
			}

			return new Prepared(Target, tableName);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {

			public Prepared(ShowTarget target, ObjectName tableName) {
				Target = target;
				TableName = tableName;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				Target = (ShowTarget) data.GetInt32("Target");
			}

			public ObjectName TableName { get; private set; }

			public ShowTarget Target { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("Target", (int)Target);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
