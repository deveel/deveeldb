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

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class ShowStatement : SqlStatement {
		public ShowStatement(ShowTarget target) {
			Target = target;
		}

		public ShowTarget Target { get; private set; }

		public ObjectName TableName { get; set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			ObjectName tableName = null;

			if (Target == ShowTarget.Table &&
			    TableName != null) {
				tableName = context.Access.ResolveTableName(TableName);
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

			private Prepared(SerializationInfo info, StreamingContext context) {
				TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
				Target = (ShowTarget) info.GetInt32("Target");
			}

			public ObjectName TableName { get; private set; }

			public ShowTarget Target { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("TableName", TableName);
				info.AddValue("Target", (int)Target);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
