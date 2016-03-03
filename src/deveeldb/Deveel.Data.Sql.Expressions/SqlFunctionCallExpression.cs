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

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlFunctionCallExpression : SqlExpression {
		internal SqlFunctionCallExpression(ObjectName functioName, SqlExpression[] arguments) {
			Arguments = arguments;
			FunctioName = functioName;
		}

		private SqlFunctionCallExpression(ObjectData data)
			: base(data) {
			FunctioName = data.GetValue<ObjectName>("FunctionName");
			Arguments = data.GetValue<SqlExpression[]>("Arguments");
		}

		public ObjectName FunctioName { get; private set; }

		public SqlExpression[] Arguments { get; private set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.FunctionCall; }
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("FunctionName", FunctioName);
			data.SetValue("Arguments", Arguments);
		}
	}
}