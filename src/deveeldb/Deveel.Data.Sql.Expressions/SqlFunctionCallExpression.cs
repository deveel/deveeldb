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

using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlFunctionCallExpression : SqlExpression {
		internal SqlFunctionCallExpression(ObjectName functioName, InvokeArgument[] arguments) {
			Arguments = arguments;
			FunctioName = functioName;
		}

		private SqlFunctionCallExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			FunctioName = (ObjectName)info.GetValue("FunctionName", typeof(ObjectName));
			Arguments = (InvokeArgument[])info.GetValue("Arguments", typeof(InvokeArgument[]));
		}

		public ObjectName FunctioName { get; private set; }

		public InvokeArgument[] Arguments { get; private set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.FunctionCall; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("FunctionName", FunctioName, typeof(ObjectName));
			info.AddValue("Arguments", Arguments, typeof(InvokeArgument[]));
		}

		internal override void AppendTo(SqlStringBuilder builder) {
			FunctioName.AppendTo(builder);
			builder.Append("(");

			if (Arguments != null &&
				Arguments.Length > 0) {
				var args = Arguments;
				var argc = args.Length;

				for (int i = 0; i < argc; i++) {
					args[i].AppendTo(builder);

					if (i < argc - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");
		}
	}
}