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

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlTupleExpression : SqlExpression {
		internal SqlTupleExpression(SqlExpression[] expressions) {
			Expressions = expressions;
		}

		private SqlTupleExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Expressions = (SqlExpression[])info.GetValue("Expressions", typeof(SqlExpression[]));
		}

		public SqlExpression[] Expressions { get; private set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Tuple; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Expressions", Expressions, typeof(SqlExpression[]));
		}

		internal override void AppendTo(SqlStringBuilder builder) {
			builder.Append("(");

			var sz = Expressions.Length;
			for (int i = 0; i < sz; i++) {
				Expressions[i].AppendTo(builder);

				if (i < sz - 1)
					builder.Append(", ");
			}

			builder.Append(")");

		}
	}
}