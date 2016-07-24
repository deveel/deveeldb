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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ReturnStatement : SqlStatement, IPlSqlStatement {
		public ReturnStatement() 
			: this(null) {
		}

		public ReturnStatement(SqlExpression returnExpression) {
			ReturnExpression = returnExpression;
		}


		private ReturnStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ReturnExpression = (SqlExpression) info.GetValue("Return", typeof (SqlExpression));
		}

		public SqlExpression ReturnExpression { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var expression = ReturnExpression;
			if (expression != null)
				expression = expression.Prepare(preparer);

			return new ReturnStatement(expression);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			return new ReturnStatement(ReturnExpression);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Return(ReturnExpression);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Return", ReturnExpression);
			base.GetData(info);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("RETURN");
			if (ReturnExpression != null) {
				builder.Append(" ");
				ReturnExpression.AppendTo(builder);
			}
		}
	}
}
