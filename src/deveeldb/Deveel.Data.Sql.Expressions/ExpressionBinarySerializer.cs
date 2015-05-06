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
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Expressions {
	class ExpressionBinarySerializer : SqlExpressionVisitor {
		private BinaryWriter writer;

		public ExpressionBinarySerializer(Stream outputStream) {
			writer = new BinaryWriter(outputStream, Encoding.Unicode);
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (expression != null)
				writer.Write((byte) expression.ExpressionType);

			return base.Visit(expression);
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			var functionName = expression.FunctioName.FullName;
			var argc = expression.Arguments != null ? expression.Arguments.Length : 0;

			writer.Write(functionName);
			writer.Write(argc);

			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			var refName = reference.ReferenceName.FullName;
			writer.Write(refName);

			return base.VisitReference(reference);
		}
	}
}
