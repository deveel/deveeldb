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
