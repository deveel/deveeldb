using System;
using System.IO;

namespace Deveel.Data.Sql.Expressions {
	static class SqlExpressionSerializer {
		public static void SerializeTo(Stream stream, SqlExpression expression) {
			var serializer = new ExpressionBinarySerializer(stream);
			serializer.Visit(expression);
		}

		public static SqlExpression DeserializeFrom(Stream stream) {
			throw new NotImplementedException();
		}
	}
}
