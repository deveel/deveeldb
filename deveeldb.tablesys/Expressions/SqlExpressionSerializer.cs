using System;
using System.IO;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlExpressionSerializer {
		public static void Serialize(SqlExpression expression, BinaryWriter writer) {
			var visitor = new SqlExpressionSerializationVisitor(writer);
			visitor.Visit(expression);
		}

		public static SqlExpression Deserialize(BinaryReader reader) {
			var type = (SqlExpressionType) reader.ReadByte();

			return Read(type, reader);
		}

		private static SqlExpression Read(SqlExpressionType type, BinaryReader reader) {
			throw new NotImplementedException();
		}
	}
}