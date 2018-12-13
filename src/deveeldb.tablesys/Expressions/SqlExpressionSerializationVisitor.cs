using System;
using System.IO;

namespace Deveel.Data.Sql.Expressions {
	class SqlExpressionSerializationVisitor : SqlExpressionVisitor {
		private BinaryWriter writer;

		public SqlExpressionSerializationVisitor(BinaryWriter writer) {
			this.writer = writer;
		}
	}
}