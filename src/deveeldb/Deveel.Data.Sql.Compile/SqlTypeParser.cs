using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	static class SqlTypeParser {
		public static SqlType Parse(PlSqlParser.DataTypeSpecContext context) {
			var typeInfo = new DataTypeVisitor().Visit(context);
			if (!typeInfo.IsPrimitive)
				throw new NotSupportedException();

			return PrimitiveTypes.Resolve(typeInfo.TypeName, typeInfo.Metadata);
		}

		public static SqlType Parse(PlSqlParser.Type_specContext context) {
			var typeInfo = new DataTypeVisitor().Visit(context);
			if (!typeInfo.IsPrimitive)
				throw new NotSupportedException();

			return PrimitiveTypes.Resolve(typeInfo.TypeName, typeInfo.Metadata);
		}

		public static SqlType Parse(PlSqlParser.DatatypeContext context) {
			var typeInfo = new DataTypeVisitor().Visit(context);
			if (!typeInfo.IsPrimitive)
				throw new NotSupportedException();

			return PrimitiveTypes.Resolve(typeInfo.TypeName, typeInfo.Metadata);
		}
	}
}
