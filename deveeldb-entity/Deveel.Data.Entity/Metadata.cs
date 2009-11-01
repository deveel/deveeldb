using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;
using System.Globalization;

namespace Deveel.Data.Entity {
	internal class Metadata {
		public static string GetNumericLiteral(PrimitiveTypeKind type, object value) {
			switch (type) {
				case PrimitiveTypeKind.Byte:
				case PrimitiveTypeKind.Int16:
				case PrimitiveTypeKind.Int32:
				case PrimitiveTypeKind.Int64:
				case PrimitiveTypeKind.SByte:
					return value.ToString();
				case PrimitiveTypeKind.Double:
					return ((double)value).ToString("R", CultureInfo.InvariantCulture);
				case PrimitiveTypeKind.Single:
					return ((float)value).ToString("R", CultureInfo.InvariantCulture);
				case PrimitiveTypeKind.Decimal:
					return ((decimal)value).ToString(CultureInfo.InvariantCulture);
			}
			return null;
		}

		public static bool IsNumericType(TypeUsage typeUsage) {
			PrimitiveType pt = (PrimitiveType)typeUsage.EdmType;

			switch (pt.PrimitiveTypeKind) {
				case PrimitiveTypeKind.Byte:
				case PrimitiveTypeKind.Double:
				case PrimitiveTypeKind.Single:
				case PrimitiveTypeKind.Int16:
				case PrimitiveTypeKind.Int32:
				case PrimitiveTypeKind.Int64:
				case PrimitiveTypeKind.SByte:
					return true;
				default:
					return false;
			}
		}

		public static DbType GetDbType(TypeUsage typeUsage) {
			PrimitiveType pt = (PrimitiveType) typeUsage.EdmType;

			switch (pt.PrimitiveTypeKind) {
				case PrimitiveTypeKind.Binary:
					return DbType.Binary;
				case PrimitiveTypeKind.Boolean:
					return DbType.Boolean;
				case PrimitiveTypeKind.Byte:
					return DbType.Byte;
				case PrimitiveTypeKind.DateTime:
					return DbType.DateTime;
				case PrimitiveTypeKind.DateTimeOffset:
					return DbType.DateTime;
				case PrimitiveTypeKind.Decimal:
					return DbType.Decimal;
				case PrimitiveTypeKind.Double:
					return DbType.Double;
				case PrimitiveTypeKind.Single:
					return DbType.Single;
				case PrimitiveTypeKind.Guid:
					return DbType.Guid;
				case PrimitiveTypeKind.Int16:
					return DbType.Int16;
				case PrimitiveTypeKind.Int32:
					return DbType.Int32;
				case PrimitiveTypeKind.Int64:
					return DbType.Int64;
				case PrimitiveTypeKind.SByte:
					return DbType.SByte;
				case PrimitiveTypeKind.String:
					return DbType.String;
				case PrimitiveTypeKind.Time:
					return DbType.Time;
				default:
					throw new InvalidOperationException("Primitive type kind '" + pt.PrimitiveTypeKind + "' not supported.");
			}
		}

		public static string GetOperator(DbExpressionKind expressionKind) {
			switch (expressionKind) {
				case DbExpressionKind.Equals:
					return "=";
				case DbExpressionKind.LessThan:
					return "<";
				case DbExpressionKind.GreaterThan:
					return ">";
				case DbExpressionKind.LessThanOrEquals:
					return "<=";
				case DbExpressionKind.GreaterThanOrEquals:
					return ">=";
				case DbExpressionKind.NotEquals:
					return "!=";
				case DbExpressionKind.LeftOuterJoin:
					return "LEFT OUTER JOIN";
				case DbExpressionKind.InnerJoin:
					return "INNER JOIN";
				case DbExpressionKind.FullOuterJoin:
					return "OUTER JOIN";
			}
			throw new NotSupportedException("expression kind not supported");
		}

		internal static IList<EdmProperty> GetProperties(EdmType type) {
			if (type is EntityType)
				return ((EntityType)type).Properties;
			if (type is ComplexType)
				return ((ComplexType)type).Properties;
			if (type is RowType)
				return ((RowType)type).Properties;
			throw new NotSupportedException();
		}
	}
}