using System;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Types;

namespace Deveel.Data.Deveel.Data.Sql.Compile {
	public sealed class DataTypeBuilder {
		public DataType Build(ISqlNode sqlNode) {
			return Build(null, sqlNode);
		}

		public DataType Build(IDataTypeResolver resolver, ISqlNode sqlNode) {
			var node = sqlNode as DataTypeNode;
			if (node == null)
				throw new ArgumentException();

			SqlTypeCode sqlTypeCode;
			if (String.Equals(node.TypeName, "LONG VARCHAR")) {
				sqlTypeCode = SqlTypeCode.LongVarChar;
			} else if (String.Equals(node.TypeName, "LONG VARBINARY")) {
				sqlTypeCode = SqlTypeCode.LongVarBinary;
			} else if (String.Equals(node.TypeName, "INT", StringComparison.OrdinalIgnoreCase)) {
				sqlTypeCode = SqlTypeCode.Integer;
			} else {
				sqlTypeCode = (SqlTypeCode) Enum.Parse(typeof (SqlTypeCode), node.TypeName, true);
			}

			if (sqlTypeCode == SqlTypeCode.Bit ||
				sqlTypeCode == SqlTypeCode.Boolean ||
				sqlTypeCode == SqlTypeCode.BigInt ||
				sqlTypeCode == SqlTypeCode.Integer ||
				sqlTypeCode == SqlTypeCode.SmallInt ||
				sqlTypeCode == SqlTypeCode.TinyInt)
				return PrimitiveTypes.Type(sqlTypeCode);

			if (sqlTypeCode == SqlTypeCode.Float ||
				sqlTypeCode == SqlTypeCode.Real ||
				sqlTypeCode == SqlTypeCode.Double ||
				sqlTypeCode == SqlTypeCode.Decimal) {
				if (node.HasScale && node.HasPrecision)
					return PrimitiveTypes.Type(sqlTypeCode, node.Scale, node.Precision);
				if (node.HasScale && !node.HasPrecision)
					return PrimitiveTypes.Type(sqlTypeCode, node.Scale);

				return PrimitiveTypes.Type(sqlTypeCode);
			}

			if (sqlTypeCode == SqlTypeCode.Char ||
				sqlTypeCode == SqlTypeCode.VarChar ||
				sqlTypeCode == SqlTypeCode.LongVarChar) {
				if (node.HasSize && node.HasLocale)
					return PrimitiveTypes.Type(sqlTypeCode, node.Size, node.Locale);
				if (node.HasSize && !node.HasLocale)
					return PrimitiveTypes.Type(sqlTypeCode, node.Size);
				if (node.HasLocale && !node.HasSize)
					return PrimitiveTypes.Type(sqlTypeCode, node.Locale);

				return PrimitiveTypes.Type(sqlTypeCode);
			}

			if (sqlTypeCode == SqlTypeCode.Date ||
				sqlTypeCode == SqlTypeCode.Time ||
				sqlTypeCode == SqlTypeCode.TimeStamp)
				return PrimitiveTypes.Type(sqlTypeCode);


			// TODO: Support %ROWTYPE and %TYPE

			if (resolver == null || node.UserTypeName == null)
				throw new NotSupportedException(String.Format("The type {0} could not be resolved.", node.TypeName));

			var type = resolver.ResolveType(node.UserTypeName);
			if (type == null)
				throw new NotSupportedException(String.Format("User defined type {0} could not be resolved.", node.UserTypeName));

			return type;
		}
	}
}
