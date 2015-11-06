using System;
using System.Data;
using System.Text;

using IQToolkit.Data;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbTypeSystem : DbTypeSystem {
		public override SqlDbType GetSqlType(string typeName) {
			typeName = typeName.ToUpperInvariant();

			switch (typeName) {
				case "VARCHAR":
				case "STRING":
				case "CLOB":
                    return SqlDbType.VarChar;
				case "BINARY":
				case "VARBINARY":
					return SqlDbType.VarBinary;
				case "BLOB":
					return SqlDbType.Binary;
				case "BOOLEAN":
					return SqlDbType.Bit;
				case "INTEGER":
					return SqlDbType.Int;
				case "NUMERIC":
					return SqlDbType.Decimal;
				case "TYPE":
					return SqlDbType.Udt;
				default:
					return base.GetSqlType(typeName);
			}
		}

		public override string GetVariableDeclaration(QueryType type, bool suppressSize) {
			// TODO: !!!

			var sb = new StringBuilder();
			var sqlType = (DbQueryType)type;
			var sqlDbType = sqlType.SqlDbType;

			switch (sqlDbType) {
				case SqlDbType.Bit: {
					sb.Append("BOOLEAN");
					break;
				}
				case SqlDbType.SmallInt: {
					sb.Append("SMALLINT");
					break;
				}
				case SqlDbType.Xml: {
					sb.Append("XMLNODE");
					break;
				}
			}

			return sb.ToString();
		}
	}
}
