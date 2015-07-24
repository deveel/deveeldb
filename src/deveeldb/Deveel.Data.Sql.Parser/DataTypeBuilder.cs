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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Parser {
	class DataTypeBuilder {
		public DataType Build(ISqlNode sqlNode) {
			return Build(null, sqlNode);
		}

		public DataType Build(IQueryContext context, ISqlNode sqlNode) {
			var node = sqlNode as DataTypeNode;
			if (node == null)
				throw new ArgumentException();

			var typeName = node.TypeName;

			if (node.IsPrimitive) {
				SqlTypeCode sqlTypeCode;
				if (String.Equals(typeName, "LONG VARCHAR")) {
					sqlTypeCode = SqlTypeCode.LongVarChar;
				} else if (String.Equals(node.TypeName, "LONG VARBINARY")) {
					sqlTypeCode = SqlTypeCode.LongVarBinary;
				} else if (String.Equals(typeName, "INT", StringComparison.OrdinalIgnoreCase)) {
					sqlTypeCode = SqlTypeCode.Integer;
				} else {
					try {
						sqlTypeCode = (SqlTypeCode) Enum.Parse(typeof (SqlTypeCode), node.TypeName, true);
					} catch (Exception ex) {
						sqlTypeCode = SqlTypeCode.Unknown;
					}
				}

				if (sqlTypeCode == SqlTypeCode.Bit ||
				    sqlTypeCode == SqlTypeCode.Boolean ||
				    sqlTypeCode == SqlTypeCode.BigInt ||
				    sqlTypeCode == SqlTypeCode.Integer ||
				    sqlTypeCode == SqlTypeCode.SmallInt ||
				    sqlTypeCode == SqlTypeCode.TinyInt)
					return PrimitiveTypes.Resolve(sqlTypeCode,typeName);

				if (sqlTypeCode == SqlTypeCode.Float ||
				    sqlTypeCode == SqlTypeCode.Real ||
				    sqlTypeCode == SqlTypeCode.Double ||
				    sqlTypeCode == SqlTypeCode.Decimal ||
				    sqlTypeCode == SqlTypeCode.Numeric) {
					var typeMeta = new List<DataTypeMeta>();
					if (node.HasScale)
						typeMeta.Add(new DataTypeMeta("Scale", node.Scale.ToString()));
					if (node.HasPrecision)
						typeMeta.Add(new DataTypeMeta("Precision", node.Precision.ToString()));

					return PrimitiveTypes.Resolve(sqlTypeCode, typeName, typeMeta.ToArray());
				}

				if (sqlTypeCode == SqlTypeCode.Char ||
				    sqlTypeCode == SqlTypeCode.VarChar ||
				    sqlTypeCode == SqlTypeCode.LongVarChar) {
						var typeMeta = new List<DataTypeMeta>();
					if (node.HasSize)
						typeMeta.Add(new DataTypeMeta("MaxSize", node.Size.ToString()));
					if (node.HasLocale)
						typeMeta.Add(new DataTypeMeta("Locale", node.Locale));
					if (node.HasEncoding)
						typeMeta.Add(new DataTypeMeta("Encoding", node.Encoding));

					return PrimitiveTypes.Resolve(sqlTypeCode, typeName, typeMeta.ToArray());
				}

				if (sqlTypeCode == SqlTypeCode.Binary ||
				    sqlTypeCode == SqlTypeCode.VarBinary ||
				    sqlTypeCode == SqlTypeCode.LongVarBinary) {
					var typeMeta = new List<DataTypeMeta>();
					if (node.HasSize)
						typeMeta.Add(new DataTypeMeta("MaxSize", node.Size.ToString()));

					return PrimitiveTypes.Resolve(sqlTypeCode, typeName, typeMeta.ToArray());
				}

				if (sqlTypeCode == SqlTypeCode.Date ||
				    sqlTypeCode == SqlTypeCode.Time ||
				    sqlTypeCode == SqlTypeCode.TimeStamp)
					return PrimitiveTypes.Resolve(sqlTypeCode, typeName);

				// TODO: Support %ROWTYPE and %TYPE
			}

			if (String.IsNullOrEmpty(typeName))
				throw new SqlParseException("Could not determine type name.");

			if (context == null)
				throw new SqlParseException(String.Format("The type {0} could not be resolved.", node.TypeName));

			DataTypeMeta[] meta = null;
			if (node.Metadata != null)
				meta = BuildTypeMeta(node.Metadata);

			var type = context.ResolveType(typeName, meta);

			if (type == null)
				throw new SqlParseException(String.Format("User defined type {0} could not be resolved.", typeName));

			return type;
		}

		private DataTypeMeta[] BuildTypeMeta(Dictionary<string, string> metadata) {
			return metadata.Select(x => new DataTypeMeta(x.Key, x.Value)).ToArray();
		}
	}
}
