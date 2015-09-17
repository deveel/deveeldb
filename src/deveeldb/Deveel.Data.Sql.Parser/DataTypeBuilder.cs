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
		public SqlType Build(ISqlNode sqlNode) {
			return Build(null, sqlNode);
		}

		public SqlType Build(ITypeResolver resolver, ISqlNode sqlNode) {
			var node = sqlNode as DataTypeNode;
			if (node == null)
				throw new ArgumentException();

			var typeName = node.TypeName;
			var typeMeta = new List<DataTypeMeta>();
			SqlTypeCode sqlTypeCode;

			if (node.IsPrimitive) {
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
				 
				if (sqlTypeCode == SqlTypeCode.Float ||
				    sqlTypeCode == SqlTypeCode.Real ||
				    sqlTypeCode == SqlTypeCode.Double ||
				    sqlTypeCode == SqlTypeCode.Decimal ||
				    sqlTypeCode == SqlTypeCode.Numeric) {
					if (node.HasScale)
						typeMeta.Add(new DataTypeMeta("Scale", node.Scale.ToString()));
					if (node.HasPrecision)
						typeMeta.Add(new DataTypeMeta("Precision", node.Precision.ToString()));
				} else if (sqlTypeCode == SqlTypeCode.Char ||
				           sqlTypeCode == SqlTypeCode.VarChar ||
				           sqlTypeCode == SqlTypeCode.LongVarChar) {
					if (node.HasSize)
						typeMeta.Add(new DataTypeMeta("MaxSize", node.Size.ToString()));
					if (node.HasLocale)
						typeMeta.Add(new DataTypeMeta("Locale", node.Locale));
					if (node.HasEncoding)
						typeMeta.Add(new DataTypeMeta("Encoding", node.Encoding));
				} else if (sqlTypeCode == SqlTypeCode.Binary ||
				           sqlTypeCode == SqlTypeCode.VarBinary ||
				           sqlTypeCode == SqlTypeCode.LongVarBinary) {
					if (node.HasSize)
						typeMeta.Add(new DataTypeMeta("MaxSize", node.Size.ToString()));
				}

				// TODO: Support %ROWTYPE and %TYPE
			} else {
				sqlTypeCode = SqlTypeCode.Type;
			}

			if (String.IsNullOrEmpty(typeName))
				throw new SqlParseException("Could not determine type name.");

			DataTypeMeta[] meta = typeMeta.ToArray();
			if (!node.IsPrimitive && node.Metadata != null)
				meta = BuildTypeMeta(node.Metadata);

			var type = TypeResolver.Resolve(sqlTypeCode, typeName, meta, resolver);

			if (type == null)
				throw new SqlParseException(String.Format("User defined type {0} could not be resolved.", typeName));

			return type;
		}

		private DataTypeMeta[] BuildTypeMeta(Dictionary<string, string> metadata) {
			return metadata.Select(x => new DataTypeMeta(x.Key, x.Value)).ToArray();
		}
	}
}
