// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Types {
	public abstract class DataType {
		protected DataType(SqlType sqlType) 
			: this(sqlType.ToString().ToUpperInvariant(), sqlType) {
		}

		protected DataType(string name, SqlType sqlType) {
			SqlType = sqlType;
			Name = name;
		}

		public string Name { get; private set; }

		public SqlType SqlType { get; private set; }

		public virtual bool IsComparable(DataType type) {
			return SqlType == type.SqlType;
		}

		public virtual bool CanCastTo(DataType type) {
			return false;
		}

		public virtual object CastTo(DataObject value, DataType destType) {
			throw new NotSupportedException();
		}

		public virtual int SizeOf(DataObject obj) {
			return 0;
		}

		public static DataType Parse(string s) {
			var sqlCompiler = new SqlCompiler();

			try {
				var node = sqlCompiler.CompileDataType(s);
				if (!node.IsPrimitive)
					throw new InvalidOperationException("Cannot resolve the given string to a primitive type.");

				return GetFromPrimitive(node.TypeName, node.Size, node.Precision, node.Scale);
			} catch (SqlParseException) {
				throw new FormatException("Unable to parse the given string to a valid data type.");
			}
		}

		private static DataType GetFromPrimitive(string typeName, int size, int precision, byte scale) {
			throw new NotImplementedException();
		}
	}
}