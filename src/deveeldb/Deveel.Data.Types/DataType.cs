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
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Types {
	public abstract class DataType : IComparer<DataObject> {
		protected DataType(SqlType sqlType) 
			: this(sqlType.ToString().ToUpperInvariant(), sqlType) {
		}

		protected DataType(string name, SqlType sqlType) {
			SqlType = sqlType;
			Name = name;
		}

		public string Name { get; private set; }

		public SqlType SqlType { get; private set; }

		public virtual bool IsIndexable {
			get { return true; }
		}

		public bool IsPrimitive {
			get {
				return SqlType != SqlType.Object &&
				       SqlType != SqlType.Unknown;
			}
		}

		public virtual bool IsComparable(DataType type) {
			return SqlType == type.SqlType;
		}

		public virtual bool CanCastTo(DataType type) {
			return false;
		}

		public virtual DataObject CastTo(DataObject value, DataType destType) {
			throw new NotSupportedException();
		}

		public virtual int SizeOf(DataObject obj) {
			return 0;
		}

		public virtual DataType Wider(DataType otherType) {
			return this;
		}

		public static DataType Parse(string s) {
			var sqlCompiler = new SqlCompiler();

			try {
				var node = sqlCompiler.CompileDataType(s);
				if (!node.IsPrimitive)
					throw new InvalidOperationException("Cannot resolve the given string to a primitive type.");

				return GetFromPrimitive(node.TypeName, node.Size, node.Scale);
			} catch (SqlParseException) {
				throw new FormatException("Unable to parse the given string to a valid data type.");
			}
		}

		private static DataType GetFromPrimitive(string typeName, int size, byte scale) {
			if (String.Equals(typeName, "BIT", StringComparison.InvariantCultureIgnoreCase) ||
			    String.Equals(typeName, "BOOLEAN", StringComparison.InvariantCultureIgnoreCase))
				return PrimitiveTypes.Boolean;
			if (String.Equals(typeName, "INT", StringComparison.InvariantCultureIgnoreCase) ||
			    String.Equals(typeName, "INTEGER", StringComparison.InvariantCultureIgnoreCase))
				return PrimitiveTypes.Integer;

			throw new NotSupportedException(String.Format("The type {0} is not primitive.", typeName));
		}

		public virtual int Compare(DataObject x, DataObject y) {
			throw new NotSupportedException();
		}
	}
}