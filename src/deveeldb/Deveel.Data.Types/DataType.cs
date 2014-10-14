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
	public abstract class DataType : IComparer<DataObject>, IEquatable<DataType> {
		protected DataType(SqlTypeCode sqlType) 
			: this(sqlType.ToString().ToUpperInvariant(), sqlType) {
		}

		protected DataType(string name, SqlTypeCode sqlType) {
			SqlType = sqlType;
			Name = name;
		}

		public string Name { get; private set; }

		public SqlTypeCode SqlType { get; private set; }

		public virtual bool IsIndexable {
			get { return true; }
		}

		public bool IsPrimitive {
			get {
				return SqlType != SqlTypeCode.Object &&
				       SqlType != SqlTypeCode.Unknown;
			}
		}

		public virtual bool IsComparable(DataType type) {
			return SqlType == type.SqlType;
		}

		public virtual bool CanCastTo(DataType type) {
			return true;
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
					throw new NotSupportedException("Cannot resolve the given string to a primitive type.");

				if (node.HasSize)
					return PrimitiveTypes.Type(node.TypeName, node.Size);
				if (node.HasScale) {
					if (node.HasPrecision)
						return PrimitiveTypes.Type(node.TypeName, node.Scale, node.Precision);
					return PrimitiveTypes.Type(node.TypeName, node.Scale);
				}

				return PrimitiveTypes.Type(node.TypeName);
			} catch (SqlParseException) {
				throw new FormatException("Unable to parse the given string to a valid data type.");
			}
		}

		public virtual int Compare(DataObject x, DataObject y) {
			if (!IsComparable(x.Type) ||
				!IsComparable(y.Type))
				throw new NotSupportedException();

			if (!x.IsComparable(y))
				throw new NotSupportedException();

			if (x.IsNull && y.IsNull)
				return 0;
			if (x.IsNull && !y.IsNull)
				return 1;
			if (!x.IsNull && y.IsNull)
				return -1;

			if (!(x is IComparable))
				throw new NotSupportedException();

			return ((IComparable) x).CompareTo(y);
		}

		public override bool Equals(object obj) {
			var dataType = obj as DataType;
			if (dataType == null)
				return false;

			return Equals(dataType);
		}

		public override int GetHashCode() {
			return SqlType.GetHashCode();
		}

		public virtual bool Equals(DataType other) {
			if (other == null)
				return false;

			return SqlType == other.SqlType;
		}
	}
}