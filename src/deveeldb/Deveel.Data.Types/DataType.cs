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
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	public abstract class DataType : IComparer<ISqlObject>, IEquatable<DataType> {
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

		public virtual ISqlObject Add(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Subtract(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Multiply(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Divide(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Modulus(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Negate(ISqlObject value) {
			return SqlNull.Value;
		}

		public virtual SqlBoolean IsEqualTo(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean IsNotEqualTo(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean IsGreatherThan(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean IsSmallerThan(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean IsGreaterOrEqualThan(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean IsSmallerOrEqualThan(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual SqlBoolean And(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual ISqlObject And(ISqlObject value) {
			return SqlNull.Value;
		}

		public virtual SqlBoolean Or(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual ISqlObject Or(ISqlObject value) {
			return SqlNull.Value;
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

				SqlTypeCode sqlTypeCode;
				if (String.Equals(node.TypeName, "LONG VARCHAR")) {
					sqlTypeCode = SqlTypeCode.LongVarChar;
				} else if (String.Equals(node.TypeName, "LONG VARBINARY")) {
					sqlTypeCode = SqlTypeCode.LongVarBinary;
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

				throw new NotSupportedException(String.Format("The SQL type {0} is not supported here.", sqlTypeCode));
			} catch (SqlParseException) {
				throw new FormatException("Unable to parse the given string to a valid data type.");
			}
		}

		public virtual int Compare(ISqlObject x, ISqlObject y) {
			if (!x.IsComparableTo(y))
				throw new NotSupportedException();

			if (x.IsNull && y.IsNull)
				return 0;
			if (x.IsNull && !y.IsNull)
				return 1;
			if (!x.IsNull && y.IsNull)
				return -1;

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