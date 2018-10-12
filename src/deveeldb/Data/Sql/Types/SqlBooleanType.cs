// 
//  Copyright 2010-2018 Deveel
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
using System.IO;

namespace Deveel.Data.Sql.Types {
	public sealed class SqlBooleanType : SqlType {
		public SqlBooleanType(SqlTypeCode typeCode) 
			: base(typeCode) {
			AssertIsBoolean(typeCode);
		}

		private static void AssertIsBoolean(SqlTypeCode sqlType) {
			if (!IsBooleanType(sqlType))
				throw new ArgumentException($"The SQL type {sqlType} is not BOOLEAN.");
		}

		internal static bool IsBooleanType(SqlTypeCode sqlType) {
			return (sqlType == SqlTypeCode.Bit ||
			        sqlType == SqlTypeCode.Boolean);
		}

		public override int Compare(ISqlValue x, ISqlValue y) {
			if (!(x is SqlBoolean))
				throw new ArgumentException("Arguments of a boolean comparison must be boolean", "x");
			if (!(y is SqlBoolean))
				throw new ArgumentException("Arguments of a boolean comparison must be boolean", "y");

			var a = (SqlBoolean)x;
			var b = (SqlBoolean)y;

			return a.CompareTo(b);
		}

		public override bool IsComparable(SqlType type) {
			return type is SqlBooleanType;
		}

		public override bool IsInstanceOf(ISqlValue value) {
			return value is SqlBoolean || value is SqlNull;
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			return destType is SqlNumericType ||
			       destType is SqlCharacterType ||
				   destType is SqlBinaryType ||
				   destType is SqlBooleanType;
		}

		public override ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (!(value is SqlBoolean))
				throw new ArgumentException($"Cannot cast from {ToString()} to {destType}");

			var b = (SqlBoolean) value;

			if (destType is SqlBooleanType)
				return b;
			if (destType is SqlNumericType)
				return ToNumber(b);

			if (destType is SqlBinaryType)
				return ToBinary(b);

			if (destType is SqlCharacterType)
				return ToString(b, (SqlCharacterType) destType);

			return base.Cast(value, destType);
		}

		private SqlNumber ToNumber(SqlBoolean value) {
			return value ? SqlNumber.One : SqlNumber.Zero;
		}

		private SqlBinary ToBinary(SqlBoolean value) {
			var bytes = new[] { value ? (byte)1 : (byte)0 };
			return new SqlBinary(bytes);
		}

		private ISqlString ToString(SqlBoolean value, SqlCharacterType destType) {
			var s = new SqlString(ToSqlString(value));
			return (ISqlString) destType.NormalizeValue(s);
		}

		public override ISqlValue Negate(ISqlValue value) {
			var b = (SqlBoolean)value;
			return !b;
		}

		public override ISqlValue And(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 & b2;
		}

		public override ISqlValue Or(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 | b2;
		}

		public override ISqlValue XOr(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 ^ b2;
		}

		public override SqlBoolean Greater(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 > b2;
		}

		public override SqlBoolean GreaterOrEqual(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 >= b2;
		}

		public override SqlBoolean Less(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 < b2;
		}

		public override SqlBoolean LessOrEqual(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1 <= b2;
		}

		public override SqlBoolean Equal(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;
			return b1.Equals(b2);
		}

		public override SqlBoolean NotEqual(ISqlValue a, ISqlValue b) {
			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;
			return !b1.Equals(b2);
		}

		public override bool Equals(SqlType other) {
			return other is SqlBooleanType;
		}

		public override string ToSqlString(ISqlValue obj) {
			var b = (SqlBoolean) obj;

			switch (TypeCode) {
				case SqlTypeCode.Bit:
					return b == true ? "1" : "0";
				case SqlTypeCode.Boolean:
				default:
					return b == true ? "TRUE" : "FALSE";
			}
		}
	}
}