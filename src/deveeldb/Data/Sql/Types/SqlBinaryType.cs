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
using System.Linq;

namespace Deveel.Data.Sql.Types {
	public sealed class SqlBinaryType : SqlType {
		public const int DefaultMaxSize = Int16.MaxValue;

		public SqlBinaryType(SqlTypeCode typeCode) 
			: this(typeCode, DefaultMaxSize) {
		}

		public SqlBinaryType(SqlTypeCode typeCode, int maxSize) 
			: base(typeCode) {
			MaxSize = maxSize;
			AssertIsBinary(typeCode);
		}

		public int MaxSize { get; private set; }

		public override bool IsIndexable => false;

		private static void AssertIsBinary(SqlTypeCode sqlType) {
			if (!IsBinaryType(sqlType))
				throw new ArgumentException(String.Format("The SQL type {0} is not a BINARY", sqlType));
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append(TypeCode.ToString().ToUpperInvariant());

			if (MaxSize > 0)
				builder.AppendFormat("({0})", MaxSize);
		}

		private SqlBoolean ToBoolean(ISqlBinary binary) {
			if (binary.Length != 1)
				throw new InvalidCastException("Exactly one byte needed to cast to boolean.");

			var b = binary.First();
			if (b != 0 && b != 1)
				throw new InvalidCastException("The first byte of the binary is invalid for a boolean");

			return b == 1;
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			return destType is SqlBooleanType ||
			       destType is SqlNumericType ||
				   destType is SqlCharacterType ||
				   destType is SqlDateTimeType;
		}

		public override ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (!(value is ISqlBinary))
				throw new ArgumentException();

			var binary = ((ISqlBinary) value);

			if (destType is SqlBooleanType)
				return ToBoolean(binary);
			if (destType is SqlCharacterType)
				return ToString(binary, (SqlCharacterType) destType);
			if (destType is SqlNumericType)
				return ToNumber(binary, (SqlNumericType) destType);
			if (destType is SqlDateTimeType)
				return ToDate(binary);

			return base.Cast(value, destType);
		}

		private SqlDateTime ToDate(ISqlBinary binary) {
			if (binary == null)
				throw new InvalidCastException();

			var bytes = binary.ToArray();
			return new SqlDateTime(bytes);
		}

		private ISqlValue ToNumber(ISqlBinary value, SqlNumericType destType) {
			if (value == null)
				throw new InvalidCastException();

			var precision = destType.Precision;
			var scale = destType.Scale;

			if (precision <= 0)
				throw new NotSupportedException();
			if (scale <= 0)
				scale = 1;

			// TODO: handle the cases when precision and/or scale are not set
			return new SqlNumber(value.ToArray(), scale, precision);
		}

		private ISqlValue ToString(ISqlBinary binary, SqlCharacterType destType) {
			if (binary == null)
				throw new InvalidCastException();

			var bytes = binary.ToArray();
			var s = new SqlString(bytes);

			return destType.NormalizeValue(s);
		}

		internal static bool IsBinaryType(SqlTypeCode sqlType) {
			return sqlType == SqlTypeCode.Binary ||
			       sqlType == SqlTypeCode.VarBinary ||
			       sqlType == SqlTypeCode.LongVarBinary ||
			       sqlType == SqlTypeCode.Blob;
		}

		public override bool IsInstanceOf(ISqlValue value) {
			return value is ISqlBinary || value is SqlNull;
		}

		public override bool Equals(SqlType other) {
			var binType = other as SqlBinaryType;
			if (binType == null)
				return false;

			return TypeCode == binType.TypeCode &&
			       MaxSize == binType.MaxSize;
		}
	}
}