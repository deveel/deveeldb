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

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlBoolean : ISqlObject, IComparable<SqlBoolean>, IConvertible {
		private readonly byte? value;

		public static readonly SqlBoolean True = new SqlBoolean(1);
		public static readonly SqlBoolean False = new SqlBoolean(0);
		public static readonly SqlBoolean Null = new SqlBoolean(null);

		public SqlBoolean(byte value)
			: this() {
			if (value != 0 &&
				value != 1)
				throw new ArgumentOutOfRangeException("value");

			this.value = value;
		}

		public SqlBoolean(bool value)
			: this((byte)(value ? 1 : 0)) {
		}

		private SqlBoolean(byte? value)
			: this() {
			this.value = value;
		}

		int IComparable.CompareTo(object obj) {
			throw new NotImplementedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public bool IsNull { get; private set; }

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		TypeCode IConvertible.GetTypeCode() {
			throw new NotImplementedException();
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			throw new NotImplementedException();
		}

		public int CompareTo(SqlBoolean other) {
			throw new NotImplementedException();
		}

		public static bool operator ==(SqlBoolean a, SqlBoolean b) {
			return a.Equals(b);
		}

		public static bool operator !=(SqlBoolean a, SqlBoolean b) {
			return !(a == b);
		}

		public static implicit operator bool(SqlBoolean value) {
			if (value.IsNull)
				throw new InvalidCastException();

			return value.value == 1;
		}

		public static implicit operator SqlBoolean(bool value) {
			return new SqlBoolean(value);
		}

		public static SqlBoolean Parse(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			SqlBoolean value;
			if (!TryParse(s, out value))
				throw new FormatException();

			return value;
		}

		public static bool TryParse(string s, out SqlBoolean value) {
			value = new SqlBoolean();

			if (String.IsNullOrEmpty(s))
				return false;

			if (String.Equals(s, "true", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(s, "1")) {
				value = True;
				return true;
			}
			if (String.Equals(s, "false", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(s, "0")) {
				value = False;
				return true;
			}

			return false;
		}

		public override string ToString() {
			if (value == null)
				return "NULL";
			if (value == 1)
				return "true";
			if (value == 0)
				return "false";

			throw new InvalidOperationException("Should never happen!");
		}
	}
}