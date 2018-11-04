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
using System.Reflection;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A value object that represents the absence of value (<c>NULL</c>).
	/// </summary>
	[Serializable]
	public struct SqlNull : ISqlValue, IConvertible, ISqlFormattable {
		/// <summary>
		/// The default instance of a <c>NULL</c> value.
		/// </summary>
		public static readonly SqlNull Value = new SqlNull();

		int IComparable.CompareTo(object obj) {
			return (this as ISqlValue).CompareTo((ISqlValue) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			if (other == null || other is SqlNull)
				return 0;

			return -1;
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other == null || other is SqlNull;
		}

		public override bool Equals(object obj) {
			return obj is SqlNull || obj == null;
		}

		public override int GetHashCode() {
			return 0;
		}

		public static bool operator ==(SqlNull a, ISqlValue b) {
			return b == null || b is SqlNull;
		}

		public static bool operator !=(SqlNull a, ISqlValue b) {
			return !(a == b);
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Empty;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return null;
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(SqlNull))
				return null;
			if (conversionType.GetTypeInfo().IsClass)
				return null;

			throw new InvalidCastException();
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder sql) {
			sql.Append("NULL");
		}
	}
}