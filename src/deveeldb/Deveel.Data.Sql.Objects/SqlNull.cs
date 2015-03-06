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

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlNull : ISqlObject, IConvertible {
		public static readonly SqlNull Value = new SqlNull();

		int IComparable.CompareTo(object obj) {
			return (this as ISqlObject).CompareTo((ISqlObject) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull {
			get { return true; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public override bool Equals(object obj) {
			if (obj is SqlNull)
				return true;
			if (obj is ISqlObject)
				return ((ISqlObject) obj).IsNull;

			return false;
		}

		public override int GetHashCode() {
			return 0;
		}

		public static bool operator ==(SqlNull a, ISqlObject b) {
			if (Equals(a, b))
				return true;
			if (b == null || b.IsNull)
				return true;

			return false;
		}

		public static bool operator !=(SqlNull a, ISqlObject b) {
			return !(a == b);
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.DBNull;
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
			throw new InvalidCastException();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			throw new InvalidCastException();
		}

		public override string ToString() {
			return "NULL";
		}
	}
}