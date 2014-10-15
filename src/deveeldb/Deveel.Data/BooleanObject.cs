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

using Deveel.Data.Types;

namespace Deveel.Data {
	/// <summary>
	/// Represents a boolean value within the system.
	/// </summary>
	/// <remarks>
	/// Booleans are bit values which consist of either 0 or 1, that
	/// are represented as <c>true</c> or <c>false</c>.
	/// </remarks>
	[Serializable]
	public sealed class BooleanObject : DataObject, IComparable<BooleanObject>, IComparable, IEquatable<BooleanObject>, IConvertible {
		private readonly bool? value;

		/// <summary>
		/// An object representing the boolean <c>true</c>.
		/// </summary>
		public static readonly BooleanObject True = new BooleanObject(PrimitiveTypes.Boolean(), true);

		/// <summary>
		/// An object representing the boolean <c>false</c>.
		/// </summary>
		public static readonly BooleanObject False = new BooleanObject(PrimitiveTypes.Boolean(), false);

		/// <summary>
		/// The <c>null</c> representation of booleans.
		/// </summary>
		public static readonly BooleanObject Null = new BooleanObject(PrimitiveTypes.Boolean(), null);

		internal BooleanObject(BooleanType type, bool? value) 
			: base(type) {
			this.value = value;
		}

		public override bool IsNull {
			get { return value == null; }
		}

		public int CompareTo(BooleanObject other) {
			if (other == null)
				throw new ArgumentNullException("other");

			if (value == null && other.value != null)
				return 1;
			if (value != null && other.value == null)
				return -1;
			if (value == null && value == null)
				return 0;

			return value.Value.CompareTo(other.value);
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is BooleanObject))
				throw new ArgumentException();

			return CompareTo((BooleanObject) obj);
		}

		public bool Equals(BooleanObject other) {
			if (value == null &&
			    other.value == null)
				return true;

			if (value == null && other.value != null)
				return false;

			return value.Equals(other.value);
		}

		public override bool Equals(object obj) {
			var other = obj as BooleanObject;
			if (other == null)
				return false;

			return Equals(other);
		}

		public override int GetHashCode() {
			return value == null ? 0 : value.GetHashCode();
		}

		public override string ToString() {
			return (value == null ? "NULL" : value == true ? "TRUE" : "FALSE");
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Boolean;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return (short) ToInt32();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			return (ushort) ToInt32();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			return (uint) ToInt32();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return (ulong) ToInt64();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToInt32();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToInt32();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			return ToInt32();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof (bool))
				return ToBoolean();
			if (conversionType == typeof (byte))
				return ToByte();
			if (conversionType == typeof (short))
				return (short) ToInt32();
			if (conversionType == typeof (int))
				return ToInt32();
			if (conversionType == typeof (long))
				return ToInt64();
			if (conversionType == typeof (float))
				return (float) ToInt32();
			if (conversionType == typeof (double))
				return (double) ToInt32();
			if (conversionType == typeof (decimal))
				return (decimal) ToInt32();

			if (conversionType == typeof (string))
				return ToString();

			throw new InvalidCastException();
		}

		/// <summary>
		/// Converts the inner value the object to a runtime
		/// boolean value.
		/// </summary>
		/// <remarks>
		/// If the inner value of the object is <c>null</c>, this method will
		/// throw a <see cref="NullReferenceException"/>.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="bool"/> that is either <c>true</c> or <c>false</c>.
		/// </returns>
		/// <exception cref="NullReferenceException">
		/// If the inner value of the instance is <c>null</c>.
		/// </exception>
		public bool ToBoolean() {
			if (value == null)
				throw new NullReferenceException();

			return value.Value;
		}

		public byte ToByte() {
			return (byte) ToInt32();
		}

		public int ToInt32() {
			return (int) ToInt64();
		}

		public long ToInt64() {
			if (value == null)
				throw new NullReferenceException();

			return value.Value ? 1 : 0;
		}

		public static bool operator ==(BooleanObject a, BooleanObject b) {
			if (Equals(a, null) && Equals(b, null))
				return true;
			if (Equals(a, null))
				return false;

			return a.Equals(b);
		}

		public static bool operator !=(BooleanObject a, BooleanObject b) {
			return !(a == b);
		}

		public static implicit operator BooleanObject(bool? b) {
			return new BooleanObject(PrimitiveTypes.Boolean(), b);
		}

		public static implicit operator BooleanObject(bool b) {
			return new BooleanObject(PrimitiveTypes.Boolean(), b);
		}

		public static implicit operator bool(BooleanObject obj) {
			if (obj.value == null)
				throw new NullReferenceException();

			return obj.value.Value;
		}
	}
}
