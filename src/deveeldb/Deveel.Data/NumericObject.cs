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
using Deveel.Math;

namespace Deveel.Data {
	[Serializable]
	public sealed class NumericObject : DataObject, IComparable<NumericObject>, IComparable, IEquatable<NumericObject>, IConvertible {
		private readonly BigDecimal innerValue;
		private int byteCount = 120;
		private long valueAsLong;

		public static readonly NumericObject Zero = new NumericObject(PrimitiveTypes.Numeric(), BigDecimal.Zero);
		public static readonly NumericObject One = new NumericObject(PrimitiveTypes.Numeric(), BigDecimal.One);

		public static readonly NumericObject NaN = new NumericObject(PrimitiveTypes.Numeric(), NumericState.NotANumber, -1);
		public static readonly NumericObject NegativeInfinity = new NumericObject(PrimitiveTypes.Numeric(), NumericState.NegativeInfinity, -1);
		public static readonly NumericObject PositiveInfinity = new NumericObject(PrimitiveTypes.Numeric(), NumericState.PositiveInfinity, -1);

		private NumericObject(DataType type, BigDecimal value) 
			: this(type, NumericState.None, value) {
		}

		private NumericObject(DataType type, NumericState state, BigDecimal value)
			: base(type) {
			if (value.Scale == 0) {
				BigInteger bint = value.ToBigInteger();
				int bitCount = bint.BitLength;
				if (bitCount < 30) {
					valueAsLong = bint.ToInt64();
					byteCount = 4;
				} else if (bitCount < 60) {
					valueAsLong = bint.ToInt64();
					byteCount = 8;
				}
			}

			innerValue = value;
			State = state;
		}

		private NumericObject(DataType type, NumericState state, byte[] bytes, int scale)
			: this(type, state, new BigDecimal(new BigInteger(bytes), scale)) {
		}

		public NumericState State { get; private set; }

		public bool CanBeInt64 {
			get { return byteCount <= 8; }
		}

		public bool CanBeInt32 {
			get { return byteCount <= 4; }
		}

		public int Scale {
			get { return State == NumericState.None ? innerValue.Scale : -1; }
		}

		public int Precision {
			get { return State == NumericState.None ? innerValue.Precision : -1; }
		}

		public int Sign {
			get { return State == NumericState.None ? innerValue.Sign : -1; }
		}

		TypeCode IConvertible.GetTypeCode() {
			if (CanBeInt32)
				return TypeCode.Int32;
			if (CanBeInt64)
				return TypeCode.Int64;

			return TypeCode.Object;
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

		int IComparable.CompareTo(object obj) {
			throw new NotImplementedException();
		}

		public int CompareTo(NumericObject other) {
			if (State != NumericState.None)
				throw new InvalidOperationException("Cannot compare numeric states.");

			return innerValue.CompareTo(other.innerValue);
		}

		public bool Equals(NumericObject other) {
			if (State == NumericState.NegativeInfinity &&
			    other.State == NumericState.NegativeInfinity)
				return true;
			if (State == NumericState.PositiveInfinity &&
			    other.State == NumericState.PositiveInfinity)
				return true;
			if (State == NumericState.NotANumber &&
			    other.State == NumericState.NotANumber)
				return true;

			return innerValue.CompareTo(other.innerValue) == 0;
		}

		public override bool Equals(object obj) {
			if (!(obj is NumericObject))
				throw new ArgumentException("The object is not a Numeric.");

			return Equals((NumericObject) obj);
		}

		public override int GetHashCode() {
			return innerValue.GetHashCode() ^ State.GetHashCode();
		}
	}
}