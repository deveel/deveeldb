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
using System.Diagnostics;

using Deveel.Data.Types;
using Deveel.Math;

namespace Deveel.Data {
	[Serializable]
	public sealed class NumericObject : DataObject, IComparable<NumericObject>, IComparable, IEquatable<NumericObject>,
		IConvertible {
		private readonly BigDecimal innerValue;
		private readonly int byteCount = 120;
		private readonly long valueAsLong;

		public static readonly NumericObject Zero = new NumericObject(PrimitiveTypes.Numeric(), NumericState.None, BigDecimal.Zero);
		public static readonly NumericObject One = new NumericObject(PrimitiveTypes.Numeric(), NumericState.None, BigDecimal.One);

		public static readonly NumericObject NaN = new NumericObject(PrimitiveTypes.Numeric(), NumericState.NotANumber, null);

		public static readonly NumericObject NegativeInfinity = new NumericObject(PrimitiveTypes.Numeric(),
			NumericState.NegativeInfinity, null);

		public static readonly NumericObject PositiveInfinity = new NumericObject(PrimitiveTypes.Numeric(),
			NumericState.PositiveInfinity, null);

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

		private NumericObject(DataType type, byte[] bytes, int scale, int precision)
			: this(type, new BigDecimal(new BigInteger(bytes), scale, new MathContext(precision))) {
		}

		public NumericState State { get; private set; }

		public override bool IsNull {
			get { return State == NumericState.None && innerValue == null; }
		}

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

		private MathContext MathContext {
			get { return State == NumericState.None ? new MathContext(Precision) : null; }
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
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException("Conversion of NUMERIC to Char is invalid.");
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException("Conversion to Signed Byte numbers not supported yet.");
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotSupportedException("Conversion to Unsigned numbers not supported yet.");
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotSupportedException("Conversion to Unsigned numbers not supported yet.");
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotSupportedException("Conversion to Unsigned numbers not supported yet.");
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToSingle();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToDouble();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotSupportedException("Conversion to Decimal not supported yet.");
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new InvalidCastException("Cannot cast NUMERIC to DateTime automatically.");
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
				return ToInt16();
			if (conversionType == typeof (int))
				return ToInt32();
			if (conversionType == typeof (long))
				return ToInt64();
			if (conversionType == typeof (float))
				return ToSingle();
			if (conversionType == typeof (double))
				return ToDouble();

			if (conversionType == typeof (byte[]))
				return ToByteArray();

			if (conversionType == typeof (string))
				return ToString();

			throw new InvalidCastException(String.Format("Cannot convert NUMERIC to {0}", conversionType));
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is NumericObject))
				throw new ArgumentException("Cannot compare the object to a NUMERIC");

			return CompareTo((NumericObject) obj);
		}

		public int CompareTo(NumericObject other) {
			if (Equals(this, other))
				return 0;

			// If this is a non-infinity number
			if (State == NumericState.None) {
				// If both values can be represented by a long value
				if (CanBeInt64 && other.CanBeInt64) {
					// Perform a long comparison check,
					if (valueAsLong > other.valueAsLong)
						return 1;
					if (valueAsLong < other.valueAsLong)
						return -1;
					return 0;
				}

				// And the compared number is non-infinity then use the BigDecimal
				// compareTo method.
				if (other.State == NumericState.None)
					return innerValue.CompareTo(other.innerValue);

				// Comparing a regular number with a NaN number.
				// If positive infinity or if NaN
				if (other.State == NumericState.PositiveInfinity ||
				    other.State == NumericState.NotANumber) {
					return -1;
				}
					// If negative infinity
				if (other.State == NumericState.NegativeInfinity)
					return 1;

				throw new ApplicationException("Unknown number state.");
			}

			// This number is a NaN number.
			// Are we comparing with a regular number?
			if (other.State == NumericState.None) {
				// Yes, negative infinity
				if (State == NumericState.NegativeInfinity)
					return -1;

				// positive infinity or NaN
				if (State == NumericState.PositiveInfinity ||
				    State == NumericState.NotANumber)
					return 1;

				throw new ApplicationException("Unknown number state.");
			}

			// Comparing NaN number with a NaN number.
			// This compares -Inf less than Inf and NaN and NaN greater than
			// Inf and -Inf.  -Inf < Inf < NaN
			return (State - other.State);
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

		public byte[] ToByteArray() {
			return State == NumericState.None
				? innerValue.MovePointRight(innerValue.Scale).ToBigInteger().ToByteArray()
				: new byte[0];
		}

		public override string ToString() {
			switch (State) {
				case (NumericState.None):
					return innerValue.ToString();
				case (NumericState.NegativeInfinity):
					return "-Infinity";
				case (NumericState.PositiveInfinity):
					return "Infinity";
				case (NumericState.NotANumber):
					return "NaN";
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public double ToDouble() {
			switch (State) {
				case (NumericState.None):
					return innerValue.ToDouble();
				case (NumericState.NegativeInfinity):
					return Double.NegativeInfinity;
				case (NumericState.PositiveInfinity):
					return Double.PositiveInfinity;
				case (NumericState.NotANumber):
					return Double.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public float ToSingle() {
			switch (State) {
				case (NumericState.None):
					return innerValue.ToSingle();
				case (NumericState.NegativeInfinity):
					return Single.NegativeInfinity;
				case (NumericState.PositiveInfinity):
					return Single.PositiveInfinity;
				case (NumericState.NotANumber):
					return Single.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public long ToInt64() {
			if (CanBeInt64)
				return valueAsLong;
			switch (State) {
				case (NumericState.None):
					return innerValue.ToInt64();
				default:
					return (long)ToDouble();
			}
		}

		public int ToInt32() {
			if (CanBeInt32)
				return (int)valueAsLong;
			switch (State) {
				case (NumericState.None):
					return innerValue.ToInt32();
				default:
					return (int)ToDouble();
			}
		}

		public short ToInt16() {
			if (!CanBeInt32)
				throw new InvalidCastException("The value of this numeric is over the maximum Int16.");

			var value = ToInt32();
			if (value > Int16.MaxValue ||
				value < Int16.MinValue)
				throw new InvalidCastException("The value of this numeric is out of range of a short integer.");

			return (short)value;
		}

		public byte ToByte() {
			if (!CanBeInt32)
				throw new InvalidCastException("The value of this numeric is over the maximum Byte.");

			var value = ToInt32();
			if (value > Byte.MaxValue ||
				value < Byte.MinValue)
				throw new InvalidCastException("The value of this numeric is out of range of a byte.");

			return (byte)value;
		}

		public bool ToBoolean() {
			if (Equals(One))
				return true;
			if (Equals(Zero))
				return false;

			throw new InvalidCastException("The value of this NUMERIC cannot be converted to a boolean.");
		}

		private NumericState InverseState() {
			if (State == NumericState.NegativeInfinity)
				return NumericState.PositiveInfinity;
			if (State == NumericState.PositiveInfinity)
				return NumericState.NegativeInfinity;
			return State;
		}

		public int Signum() {
			if (State == NumericState.None)
				return innerValue.Sign;
			if (State == NumericState.NegativeInfinity)
				return -1;
			return 1;
		}

		public NumericObject XOr(NumericObject value) {
			if (value == null)
				throw new ArgumentNullException("value");

			if (State == NumericState.NotANumber)
				return this;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = innerValue.ToBigInteger();
				return new NumericObject(Type, NumericState.None, new BigDecimal(bi1.Or(bi2)));
			}

			return null;
		}

		public NumericObject Add(NumericObject value) {
			if (value == null)
				throw new ArgumentNullException("value");

			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					var widerType = Type.Wider(value.Type);
					return new NumericObject(widerType, NumericState.None, innerValue.Add(value.innerValue));
				}

				return new NumericObject(value.Type, value.State, null);
			}

			return new NumericObject(Type, State, null);
		}

		public NumericObject Subtract(NumericObject value) {
			if (value == null)
				throw new ArgumentNullException("value");

			if (value == null)
				throw new ArgumentNullException("value");

			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					var widerType = Type.Wider(value.Type);
					return new NumericObject(widerType, NumericState.None, innerValue.Subtract(value.innerValue));
				}
				return new NumericObject(Type, value.InverseState(), null);
			}
			
			return new NumericObject(Type, State, null);
		}

		public NumericObject Multiply(NumericObject value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					var widerType = Type.Wider(value.Type);
					return new NumericObject(widerType, NumericState.None, innerValue.Multiply(value.innerValue));
				}

				return new NumericObject(value.Type, value.State, null);
			}

			return new NumericObject(Type, State, null);
		}

		public NumericObject Divide(NumericObject value) {
			if (value == null)
				throw new ArgumentNullException("value");

			if (State == NumericState.None) {
				if (value.State == 0) {
					BigDecimal divBy = value.innerValue;
					if (divBy.CompareTo (BigDecimal.Zero) != 0) {
						var widerType = Type.Wider(value.Type);
						return new NumericObject(widerType, NumericState.None, innerValue.Divide(divBy, 10, RoundingMode.HalfUp));
					}
				}
			}

			// Return NaN if we can't divide
			return new NumericObject(Type, NumericState.NotANumber, null);
		}

		public NumericObject Modulo(NumericObject value) {
			if (State == 0) {
				if (value.State == NumericState.None) {
					BigDecimal divBy = value.innerValue;
					if (divBy.CompareTo(BigDecimal.Zero) != 0) {
						var widerType = Type.Wider(value.Type);
						BigDecimal remainder = innerValue.Remainder(divBy);
						return new NumericObject(widerType, NumericState.None, remainder);
					}
				}
			}

			return new NumericObject(Type, NumericState.NotANumber, null);
		}

		public NumericObject Abs() {
			if (State == NumericState.None)
				return new NumericObject(Type, NumericState.None, innerValue.Abs());
			if (State == NumericState.NegativeInfinity)
				return new NumericObject(Type, NumericState.PositiveInfinity, null);
			return new NumericObject(Type, State, null);
		}

		public NumericObject SetScale(int scale, RoundingMode mode) {
			if (State == NumericState.None)
				return new NumericObject(Type, innerValue.SetScale(scale, mode));

			// Can't round -inf, +inf and NaN
			return this;
		}

		public NumericObject Negate() {
			if (State == NumericState.None)
				return new NumericObject(Type, innerValue.Negate());
			if (State == NumericState.NegativeInfinity ||
				State == NumericState.PositiveInfinity)
				return new NumericObject(Type, InverseState(), null);

			return this;
		}

		public NumericObject Plus() {
			if (State == NumericState.None)
				return new NumericObject(Type, innerValue.Plus());
			if (State == NumericState.NegativeInfinity ||
				State == NumericState.PositiveInfinity)
				return new NumericObject(Type, InverseState(), null);

			return this;
		}

		public NumericObject Sqrt() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Sqrt(innerValue));

			return this;
		}

		public NumericObject Root(int n) {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Root(n, innerValue));

			return this;
		}

		public NumericObject Sin() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Sin(innerValue));

			return this;
		}

		public NumericObject Cos() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Cos(innerValue));

			return this;
		}

		public NumericObject Cot() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Cot(innerValue));

			return this;
		}

		public NumericObject Tan() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Tan(innerValue));

			return this;
		}

		public NumericObject Pow(NumericObject exp) {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Pow(innerValue, exp.innerValue));

			return this;
		}

		public NumericObject Log2() {
			if (State == NumericState.None)
				return new NumericObject(Type, BigMath.Log(innerValue));

			return this;
		}

		public static bool TryParse(string s, out NumericObject value) {
			value = null;

			if (String.IsNullOrEmpty(s))
				return false;

			if (string.Equals(s, "+Infinity", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(s, "+Inf", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(s, "Infinity", StringComparison.OrdinalIgnoreCase)) {
				value = PositiveInfinity;
				return true;
			}

			if (String.Equals(s, "-Infinity", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(s, "-Inf", StringComparison.OrdinalIgnoreCase)) {
				value = NegativeInfinity;
				return true;
			}

			if (String.Equals(s, "NaN", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(s, "NotANumber", StringComparison.OrdinalIgnoreCase)) {
				value = NaN;
				return true;
			}

			BigDecimal decimalValue;

			try {
				decimalValue = new BigDecimal(s);
			} catch (Exception) {
				return false;
			}

			value = new NumericObject(PrimitiveTypes.Numeric(), NumericState.None, decimalValue);
			return true;
		}

		public static NumericObject Parse(string s) {
			NumericObject value;
			if (!TryParse(s, out value))
				throw new FormatException(String.Format("Cannot parse the string '{0}' to a valid Numeric object.", s));

			return value;
		}

		public static NumericObject Create(NumericState state, byte[] bytes, int scale, int precision) {
			if (state == NumericState.None) {
				// This inlines common numbers to save a bit of memory.
				if (scale == 0 && bytes.Length == 1) {
					if (bytes[0] == 0)
						return Zero;
					if (bytes[0] == 1)
						return One;
				}

				return new NumericObject(PrimitiveTypes.Numeric(), bytes, scale, precision);
			}

			if (state == NumericState.NegativeInfinity)
				return NegativeInfinity;
			if (state == NumericState.PositiveInfinity)
				return PositiveInfinity;
			if (state == NumericState.NotANumber)
				return NaN;

			throw new ApplicationException("Unknown number state.");
		}

		public static NumericObject operator +(NumericObject a, NumericObject b) {
			return a.Add(b);
		}

		public static NumericObject operator -(NumericObject a, NumericObject b) {
			return a.Subtract(b);
		}

		public static NumericObject operator *(NumericObject a, NumericObject b) {
			return a.Multiply(b);
		}

		public static NumericObject operator /(NumericObject a, NumericObject b) {
			return a.Divide(b);
		}

		public static NumericObject operator %(NumericObject a, NumericObject b) {
			return a.Modulo(b);
		}

		public static NumericObject operator |(NumericObject a, NumericObject b) {
			return a.XOr(b);
		}

		public static NumericObject operator -(NumericObject a) {
			return a.Negate();
		}

		public static NumericObject operator +(NumericObject a) {
			return a.Plus();
		}

		public static bool operator ==(NumericObject a, NumericObject b) {
			if ((object) a == null &&
			    (object) b == null)
				return true;
			if ((object) a == null)
				return false;
				
			return a.Equals(b);
		}

		public static bool operator !=(NumericObject a, NumericObject b) {
			return !(a == b);
		}

		public static bool operator >(NumericObject a, NumericObject b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator <(NumericObject a, NumericObject b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator >=(NumericObject a, NumericObject b) {
			var i = a.CompareTo(b);
			return i == 0 || i < 0;
		}

		public static bool operator <=(NumericObject a, NumericObject b) {
			var i = a.CompareTo(b);
			return i == 0 || i > 0;
		}

		public static implicit operator NumericObject(int i) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.Integer), NumericState.None, new BigDecimal(i));
		}

		public static implicit operator NumericObject(long i) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.BigInt), NumericState.None, new BigDecimal(i));
		}

		public static implicit operator NumericObject(short i) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.SmallInt), NumericState.None, new BigDecimal((int)i));
		}

		public static implicit operator NumericObject(byte i) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.TinyInt), NumericState.None, new BigDecimal((int)i));
		}

		public static implicit operator NumericObject(float f) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.Real), NumericState.None, new BigDecimal(f));
		}

		public static implicit operator NumericObject(double d) {
			return new NumericObject(PrimitiveTypes.Numeric(SqlTypeCode.Double), NumericState.None, new BigDecimal(d));
		}
	}
}