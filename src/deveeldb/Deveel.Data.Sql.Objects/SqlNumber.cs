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

using Deveel.Math;

namespace Deveel.Data.Sql.Objects {
	public struct SqlNumber : ISqlObject, IComparable<SqlNumber>, IConvertible, IEquatable<SqlNumber> {
		private readonly BigDecimal innerValue;
		private readonly int byteCount;
		private readonly long valueAsLong;

		public static readonly SqlNumber Zero = new SqlNumber(NumericState.None, BigDecimal.Zero);
		public static readonly SqlNumber One = new SqlNumber(NumericState.None, BigDecimal.One);
		public static readonly SqlNumber Null = new SqlNumber(NumericState.None, null);

		public static readonly SqlNumber NaN = new SqlNumber(NumericState.NotANumber, null);

		public static readonly SqlNumber NegativeInfinity = new SqlNumber(NumericState.NegativeInfinity, null);

		public static readonly SqlNumber PositiveInfinity = new SqlNumber(NumericState.PositiveInfinity, null);

		private SqlNumber(BigDecimal value)
			: this(NumericState.None, value) {
		}

		private SqlNumber(NumericState state, BigDecimal value)
			: this() {
			valueAsLong = 0;
			byteCount = 120;

			if (value != null && value.Scale == 0) {
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

		public SqlNumber(byte[] bytes, int scale)
			: this(new BigDecimal(new BigInteger(bytes), scale)) {
		}

		public SqlNumber(byte[] bytes, int scale, int precision)
			: this(new BigDecimal(new BigInteger(bytes), scale, new MathContext(precision))) {
		}

		public SqlNumber(int value, int precision)
			: this(new BigDecimal(value, new MathContext(precision))) {
		}

		public SqlNumber(int value)
			: this(value, MathContext.Decimal32.Precision) {
		}

		public SqlNumber(long value, int precision)
			: this(new BigDecimal(value, new MathContext(precision))) {
		}

		public SqlNumber(long value)
			: this(value, MathContext.Decimal64.Precision) {
		}

		public SqlNumber(double value)
			: this(value, MathContext.Decimal128.Precision) {
		}

		public SqlNumber(double value, int precision)
			: this(new BigDecimal(value, new MathContext(precision))) {
		}

		public NumericState State { get; private set; }

		public bool CanBeInt64 {
			get { return byteCount <= 8; }
		}

		public bool CanBeInt32 {
			get { return byteCount <= 4; }
		}

		public int Scale {
			get { return State == NumericState.None ? innerValue.Scale : 0; }
		}

		public int Precision {
			get { return State == NumericState.None ? innerValue.Precision : 0; }
		}

		private MathContext MathContext {
			get { return State == NumericState.None ? new MathContext(Precision) : null; }
		}

		public int Sign {
			get { return State == NumericState.None ? innerValue.Sign : 0; }
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlNumber))
				throw new ArgumentException();

			return CompareTo((SqlNumber) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			if (!(other is SqlNumber))
				throw new ArgumentException();

			return CompareTo((SqlNumber) other);
		}

		public bool IsNull {
			get { return State == NumericState.None && innerValue == null; }
		}

		private NumericState InverseState() {
			if (State == NumericState.NegativeInfinity)
				return NumericState.PositiveInfinity;
			if (State == NumericState.PositiveInfinity)
				return NumericState.NegativeInfinity;
			return State;
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return other is SqlNumber;
		}

		public bool Equals(SqlNumber other) {
			if (State == NumericState.NegativeInfinity &&
			    other.State == NumericState.NegativeInfinity)
				return true;
			if (State == NumericState.PositiveInfinity &&
			    other.State == NumericState.PositiveInfinity)
				return true;
			if (State == NumericState.NotANumber &&
			    other.State == NumericState.NotANumber)
				return true;

			if (IsNull && other.IsNull)
				return true;
			if (IsNull && !other.IsNull)
				return false;
			if (!IsNull && other.IsNull)
				return false;

			return innerValue.CompareTo(other.innerValue) == 0;
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlNumber))
				return false;

			return Equals((SqlNumber) obj);
		}

		public override int GetHashCode() {
			return innerValue.GetHashCode() ^ State.GetHashCode();
		}

		public int CompareTo(SqlNumber other) {
			if (Equals(this, other))
				return 0;

			// If this is a non-infinity number
			if (State == NumericState.None) {
				// If both values can be represented by a long value
				if (CanBeInt64 && other.CanBeInt64) {
					// Perform a long comparison check,
					return valueAsLong.CompareTo(other.valueAsLong);
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

				throw new ArgumentException("Unknown number state.");
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

				throw new ArgumentException("Unknown number state.");
			}

			// Comparing NaN number with a NaN number.
			// This compares -Inf less than Inf and NaN and NaN greater than
			// Inf and -Inf.  -Inf < Inf < NaN
			return (State - other.State);
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

			if (conversionType == typeof (SqlBoolean))
				return new SqlBoolean(ToBoolean());
			if (conversionType == typeof (SqlBinary))
				return new SqlBinary(ToByteArray());
			if (conversionType == typeof (SqlString))
				return new SqlString(ToString());

			throw new InvalidCastException(System.String.Format("Cannot convert NUMERIC to {0}", conversionType));
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
					throw new InvalidCastException("Unknown number state");
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
					throw new InvalidCastException("Unknown number state");
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
					throw new InvalidCastException("Unknown number state");
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

		public SqlNumber XOr(SqlNumber value) {
			if (State == NumericState.NotANumber)
				return this;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1.XOr(bi2)));
			}

			return Null;
		}

		public SqlNumber And(SqlNumber value) {
			if (State == NumericState.NotANumber)
				return this;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1.And(bi2)));
			}

			return Null;			
		}

		public SqlNumber Or(SqlNumber value) {
			if (State == NumericState.NotANumber)
				return this;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1.Or(bi2)));
			}

			return Null;
		}

		public SqlNumber Add(SqlNumber value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					if (IsNull || value.IsNull)
						return Null;

					return new SqlNumber(NumericState.None, innerValue.Add(value.innerValue));
				}

				return new SqlNumber(value.State, null);
			}

			return new SqlNumber(State, null);
		}

		public SqlNumber Subtract(SqlNumber value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					if (IsNull || value.IsNull)
						return Null;

					return new SqlNumber(NumericState.None, innerValue.Subtract(value.innerValue));
				}
				return new SqlNumber(value.InverseState(), null);
			}
			
			return new SqlNumber(State, null);
		}

		public SqlNumber Multiply(SqlNumber value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					if (IsNull || value.IsNull)
						return Null;

					return new SqlNumber(NumericState.None, innerValue.Multiply(value.innerValue));
				}

				return new SqlNumber(value.State, null);
			}

			return new SqlNumber(State, null);
		}

		public SqlNumber Divide(SqlNumber value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					if (IsNull || value.IsNull)
						return Null;

					BigDecimal divBy = value.innerValue;
					if (divBy.CompareTo (BigDecimal.Zero) != 0) {
						return new SqlNumber(NumericState.None, innerValue.Divide(divBy, 10, RoundingMode.HalfUp));
					}
				}
			}

			// Return NaN if we can't divide
			return new SqlNumber(NumericState.NotANumber, null);
		}

		public SqlNumber Modulo(SqlNumber value) {
			if (State == NumericState.None) {
				if (value.State == NumericState.None) {
					if (IsNull || value.IsNull)
						return Null;

					BigDecimal divBy = value.innerValue;
					if (divBy.CompareTo(BigDecimal.Zero) != 0) {
						BigDecimal remainder = innerValue.Remainder(divBy);
						return new SqlNumber(NumericState.None, remainder);
					}
				}
			}

			return new SqlNumber(NumericState.NotANumber, null);
		}

		public SqlNumber Abs() {
			if (State == NumericState.None)
				return new SqlNumber(NumericState.None, innerValue.Abs());
			if (State == NumericState.NegativeInfinity)
				return new SqlNumber(NumericState.PositiveInfinity, null);
			return new SqlNumber(State, null);
		}

		public SqlNumber SetScale(int scale, RoundingMode mode) {
			if (State == NumericState.None)
				return new SqlNumber(innerValue.SetScale(scale, mode));

			// Can't round -inf, +inf and NaN
			return this;
		}

		public SqlNumber Negate() {
			if (State == NumericState.None)
				return new SqlNumber(innerValue.Negate());
			if (State == NumericState.NegativeInfinity ||
				State == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;
		}

		public SqlNumber Plus() {
			if (State == NumericState.None)
				return new SqlNumber(innerValue.Plus());
			if (State == NumericState.NegativeInfinity ||
				State == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;
		}

		public SqlNumber Not() {
			if (State == NumericState.None)
				return new SqlNumber(new BigDecimal(innerValue.ToBigInteger().Not()));
			if (State == NumericState.NegativeInfinity ||
				State == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;			
		}

		public SqlNumber Sqrt() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Sqrt(innerValue));

			return this;
		}

		public SqlNumber Root(int n) {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Root(n, innerValue));

			return this;
		}

		public SqlNumber Sin() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Sin(innerValue));

			return this;
		}

		public SqlNumber Cos() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Cos(innerValue));

			return this;
		}

		public SqlNumber Cot() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Cot(innerValue));

			return this;
		}

		public SqlNumber Tan() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Tan(innerValue));

			return this;
		}

		public SqlNumber Pow(SqlNumber exp) {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Pow(innerValue, exp.innerValue));

			return this;
		}

		public SqlNumber Log2() {
			if (State == NumericState.None)
				return new SqlNumber(BigMath.Log(innerValue));

			return this;
		}

		public SqlNumber Round() {
			return Round(MathContext.Precision);
		}

		public SqlNumber Round(int precision) {
			if (State == NumericState.None)
				return new SqlNumber(innerValue.Round(new MathContext(precision, RoundingMode.HalfUp)));

			return this;			
		}

		public static bool TryParse(string s, out SqlNumber value) {
			if (String.IsNullOrEmpty(s)) {
				value = Null;
				return false;
			}

			if (string.Equals(s, "+Infinity", StringComparison.OrdinalIgnoreCase) ||
			    string.Equals(s, "+Inf", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(s, "Infinity", StringComparison.OrdinalIgnoreCase)) {
				value = PositiveInfinity;
				return true;
			}

			if (string.Equals(s, "-Infinity", StringComparison.OrdinalIgnoreCase) ||
			    string.Equals(s, "-Inf", StringComparison.OrdinalIgnoreCase)) {
				value = NegativeInfinity;
				return true;
			}

			if (string.Equals(s, "NaN", StringComparison.OrdinalIgnoreCase) ||
			    string.Equals(s, "NotANumber", StringComparison.OrdinalIgnoreCase)) {
				value = NaN;
				return true;
			}

			BigDecimal decimalValue;

			if (!BigDecimal.TryParse(s, out decimalValue)) {
				value = Null;
				return false;
			}

			value = new SqlNumber(NumericState.None, decimalValue);
			return true;
		}

		public static SqlNumber Parse(string s) {
			SqlNumber value;
			if (!TryParse(s, out value))
				throw new FormatException(string.Format("Cannot parse the string '{0}' to a valid Numeric object.", s));

			return value;
		}

				public static SqlNumber operator +(SqlNumber a, SqlNumber b) {
			return a.Add(b);
		}

		public static SqlNumber operator -(SqlNumber a, SqlNumber b) {
			return a.Subtract(b);
		}

		public static SqlNumber operator *(SqlNumber a, SqlNumber b) {
			return a.Multiply(b);
		}

		public static SqlNumber operator /(SqlNumber a, SqlNumber b) {
			return a.Divide(b);
		}

		public static SqlNumber operator %(SqlNumber a, SqlNumber b) {
			return a.Modulo(b);
		}

		public static SqlNumber operator |(SqlNumber a, SqlNumber b) {
			return a.XOr(b);
		}

		public static SqlNumber operator -(SqlNumber a) {
			return a.Negate();
		}

		public static SqlNumber operator +(SqlNumber a) {
			return a.Plus();
		}

		public static bool operator ==(SqlNumber a, SqlNumber b) {				
			return a.Equals(b);
		}

		public static bool operator !=(SqlNumber a, SqlNumber b) {
			return !(a == b);
		}

		public static bool operator >(SqlNumber a, SqlNumber b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator <(SqlNumber a, SqlNumber b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator >=(SqlNumber a, SqlNumber b) {
			var i = a.CompareTo(b);
			return i == 0 || i > 0;
		}

		public static bool operator <=(SqlNumber a, SqlNumber b) {
			var i = a.CompareTo(b);
			return i == 0 || i < 0;
		}
	}
}