// 
//  Copyright 2010-2017 Deveel
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
using System.Globalization;

using Deveel.Data.Sql.Types;
using Deveel.Math;

namespace Deveel.Data.Sql {
	public struct SqlNumber : ISqlValue, IComparable<SqlNumber>, IEquatable<SqlNumber>, IConvertible, ISqlFormattable {
		internal readonly BigDecimal innerValue;
		private readonly int byteCount;
		internal readonly long valueAsLong;
		private readonly NumericState state;

		public static readonly SqlNumber Zero = new SqlNumber(NumericState.None, BigDecimal.Zero);
		public static readonly SqlNumber One = new SqlNumber(NumericState.None, BigDecimal.One);

		public static readonly SqlNumber NaN = new SqlNumber(NumericState.NotANumber, null);

		public static readonly SqlNumber NegativeInfinity = new SqlNumber(NumericState.NegativeInfinity, null);

		public static readonly SqlNumber PositiveInfinity = new SqlNumber(NumericState.PositiveInfinity, null);

		internal SqlNumber(BigDecimal value)
			: this(NumericState.None, value) {
		}

		internal SqlNumber(NumericState state, BigDecimal value)
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
			this.state = state;
		}

		public SqlNumber(byte[] bytes, int scale, int precision)
			: this(new BigDecimal(new BigInteger(bytes), scale, new MathContext(precision))) {
		}

		public SqlNumber(byte[] bytes)
			: this(GetUnscaledBytes(bytes), GetScale(bytes), GetPrecision(bytes)) {
		}

		internal SqlNumber(BigInteger unscaled, int scale, int precision)
			: this(new BigDecimal(unscaled, scale, new MathContext(precision))) {
		}

		public bool CanBeInt64 => byteCount <= 8;

		public bool CanBeInt32 => byteCount <= 4;

		public int Scale => state == NumericState.None ? innerValue.Scale : 0;

		public int Precision => state == NumericState.None ? innerValue.Precision : 0;

		internal MathContext MathContext => state == NumericState.None ? new MathContext(Precision) : null;

		public int Sign => state == NumericState.None ? innerValue.Sign : 0;

		private static NumericState GetNumberState(double value) {
			if (Double.IsPositiveInfinity(value))
				return NumericState.PositiveInfinity;
			if (Double.IsNegativeInfinity(value))
				return NumericState.NegativeInfinity;
			if (Double.IsNaN(value))
				return NumericState.NotANumber;
			if (Double.IsInfinity(value))
				throw new NotSupportedException();

			return NumericState.None;
		}

		private static byte[] GetUnscaledBytes(byte[] bytes) {
			var result = new byte[bytes.Length - 8];
			Array.Copy(bytes, 8, result, 0, bytes.Length - 8);
			return result;
		}

		private static int GetPrecision(byte[] bytes) {
			return BitConverter.ToInt32(bytes, 0);
		}

		private static int GetScale(byte[] bytes) {
			return BitConverter.ToInt32(bytes, 4);
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlNumber))
				throw new ArgumentException();

			return CompareTo((SqlNumber) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			if (!(other is SqlNumber))
				throw new ArgumentException();

			return CompareTo((SqlNumber) other);
		}

		internal NumericState InverseState() {
			if (state == NumericState.NegativeInfinity)
				return NumericState.PositiveInfinity;
			if (state == NumericState.PositiveInfinity)
				return NumericState.NegativeInfinity;
			return state;
		}

		public static bool IsNaN(SqlNumber number) {
			return number.state == NumericState.NotANumber;
		}

		public static bool IsPositiveInfinity(SqlNumber number) {
			return number.state == NumericState.PositiveInfinity;
		}

		public static bool IsNegativeInfinity(SqlNumber number) {
			return number.state == NumericState.NegativeInfinity;
		}

		public static bool IsNumber(SqlNumber number) {
			return number.state == NumericState.None;
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other is SqlNumber;
		}

		public bool Equals(SqlNumber other) {
			if (state != other.state)
				return false;

			if (state != NumericState.None)
				return true;

			return innerValue.CompareTo(other.innerValue) == 0;
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlNumber))
				return false;

			return Equals((SqlNumber) obj);
		}

		public override int GetHashCode() {
			return innerValue.GetHashCode() ^ state.GetHashCode();
		}

		public int CompareTo(SqlNumber other) {
			if (Equals(this, other))
				return 0;

			// If this is a non-infinity number
			if (state == NumericState.None) {
				// If both values can be represented by a long value
				if (CanBeInt64 && other.CanBeInt64) {
					// Perform a long comparison check,
					return valueAsLong.CompareTo(other.valueAsLong);
				}

				// And the compared number is non-infinity then use the BigDecimal
				// compareTo method.
				if (other.state == NumericState.None)
					return  innerValue.CompareTo(other.innerValue);

				// Comparing a regular number with a NaN number.
				// If positive infinity or if NaN
				if (other.state == NumericState.PositiveInfinity ||
				    other.state == NumericState.NotANumber) {
					return -1;
				}
					// If negative infinity
				if (other.state == NumericState.NegativeInfinity)
					return 1;

				throw new ArgumentException("Unknown number state.");
			}

			// This number is a NaN number.
			// Are we comparing with a regular number?
			if (other.state == NumericState.None) {
				// Yes, negative infinity
				if (state == NumericState.NegativeInfinity)
					return -1;

				// positive infinity or NaN
				if (state == NumericState.PositiveInfinity ||
				    state == NumericState.NotANumber)
					return 1;

				throw new ArgumentException("Unknown number state.");
			}

			// Comparing NaN number with a NaN number.
			// This compares -Inf less than Inf and NaN and NaN greater than
			// Inf and -Inf.  -Inf < Inf < NaN
			var c = (state - other.state);
			if (c == 0)
				return 0;
			if (c < 0)
				return -1;

			return 1;
		}

		TypeCode IConvertible.GetTypeCode() {
			if (Scale == 0) {
				if (Precision <= SqlNumericType.TinyIntPrecision)
					return TypeCode.Byte;
				if (Precision <= SqlNumericType.SmallIntPrecision)
					return TypeCode.Int16;
				if (Precision <= SqlNumericType.IntegerPrecision)
					return TypeCode.Int32;
				if (Precision <= SqlNumericType.BigIntPrecision)
					return TypeCode.Int64;
			} else {
				if (Precision <= SqlNumericType.FloatPrecision)
					return TypeCode.Single;
				if (Precision <= SqlNumericType.DoublePrecision)
					return TypeCode.Double;
			}

			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException("Conversion of NUMERIC to Char is invalid.");
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new InvalidCastException("Conversion to Signed Byte numbers not supported yet.");
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new InvalidCastException("Conversion to Unsigned numbers not supported yet.");
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new InvalidCastException("Conversion to Unsigned numbers not supported yet.");
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
			throw new InvalidCastException("Conversion to Decimal not supported yet.");
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new InvalidCastException("Cannot cast NUMERIC to DateTime automatically.");
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof (byte[]))
				return ToByteArray();

			if (conversionType == typeof (SqlBoolean))
				return new SqlBoolean(ToBoolean());
			if (conversionType == typeof (SqlBinary))
				return new SqlBinary(ToByteArray());
			if (conversionType == typeof (SqlString))
				return new SqlString(ToString());

			throw new InvalidCastException($"Cannot convert NUMERIC to {conversionType}");
		}

		public byte[] ToUnscaledByteArray() {
			if (state != NumericState.None)
				return new byte[0];

			return innerValue.UnscaledValue.ToByteArray();
		}


		public SqlNumber Precise(int scale, int precision) {
			return new SqlNumber(innerValue, scale, precision);
		}

		public byte[] ToByteArray() {
			if (state != NumericState.None)
				return new byte[0];

			var unscaled = innerValue.UnscaledValue.ToByteArray();
			var precision = BitConverter.GetBytes(Precision);
			var scale = BitConverter.GetBytes(Scale);

			var result = new byte[unscaled.Length + 4 + 4];
			Array.Copy(precision, 0, result, 0, 4);
			Array.Copy(scale, 0, result, 4, 4);
			Array.Copy(unscaled, 0, result, 8, unscaled.Length);

			return result;
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			switch (state) {
				case (NumericState.None): {
					if (CanBeInt32 || CanBeInt64) {
						builder.Append(valueAsLong);
					} else {
						var norm = BigMath.StripTrailingZeros(innerValue);
						builder.Append(norm.ToString());
					}
					break;
				}
				case (NumericState.NegativeInfinity):
					builder.Append("-Infinity");
					break;
				case (NumericState.PositiveInfinity):
					builder.Append("+Infinity");
					break;
				case (NumericState.NotANumber):
					builder.Append("NaN");
					break;
				default:
					throw new InvalidCastException("Unknown number state");
			}
		}

		private double ToDouble() {
			switch (state) {
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

		private float ToSingle() {
			switch (state) {
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

		private long ToInt64() {
			if (CanBeInt64)
				return valueAsLong;
			switch (state) {
				case (NumericState.None):
					return innerValue.ToInt64();
				default:
					return (long)ToDouble();
			}
		}

		private int ToInt32() {
			if (CanBeInt32)
				return (int)valueAsLong;
			switch (state) {
				case (NumericState.None):
					return innerValue.ToInt32();
				default:
					return (int)ToDouble();
			}
		}

		private short ToInt16() {
			if (!CanBeInt32)
				throw new InvalidCastException("The value of this numeric is over the maximum Int16.");

			var value = ToInt32();
			if (value > Int16.MaxValue ||
				value < Int16.MinValue)
				throw new InvalidCastException("The value of this numeric is out of range of a short integer.");

			return (short)value;
		}

		private byte ToByte() {
			if (!CanBeInt32)
				throw new InvalidCastException("The value of this numeric is over the maximum Byte.");

			var value = ToInt32();
			if (value > Byte.MaxValue ||
				value < Byte.MinValue)
				throw new InvalidCastException("The value of this numeric is out of range of a byte.");

			return (byte)value;
		}

		private bool ToBoolean() {
			if (Equals(One))
				return true;
			if (Equals(Zero))
				return false;

			throw new InvalidCastException("The value of this NUMERIC cannot be converted to a boolean.");
		}

		private SqlNumber XOr(SqlNumber value) {
			if (state != NumericState.None)
				return this;
			if (value.state != NumericState.None)
				return value;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1 ^ bi2));
			}

			return this;
		}

		private SqlNumber And(SqlNumber value) {
			if (state != NumericState.None)
				return this;
			if (value.state != NumericState.None)
				return value;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1 & bi2));
			}

			return this;
		}

		private SqlNumber Or(SqlNumber value) {
			if (state != NumericState.None)
				return this;
			if (value.state != NumericState.None)
				return value;

			if (Scale == 0 && value.Scale == 0) {
				BigInteger bi1 = innerValue.ToBigInteger();
				BigInteger bi2 = value.innerValue.ToBigInteger();
				return new SqlNumber(NumericState.None, new BigDecimal(bi1 | bi2));
			}

			return this;
		}

		private SqlNumber Negate() {
			if (state == NumericState.None) {
				return new SqlNumber(-innerValue);
			}

			if (state == NumericState.NegativeInfinity ||
				state == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;
		}

		private SqlNumber Plus() {
			if (state == NumericState.None) {
				return new SqlNumber(+innerValue);
			}

			if (state == NumericState.NegativeInfinity ||
				state == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;
		}

		private SqlNumber Not() {
			if (state == NumericState.None) {
				return new SqlNumber(new BigDecimal(~innerValue.ToBigInteger()));
			}

			if (state == NumericState.NegativeInfinity ||
				state == NumericState.PositiveInfinity)
				return new SqlNumber(InverseState(), null);

			return this;			
		}

		public static bool TryParse(string s, out SqlNumber value) {
			return TryParse(s, CultureInfo.InvariantCulture, out value);
		}

		public static bool TryParse(string s, IFormatProvider provider, out SqlNumber value) {
			if (String.IsNullOrEmpty(s)) {
				value = new SqlNumber();
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

			if (!BigDecimal.TryParse(s, provider, out decimalValue)) {
				value = new SqlNumber();
				return false;
			}

			value = new SqlNumber(NumericState.None, decimalValue);
			return true;
		}

		public static SqlNumber Parse(string s) {
			return Parse(s, CultureInfo.InvariantCulture);
		}

		public static SqlNumber Parse(string s, IFormatProvider formatProvider) {
			SqlNumber value;
			if (!TryParse(s, formatProvider, out value))
				throw new FormatException($"Cannot parse the string '{s}' to a valid Numeric object.");

			return value;
		}

		public static SqlNumber operator +(SqlNumber a, SqlNumber b) {
			return SqlMath.Add(a, b);
		}

		public static SqlNumber operator -(SqlNumber a, SqlNumber b) {
			return SqlMath.Subtract(a, b);
		}

		public static SqlNumber operator *(SqlNumber a, SqlNumber b) {
			return SqlMath.Multiply(a, b);
		}

		public static SqlNumber operator /(SqlNumber a, SqlNumber b) {
			return SqlMath.Divide(a, b);
		}

		public static SqlNumber operator %(SqlNumber a, SqlNumber b) {
			return SqlMath.Remainder(a, b);
		}

		public static SqlNumber operator |(SqlNumber a, SqlNumber b) {
			return a.Or(b);
		}

		public static SqlNumber operator &(SqlNumber a, SqlNumber b) {
			return a.And(b);
		}

		public static SqlNumber operator ^(SqlNumber a, SqlNumber b) {
			return a.XOr(b);
		}

		public static SqlNumber operator -(SqlNumber a) {
			return a.Negate();
		}

		public static SqlNumber operator +(SqlNumber a) {
			return a.Plus();
		}

		public static SqlNumber operator ~(SqlNumber a) {
			return a.Not();
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
			return a.CompareTo(b) >= 0;
		}

		public static bool operator <=(SqlNumber a, SqlNumber b) {
			return a.CompareTo(b) <= 0;
		}

		#region Explicit Operators

		public static explicit operator int(SqlNumber number) {
			return number.ToInt32();
		}

		public static explicit operator byte(SqlNumber number) {
			return number.ToByte();
		}

		public static explicit operator short(SqlNumber number) {
			return number.ToInt16();
		}

		public static explicit operator long(SqlNumber number) {
			return number.ToInt64();
		}

		public static explicit operator double(SqlNumber number) {
			return number.ToDouble();
		}

		public static explicit operator float(SqlNumber number) {
			return number.ToSingle();
		}

		public static explicit operator SqlNumber(byte value) {
			return new SqlNumber(value, 0, SqlNumericType.TinyIntPrecision);
		}

		public static explicit operator SqlNumber(short value) {
			return new SqlNumber(value, 0, SqlNumericType.SmallIntPrecision);
		}

		public static explicit operator SqlNumber(double value) {
			return FromDouble(value);
		}

		public static explicit operator SqlNumber(float value) {
			return new SqlNumber(new BigDecimal(value, MathContext.Decimal32));
		}

		public static explicit operator SqlNumber(int value) {
			return new SqlNumber(value, 0, SqlNumericType.IntegerPrecision);
		}

		public static explicit operator SqlNumber(long value) {
			return new SqlNumber(value, 0, SqlNumericType.BigIntPrecision);
		}

		#endregion

		private static SqlNumber FromDouble(double value) {
			var state = GetNumberState(value);
			if (state == NumericState.None) {
				return new SqlNumber(new BigDecimal(value, new MathContext(SqlNumericType.DoublePrecision)));
			}

			return new SqlNumber(state, null);
		}

		#region NumericState

		internal enum NumericState {
			None = 0,
			NegativeInfinity = 1,
			PositiveInfinity = 2,
			NotANumber = 3
		}

		#endregion
	}
}