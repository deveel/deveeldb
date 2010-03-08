// 
//  Copyright 2010  Deveel
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
using System.Globalization;

using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel.Data {
	///<summary>
	/// Extends <see cref="BigDecimal"/> to allow a number to be positive 
	/// infinity, negative infinity and not-a-number.
	///</summary>
	/// <remarks>
	/// This provides compatibility with float and double types.
	/// </remarks>
	[Serializable]
	public sealed class BigNumber : IConvertible {
		private static readonly BigDecimal BD_ZERO = new BigDecimal(0);

		///<summary>
		/// Represets a <see cref="BigNumber"/> for the 1 number.
		///</summary>
		public static readonly BigNumber One = 1L;

		///<summary>
		/// Represents a <see cref="BigNumber"/> for the 0 number.
		///</summary>
		public static readonly BigNumber Zero = 0L;

		// Statics for negative infinity, positive infinity and NaN.
		///<summary>
		///</summary>
		public static readonly BigNumber NegativeInfinity = new BigNumber(NumberState.NegativeInfinity, null);
		///<summary>
		///</summary>
		public static readonly BigNumber PositiveInfinity = new BigNumber(NumberState.PositiveInfinity, null);
		///<summary>
		///</summary>
		public static readonly BigNumber NaN = new BigNumber(NumberState.NotANumber, null);

		/// <summary>
		/// The BigDecimal representation.
		/// </summary>
		private BigDecimal big_decimal;

		/// <summary>
		/// If this can be represented as an int or long, this contains the number
		/// of bytes needed to represent the number.
		/// </summary>
		private byte byte_count = 120;
		/// <summary>
		/// A 'long' representation of this number.
		/// </summary>
		private long long_representation;
		private NumberState number_state;

		/// <summary>
		///  Constructs the number.
		/// </summary>
		/// <param name="number_state"></param>
		/// <param name="big_decimal"></param>
		internal BigNumber(NumberState number_state, BigDecimal big_decimal) {
			this.number_state = number_state;
			if (number_state == NumberState.None)
				SetBigDecimal(big_decimal);
		}

		private BigNumber(byte[] buf, int scale, NumberState state) {
			number_state = state;
			if (number_state == NumberState.None) {
				BigInteger bigint = new BigInteger(buf);
				SetBigDecimal(new BigDecimal(bigint, scale));
			}
		}

		// Only call this from a constructor!
		private void SetBigDecimal(BigDecimal value) {
			big_decimal = value;
			if (big_decimal.Scale == 0) {
				BigInteger bint = value.ToBigInteger();
				int bit_count = bint.BitLength;
				if (bit_count < 30) {
					long_representation = bint.ToInt64();
					byte_count = 4;
				} else if (bit_count < 60) {
					long_representation = bint.ToInt64();
					byte_count = 8;
				}
			}
		}

		///<summary>
		/// Returns true if this BigNumber can be represented by a 64-bit long (has
		/// no scale).
		///</summary>
		public bool CanBeLong {
			get { return byte_count <= 8; }
		}

		///<summary>
		/// Returns true if this BigNumber can be represented by a 32-bit int (has
		/// no scale).
		///</summary>
		public bool CanBeInt {
			get { return byte_count <= 4; }
		}

		///<summary>
		/// Returns the scale of this number, or -1 if the number has no scale (if
		/// it -inf, +inf or NaN).
		///</summary>
		public int Scale {
			get {
				if (number_state == 0)
					return big_decimal.Scale;
				return -1;
			}
		}

		///<summary>
		/// Returns the state of this number.
		///</summary>
		public NumberState State {
			get { return number_state; }
		}

		/// <summary>
		/// Returns the inverse of the state.
		/// </summary>
		private NumberState InverseState {
			get {
				if (number_state == NumberState.NegativeInfinity)
					return NumberState.PositiveInfinity;
				if (number_state == NumberState.PositiveInfinity)
					return NumberState.NegativeInfinity;
				return number_state;
			}
		}

		///<summary>
		/// Returns this number as a byte array (unscaled).
		///</summary>
		///<returns></returns>
		public byte[] ToByteArray() {
			return number_state == 0 ? big_decimal.MovePointRight(big_decimal.Scale).ToBigInteger().ToByteArray() : new byte[0];
		}

		/// <inheritdoc/>
		public override string ToString() {
			switch (number_state) {
				case (NumberState.None):
					return big_decimal.ToString();
				case (NumberState.NegativeInfinity):
					return "-Infinity";
				case (NumberState.PositiveInfinity):
					return "Infinity";
				case (NumberState.NotANumber):
					return "NaN";
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public double ToDouble() {
			switch (number_state) {
				case (NumberState.None):
					return big_decimal.ToDouble();
				case (NumberState.NegativeInfinity):
					return Double.NegativeInfinity;
				case (NumberState.PositiveInfinity):
					return Double.PositiveInfinity;
				case (NumberState.NotANumber):
					return Double.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public float ToSingle() {
			switch (number_state) {
				case (NumberState.None):
					return big_decimal.ToSingle();
				case (NumberState.NegativeInfinity):
					return Single.NegativeInfinity;
				case (NumberState.PositiveInfinity):
					return Single.PositiveInfinity;
				case (NumberState.NotANumber):
					return Single.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public long ToInt64() {
			if (CanBeLong)
				return long_representation;
			switch (number_state) {
				case (NumberState.None):
					return big_decimal.ToInt64();
				default:
					return (long)ToDouble();
			}
		}

		public int ToInt32() {
			if (CanBeLong)
				return (int)long_representation;
			switch (number_state) {
				case (NumberState.None):
					return big_decimal.ToInt32();
				default:
					return (int)ToDouble();
			}
		}

		public short ToInt16() {
			return (short)ToInt32();
		}

		public byte ToByte() {
			return (byte)ToInt32();
		}

		///<summary>
		/// Returns the big number as a <see cref="BigDecimal"/> object.
		///</summary>
		///<returns></returns>
		///<exception cref="ArithmeticException">
		/// If this number represents NaN, +Inf or -Inf.
		/// </exception>
		public BigDecimal ToBigDecimal() {
			if (number_state != NumberState.None)
				throw new ArithmeticException("NaN, +Infinity or -Infinity can't be translated to a BigDecimal");

			return big_decimal;
		}

		public int CompareTo(object obj) {
			return CompareTo((BigNumber)obj);
		}

		/**
		 * Compares this BigNumber with the given BigNumber.  Returns 0 if the values
		 * are equal, >0 if this is greater than the given value, and &lt; 0 if this
		 * is less than the given value.
		 */

		public int CompareTo(BigNumber number) {
			if (this == number) {
				return 0;
			}

			// If this is a non-infinity number
			if (number_state == 0) {
				// If both values can be represented by a long value
				if (CanBeLong && number.CanBeLong) {
					// Perform a long comparison check,
					if (long_representation > number.long_representation) {
						return 1;
					} else if (long_representation < number.long_representation) {
						return -1;
					} else {
						return 0;
					}
				}

				// And the compared number is non-infinity then use the BigDecimal
				// compareTo method.
				if (number.number_state == 0) {
					return big_decimal.CompareTo(number.big_decimal);
				} else {
					// Comparing a regular number with a NaN number.
					// If positive infinity or if NaN
					if (number.number_state == NumberState.PositiveInfinity ||
					    number.number_state == NumberState.NotANumber) {
						return -1;
					}
						// If negative infinity
					else if (number.number_state == NumberState.NegativeInfinity) {
						return 1;
					} else {
						throw new ApplicationException("Unknown number state.");
					}
				}
			} else {
				// This number is a NaN number.
				// Are we comparing with a regular number?
				if (number.number_state == 0) {
					// Yes, negative infinity
					if (number_state == NumberState.NegativeInfinity) {
						return -1;
					}
						// positive infinity or NaN
					else if (number_state == NumberState.PositiveInfinity ||
					         number_state == NumberState.NotANumber) {
						return 1;
					} else {
						throw new ApplicationException("Unknown number state.");
					}
				} else {
					// Comparing NaN number with a NaN number.
					// This compares -Inf less than Inf and NaN and NaN greater than
					// Inf and -Inf.  -Inf < Inf < NaN
					return (int)(number_state - number.number_state);
				}
			}
		}

		/**
		 * The equals comparison uses the BigDecimal 'equals' method to compare
		 * values.  This means that '0' is NOT equal to '0.0' and '10.0' is NOT equal
		 * to '10.00'.  Care should be taken when using this method.
		 */

		public override bool Equals(Object ob) {
			BigNumber bnum = (BigNumber)ob;
			if (number_state != 0) {
				return (number_state == bnum.number_state);
			} else {
				return big_decimal.Equals(bnum.big_decimal);
			}
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}


		// ---- Mathematical functions ----

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber BitWiseOr(BigNumber number) {
			if (number_state == NumberState.None && Scale == 0 &&
			    number.number_state == 0 && number.Scale == 0) {
				BigInteger bi1 = big_decimal.ToBigInteger();
				BigInteger bi2 = number.big_decimal.ToBigInteger();
				return new BigNumber(NumberState.None, new BigDecimal(bi1.Or(bi2)));
			}
			return null;
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Add (BigNumber number) {
			if (number_state == NumberState.None) {
				if (number.number_state == NumberState.None)
					return new BigNumber(NumberState.None, big_decimal.Add(number.big_decimal));
				return new BigNumber(number.number_state, null);
			}
			return new BigNumber(number_state, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Subtract (BigNumber number) {
			if (number_state == NumberState.None) {
				if (number.number_state == NumberState.None)
					return new BigNumber(NumberState.None, big_decimal.Subtract(number.big_decimal));
				return new BigNumber(number.InverseState, null);
			}
			
			return new BigNumber(number_state, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Multiply(BigNumber number) {
			if (number_state == NumberState.None) {
				if (number.number_state == 0)
					return new BigNumber(NumberState.None, big_decimal.Multiply(number.big_decimal));
				return new BigNumber(number.number_state, null);
			}
			return new BigNumber(number_state, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Divide (BigNumber number) {
			if (number_state == 0) {
				if (number.number_state == 0) {
					BigDecimal div_by = number.big_decimal;
					if (div_by.CompareTo (BD_ZERO) != 0) {
						return new BigNumber(NumberState.None, big_decimal.Divide(div_by, 10, RoundingMode.HalfUp));
					}
				}
			}
			// Return NaN if we can't divide
			return new BigNumber(NumberState.NotANumber, null);
		}

		public BigNumber Modulus(BigNumber number) {
			if (number_state == 0) {
				if (number.number_state == 0) {
					BigDecimal div_by = number.big_decimal;
					if (div_by.CompareTo(BD_ZERO) != 0) {
						BigDecimal remainder = big_decimal.Remainder(div_by);
						return new BigNumber(NumberState.None, remainder);
					}
				}
			}

			return new BigNumber(NumberState.NotANumber, null);
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public BigNumber Abs () {
			if (number_state == 0) {
				return new BigNumber(NumberState.None, big_decimal.Abs());
			} else if (number_state == NumberState.NegativeInfinity) {
				return new BigNumber(NumberState.PositiveInfinity, null);
			} else {
				return new BigNumber(number_state, null);
			}
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public int Signum() {
			if (number_state == 0) {
				return big_decimal.Signum();
			} else if (number_state == NumberState.NegativeInfinity) {
				return -1;
			} else {
				return 1;
			}
		}

		///<summary>
		///</summary>
		///<param name="d"></param>
		///<param name="round_enum"></param>
		///<returns></returns>
		public BigNumber SetScale (int d, RoundingMode round_enum) {
			if (number_state == 0) {
				return new BigNumber(NumberState.None, big_decimal.SetScale(d, round_enum));
			}
			// Can't round -inf, +inf and NaN
			return this;
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public BigNumber Sqrt() {
			return System.Math.Sqrt(ToDouble());
		}


		// ---------- Casting from types ----------

		/**
		 * Creates a BigNumber from a double.
		 */

		public static implicit operator BigNumber(double value) {
			if (value == Double.NegativeInfinity) {
				return NegativeInfinity;
			} else if (value == Double.PositiveInfinity) {
				return PositiveInfinity;
			} else if (value != value) {
				return NaN;
			}
			return new BigNumber(NumberState.None, new BigDecimal(Convert.ToString(value, CultureInfo.InvariantCulture)));
		}

		public static implicit operator  BigNumber(float value) {
			if (value == Single.NegativeInfinity) {
				return NegativeInfinity;
			} else if (value == Single.PositiveInfinity) {
				return PositiveInfinity;
			} else if (value != value) {
				return NaN;
			}
			return new BigNumber(NumberState.None, new BigDecimal(Convert.ToString(value, CultureInfo.InvariantCulture)));
		}

		public static implicit operator BigNumber(long value) {
			return new BigNumber(NumberState.None, BigDecimal.ValueOf(value));
		}

		public static implicit operator BigNumber(int value) {
			return new BigNumber(NumberState.None, BigDecimal.ValueOf(value));
		}

		public static implicit operator BigNumber(BigDecimal value) {
			return new BigNumber(NumberState.None, value);
		}

		//TODO:
		public static implicit operator BigNumber(decimal value) {
			int[] bits = Decimal.GetBits(value);
			byte[] buffer = new byte[(3 * 4) + 1];
			buffer[0] = (byte)(bits[0] == 0 ? 0 : 1);
			for (int i = bits.Length - 1; i >= 0; i--) {
				ByteBuffer.WriteInteger(bits[i], buffer, i + 4);
			}
			return new BigNumber(NumberState.None, new BigDecimal(new BigInteger(buffer)));
		}

		public static implicit operator Int16(BigNumber number) {
			return number.ToInt16();
		}

		public static implicit operator Int32(BigNumber number) {
			return number.ToInt32();
		}

		public static implicit operator Int64(BigNumber number) {
			return number.ToInt64();
		}

		public static implicit operator Single(BigNumber number) {
			return number.ToSingle();
		}

		public static implicit operator Double(BigNumber number) {
			return number.ToDouble();
		}

		/**
		 * Creates a BigNumber from a string.
		 */

		public static BigNumber Parse (String str) {
			if (str.Equals ("Infinity"))
				return PositiveInfinity;
			if (str.Equals ("-Infinity"))
				return NegativeInfinity;
			if (str.Equals ("NaN"))
				return NaN;
			
			return new BigNumber(NumberState.None, new BigDecimal(str));
		}

		/**
		 * Creates a BigNumber from the given data.
		 */

		public static BigNumber Create(byte[] buf, int scale, NumberState state) {
			if (state == NumberState.None) {
				// This inlines common numbers to save a bit of memory.
				if (scale == 0 && buf.Length == 1) {
					if (buf[0] == 0) {
						return Zero;
					} else if (buf[0] == 1) {
						return One;
					}
				}
				return new BigNumber(buf, scale, state);
			} else if (state == NumberState.NegativeInfinity) {
				return NegativeInfinity;
			} else if (state == NumberState.PositiveInfinity) {
				return PositiveInfinity;
			} else if (state == NumberState.NotANumber) {
				return NaN;
			} else {
				throw new ApplicationException("Unknown number state.");
			}
		}

		#region Implementation of IConvertible

		TypeCode IConvertible.GetTypeCode() {
			if (CanBeInt)
				return TypeCode.Int32;
			if (CanBeLong)
				return TypeCode.Int64;
			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			int value = ToInt16();
			if (value == 0)
				return false;
			if (value == 1)
				return true;
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			short value = ToInt16();
			if (value > Char.MaxValue || value < Char.MinValue)
				throw new InvalidCastException();
			return (char) value;
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToSingle();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToDouble();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(bool))
				return (this as IConvertible).ToBoolean(provider);
			if (conversionType == typeof(byte))
				return ToByte();
			if (conversionType == typeof(short))
				return ToInt16();
			if (conversionType == typeof(int))
				return ToInt32();
			if (conversionType == typeof(long))
				return ToInt64();
			if (conversionType == typeof(float))
				return ToSingle();
			if (conversionType == typeof(double))
				return ToDouble();

			if (conversionType == typeof(BigDecimal))
				return ToBigDecimal();
			if (conversionType == typeof(byte[]))
				return ToByteArray();

			throw new NotSupportedException();
		}

		#endregion
	}
}