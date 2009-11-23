// 
//  BigNumber.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Globalization;
using System.Runtime.Serialization;

using Deveel.Data.Util;

namespace Deveel.Math {
	///<summary>
	/// Extends <see cref="BigDecimal"/> to allow a number to be positive 
	/// infinity, negative infinity and not-a-number.
	///</summary>
	/// <remarks>
	/// This provides compatibility with float and double types.
	/// </remarks>
	[Serializable]
	public sealed class BigNumber : Number {
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
		private BigNumber(NumberState number_state, BigDecimal big_decimal) {
			this.number_state = number_state;
			if (number_state == 0) {
				SetBigDecimal(big_decimal);
			}
		}

		private BigNumber(byte[] buf, int scale, NumberState state) {
			number_state = state;
			if (number_state == 0) {
				BigInteger bigint = new BigInteger(buf);
				SetBigDecimal(new BigDecimal(bigint, scale));
			}
		}

		// Only call this from a constructor!
		private void SetBigDecimal(BigDecimal big_decimal) {
			this.big_decimal = big_decimal;
			if (big_decimal.Scale == 0) {
				BigInteger bint = big_decimal.ToInteger();
				int bit_count = big_decimal.ToInteger().BitLength;
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
				if (number_state == 0) {
					return big_decimal.Scale;
				} else {
					return -1;
				}
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
				if (number_state == NumberState.NegativeInfinity) {
					return NumberState.PositiveInfinity;
				} else if (number_state == NumberState.PositiveInfinity) {
					return NumberState.NegativeInfinity;
				} else {
					return number_state;
				}
			}
		}

		///<summary>
		/// Returns this number as a byte array (unscaled).
		///</summary>
		///<returns></returns>
		public byte[] ToByteArray() {
			return number_state == 0 ? big_decimal.MovePointRight(big_decimal.Scale).ToInteger().ToByteArray() : new byte[0];
		}

		/// <inheritdoc/>
		public override string ToString() {
			switch (number_state) {
				case (0):
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

		/// <inheritdoc/>
		public override double ToDouble() {
			switch (number_state) {
				case (0):
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

		/// <inheritdoc/>
		public override float ToSingle() {
			switch (number_state) {
				case (0):
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

		/// <inheritdoc/>
		public override long ToInt64() {
			if (CanBeLong) {
				return long_representation;
			}
			switch (number_state) {
				case (0):
					return big_decimal.ToInt64();
				default:
					return (long)ToDouble();
			}
		}

		/// <inheritdoc/>
		public override int ToInt32() {
			if (CanBeLong) {
				return (int)long_representation;
			}
			switch (number_state) {
				case (0):
					return big_decimal.ToInt32();
				default:
					return (int)ToDouble();
			}
		}

		/// <inheritdoc/>
		public override short ToInt16() {
			return (short)ToInt32();
		}

		/// <inheritdoc/>
		public override byte ToByte() {
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
			if (number_state == 0) {
				return big_decimal;
			} else {
				throw new ArithmeticException("NaN, +Infinity or -Infinity can't be translated to a BigDecimal");
			}
		}

		/// <inheritdoc/>
		public override int CompareTo(object obj) {
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
			if (number_state == 0 && Scale == 0 &&
				number.number_state == 0 && number.Scale == 0) {
				BigInteger bi1 = big_decimal.ToInteger();
				BigInteger bi2 = number.big_decimal.ToInteger();
				return new BigNumber(NumberState.None, new BigDecimal(bi1.Or(bi2)));
			} else {
				return null;
			}
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Add (BigNumber number) {
			if (number_state == 0) {
				if (number.number_state == 0) {
					return new BigNumber(NumberState.None, big_decimal.Add(number.big_decimal));
				} else {
					return new BigNumber(number.number_state, null);
				}
			} else {
				return new BigNumber(number_state, null);
			}
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Subtract (BigNumber number) {
			if (number_state == 0) {
				if (number.number_state == 0) {
					return new BigNumber(NumberState.None, big_decimal.Subtract(number.big_decimal));
				} else {
					return new BigNumber(number.InverseState, null);
				}
			} else {
				return new BigNumber(number_state, null);
			}
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Multiply (BigNumber number) {
			if (number_state == 0) {
				if (number.number_state == 0) {
					return new BigNumber(NumberState.None, big_decimal.Multiply(number.big_decimal));
				} else {
					return new BigNumber(number.number_state, null);
				}
			} else {
				return new BigNumber(number_state, null);
			}
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
						return new BigNumber(NumberState.None, big_decimal.Divide(div_by, 10, DecimalRoundingMode.HalfUp));
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
						BigDecimal remainder;
						big_decimal.Divide(div_by, 10, DecimalRoundingMode.HalfUp, out remainder);
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
				return big_decimal.Sign();
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
		public BigNumber SetScale (int d, DecimalRoundingMode round_enum) {
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

		/**
		 * Creates a BigNumber from a string.
		 */

		public static BigNumber Parse (String str) {
			if (str.Equals ("Infinity")) {
				return PositiveInfinity;
			} else if (str.Equals ("-Infinity")) {
				return NegativeInfinity;
			} else if (str.Equals ("NaN")) {
				return NaN;
			} else {
				return new BigNumber(NumberState.None, new BigDecimal(str));
			}
		}

		/**
		 * Creates a BigNumber from the given data.
		 */

		public static BigNumber Create(byte[] buf, int scale, NumberState state) {
			if (state == 0) {
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
	}
}