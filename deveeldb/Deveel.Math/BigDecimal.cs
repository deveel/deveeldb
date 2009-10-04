//  
//  BigDecimal.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

// This class was derived from the GNU Classpath's java.math.BigDecimal

using System;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Math {
	[Serializable]
	public sealed class BigDecimal : Number, IComparable {
		#region ctors
		public BigDecimal(BigInteger num) : this(num, 0) {
		}

		public BigDecimal(BigInteger num, int scale) {
			if (scale < 0)
				throw new FormatException("scale of " + scale + " is < 0");
			intVal = num;
			this.scale = scale;
		}

		public BigDecimal(double num) {
			if (Double.IsInfinity(num) || Double.IsNaN(num))
				throw new FormatException("invalid argument: " + num);
			// Note we can't convert NUM to a String and then use the
			// String-based constructor.  The BigDecimal documentation makes
			// it clear that the two constructors work differently.

			int mantissaBits = 52;
			int exponentBits = 11;
			long mantMask = (1L << mantissaBits) - 1;
			long expMask = (1L << exponentBits) - 1;

			long bits = Convert.ToInt64(num);
			long mantissa = bits & mantMask;
			long exponent = (ByteBuffer.URShift(bits, mantissaBits)) & expMask;
			bool denormal = exponent == 0;
			// Correct the exponent for the bias.
			exponent -= denormal ? 1022 : 1023;
			// Now correct the exponent to account for the bits to the right
			// of the decimal.
			exponent -= mantissaBits;
			// Ordinary numbers have an implied leading `1' bit.
			if (!denormal)
				mantissa |= (1L << mantissaBits);

			// Shave off factors of 10.
			while (exponent < 0 && (mantissa & 1) == 0) {
				++exponent;
				mantissa >>= 1;
			}

			intVal = BigInteger.ValueOf(bits < 0 ? -mantissa : mantissa);
			if (exponent < 0) {
				// We have MANTISSA * 2 ^ (EXPONENT).
				// Since (1/2)^N == 5^N * 10^-N we can easily convert this
				// into a power of 10.
				scale = (int)(-exponent);
				BigInteger mult = BigInteger.ValueOf(5).Pow(scale);
				intVal = intVal.Multiply(mult);
			} else {
				intVal = intVal.ShiftLeft((int)exponent);
				scale = 0;
			}
		}

		public BigDecimal(string num) {
			int len = num.Length;
			int start = 0, point = 0;
			int dot = -1;
			bool negative = false;
			if (num[0] == '+') {
				++start;
				++point;
			} else if (num[0] == '-') {
				++start;
				++point;
				negative = true;
			}

			while (point < len) {
				char c = num[point];
				if (c == '.') {
					if (dot >= 0)
						throw new FormatException("multiple `.'s in number");
					dot = point;
				} else if (c == 'e' || c == 'E')
					break;
				else if (!Char.IsDigit(c))
					throw new FormatException("unrecognized character: " + c);
				++point;
			}

			string val;
			if (dot >= 0) {
				val = num.Substring(start, dot - start) + num.Substring(dot + 1, point - (dot + 1));
				scale = point - 1 - dot;
			} else {
				val = num.Substring(start, point - start);
				scale = 0;
			}

			if (val.Length == 0)
				throw new FormatException("no digits seen");

			if (negative)
				val = "-" + val;
			intVal = new BigInteger(val);

			// Now parse exponent.
			if (point < len) {
				point++;
				if (num[point] == '+')
					point++;

				if (point >= len)
					throw new FormatException("no exponent following e or E");

				try {
					int exp = Int32.Parse(num.Substring(point));
					exp -= scale;
					if (Sign() == 0)
						scale = 0;
					else if (exp > 0) {
						intVal = intVal.Multiply(BigInteger.ValueOf(10).Pow(exp));
						scale = 0;
					} else
						scale = -exp;
				} catch (FormatException) {
					throw new FormatException("malformed exponent");
				}
			}
		}
		#endregion

		#region Fields
		private BigInteger intVal;
		internal int scale;

		private static readonly BigDecimal Zero = new BigDecimal(BigInteger.ValueOf(0), 0);
		private static readonly BigDecimal One = new BigDecimal(BigInteger.ValueOf(1), 0);
		#endregion

		#region Properties
		public int Scale {
			get { return scale; }
		}

		public BigInteger UnscaledValue {
			get { return intVal; }
		}
		#endregion

		#region Operators
		public static BigDecimal operator +(BigDecimal a, BigDecimal b) {
			return a.Add(b);
		}

		public static BigDecimal operator -(BigDecimal a, BigDecimal b) {
			return a.Subtract(b);
		}
		
		public static BigDecimal operator *(BigDecimal a, BigDecimal b) {
			return a.Multiply(b);
		}
		#endregion

		#region Public Methods
		public BigDecimal Add(BigDecimal value) {
			// For addition, need to line up decimals.  Note that the movePointRight
			// method cannot be used for this as it might return a BigDecimal with
			// scale == 0 instead of the scale we need.
			BigInteger op1 = intVal;
			BigInteger op2 = value.intVal;
			if (scale < value.scale)
				op1 = op1.Multiply(BigInteger.ValueOf(10).Pow(value.scale - scale));
			else if (scale > value.scale)
				op2 = op2.Multiply(BigInteger.ValueOf(10).Pow(scale - value.scale));

			return new BigDecimal(op1.Add(op2), System.Math.Max(scale, value.scale));
		}

		public BigDecimal Subtract(BigDecimal value) {
			return Add(value.Negate());
		}

		public BigDecimal Multiply(BigDecimal value) {
			return new BigDecimal(intVal.Multiply(value.intVal), scale + value.scale);
		}

		public BigDecimal Divide(BigDecimal value, DecimalRoundingMode roundingMode) {
			return Divide(value, scale, roundingMode);
		}

		public BigDecimal Divide(BigDecimal value, int newScale, DecimalRoundingMode roundingMode) {
			if (newScale < 0)
				throw new ArithmeticException("scale is negative: " + newScale);

			if (intVal.Sign() == 0)	// handle special case of 0.0/0.0
				return newScale == 0 ? Zero : new BigDecimal(Zero.intVal, newScale);

			// Ensure that pow gets a non-negative value.
			BigInteger valIntVal = value.intVal;
			int power = newScale - (scale - value.scale);
			if (power < 0) {
				// Effectively increase the scale of val to avoid an
				// ArithmeticException for a negative power.
				valIntVal = valIntVal.Multiply(BigInteger.ValueOf(10).Pow(-power));
				power = 0;
			}

			BigInteger dividend = intVal.Multiply(BigInteger.ValueOf(10).Pow(power));

			BigInteger[] parts = dividend.DivideAndRemainder(valIntVal);

			BigInteger unrounded = parts[0];
			if (parts[1].Sign() == 0) // no remainder, no rounding necessary
				return new BigDecimal(unrounded, newScale);

			if (roundingMode == DecimalRoundingMode.Unnecessary)
				throw new ArithmeticException("newScale is not large enough");

			int sign = intVal.Sign() * valIntVal.Sign();

			if (roundingMode == DecimalRoundingMode.Ceiling)
				roundingMode = (sign > 0) ? DecimalRoundingMode.Up : DecimalRoundingMode.Down;
			else if (roundingMode == DecimalRoundingMode.Floor)
				roundingMode = (sign < 0) ? DecimalRoundingMode.Up : DecimalRoundingMode.Down;
			else {
				// half is -1 if remainder*2 < positive intValue (*power), 0 if equal,
				// 1 if >. This implies that the remainder to round is less than,
				// equal to, or greater than half way to the next digit.
				BigInteger posRemainder = parts[1].Sign() < 0 ? parts[1].Negate() : parts[1];
				valIntVal = valIntVal.Sign() < 0 ? valIntVal.Negate() : valIntVal;
				int half = posRemainder.ShiftLeft(1).CompareTo(valIntVal);

				switch (roundingMode) {
					case DecimalRoundingMode.HalfUp:
						roundingMode = (half < 0) ? DecimalRoundingMode.Down : DecimalRoundingMode.Up;
						break;
					case DecimalRoundingMode.HalfDown:
						roundingMode = (half > 0) ? DecimalRoundingMode.Up : DecimalRoundingMode.Down;
						break;
					case DecimalRoundingMode.HalfEven:
						if (half < 0)
							roundingMode = DecimalRoundingMode.Down;
						else if (half > 0)
							roundingMode = DecimalRoundingMode.Up;
						else if (unrounded.TestBit(0)) // odd, then ROUND_HALF_UP
							roundingMode = DecimalRoundingMode.Up;
						else                           // even, ROUND_HALF_DOWN
							roundingMode = DecimalRoundingMode.Down;
						break;
				}
			}

			if (roundingMode == DecimalRoundingMode.Up)
				unrounded = unrounded.Add(BigInteger.ValueOf(sign > 0 ? 1 : -1));

			// roundingMode == ROUND_DOWN
			return new BigDecimal(unrounded, newScale);
		}

		public BigDecimal Remainder(BigDecimal value, int newScale) {
			if (newScale < 0)
				throw new ArithmeticException("scale is negative: " + newScale);

			if (intVal.Sign() == 0)	// handle special case of 0.0/0.0
				return newScale == 0 ? Zero : new BigDecimal(Zero.intVal, newScale);

			// Ensure that pow gets a non-negative value.
			BigInteger valIntVal = value.intVal;
			int power = newScale - (scale - value.scale);
			if (power < 0) {
				// Effectively increase the scale of val to avoid an
				// ArithmeticException for a negative power.
				valIntVal = valIntVal.Multiply(BigInteger.ValueOf(10).Pow(-power));
				power = 0;
			}

			BigInteger dividend = intVal.Multiply(BigInteger.ValueOf(10).Pow(power));

			BigInteger[] parts = dividend.DivideAndRemainder(valIntVal);
			return new BigDecimal(parts[1]);
		}

		public override int CompareTo(object obj) {
			if (!(obj is BigDecimal))
				throw new ArgumentException();
			return CompareTo((BigDecimal)obj);
		}

		public int CompareTo(BigDecimal value) {
			if (scale == value.scale)
				return intVal.CompareTo(value.intVal);

			BigInteger[] thisParts = intVal.DivideAndRemainder(BigInteger.ValueOf(10).Pow(scale));
			BigInteger[] valParts = value.intVal.DivideAndRemainder(BigInteger.ValueOf(10).Pow(value.scale));

			int compare;
			if ((compare = thisParts[0].CompareTo(valParts[0])) != 0)
				return compare;

			// quotients are the same, so compare remainders

			// remove trailing zeros
			if (thisParts[1].Equals(BigInteger.ValueOf(0)) == false)
				while (thisParts[1].Mod(BigInteger.ValueOf(10)).Equals(BigInteger.ValueOf(0)))
					thisParts[1] = thisParts[1].Divide(BigInteger.ValueOf(10));
			// again...
			if (valParts[1].Equals(BigInteger.ValueOf(0)) == false)
				while (valParts[1].Mod(BigInteger.ValueOf(10)).Equals(BigInteger.ValueOf(0)))
					valParts[1] = valParts[1].Divide(BigInteger.ValueOf(10));

			// and compare them
			return thisParts[1].CompareTo(valParts[1]);
		}

		public override bool Equals(object obj) {
			return (obj is BigDecimal
			        && scale == ((BigDecimal)obj).scale
			        && CompareTo((BigDecimal)obj) == 0);
		}

		public override int GetHashCode() {
			return ToInt32() ^ scale;
		}

		public BigDecimal Max(BigDecimal value) {
			if (CompareTo(value) == 1)
				return this;
			return value;
		}

		public BigDecimal Min(BigDecimal value) {
			if (CompareTo(value) == -1)
				return this;
			return value;
		}

		public int Sign() {
			return intVal.Sign();
		}

		public BigDecimal MovePointLeft(int n) {
			return (n < 0) ? MovePointRight(-n) : new BigDecimal(intVal, scale + n);
		}

		public BigDecimal MovePointRight(int n) {
			if (n < 0)
				return MovePointLeft(-n);

			if (scale >= n)
				return new BigDecimal(intVal, scale - n);

			return new BigDecimal(intVal.Multiply(BigInteger.ValueOf(10).Pow(n - scale)), 0);
		}

		public BigDecimal Abs() {
			return new BigDecimal(intVal.Abs(), scale);
		}

		public BigDecimal Negate() {
			return new BigDecimal(intVal.Negate(), scale);
		}

		public override string ToString() {
			string bigStr = intVal.ToString();
			if (scale == 0)
				return bigStr;

			bool negative = (bigStr[0] == '-');

			int point = bigStr.Length - scale - (negative ? 1 : 0);

			StringBuilder sb = new StringBuilder(bigStr.Length + 2 + (point <= 0 ? (-point + 1) : 0));
			if (point <= 0) {
				if (negative)
					sb.Append('-');
				sb.Append('0').Append('.');
				while (point < 0) {
					sb.Append('0');
					point++;
				}
				sb.Append(bigStr.Substring(negative ? 1 : 0));
			} else {
				sb.Append(bigStr);
				sb.Insert(point + (negative ? 1 : 0), '.');
			}
			return sb.ToString();
		}

		public BigInteger ToInteger() {
			return scale == 0 ? intVal : intVal.Divide(BigInteger.ValueOf(10).Pow(scale));
		}

		public override int ToInt32() {
			return ToInteger().ToInt32();
		}

		public override long ToInt64() {
			return ToInteger().ToInt64();
		}

		public override float ToSingle() {
			return ToInteger().ToSingle();
		}

		public override double ToDouble() {
			return ToInteger().ToDouble();
		}

		public BigDecimal SetScale(int scale) {
			return SetScale(scale, DecimalRoundingMode.Unnecessary);
		}

		public BigDecimal SetScale(int scale, DecimalRoundingMode roundingMode) {
			return Divide(One, scale, roundingMode);
		}
		#endregion

		#region Public Static Methods
		public static BigDecimal ValueOf(long value) {
			return ValueOf(value, 0);
		}

		public static BigDecimal ValueOf(long value, int scale) {
			if ((scale == 0) && ((int)value == value))
				switch ((int)value) {
					case 0:
						return Zero;
					case 1:
						return One;
				}

			return new BigDecimal(BigInteger.ValueOf(value), scale);
		}
		#endregion
	}
}