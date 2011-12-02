// 
//  Copyright 2009  Deveel
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
using System.Text;

namespace Deveel.Math {
	/// <summary>
	/// Immutable objects describing settings such as rounding 
	/// mode and digit precision for the numerical operations 
	/// provided by class <see cref="BigDecimal"/>.
	/// </summary>
	[Serializable]
	public sealed class MathContext {

		/**
		 * A {@code MathContext} which corresponds to the IEEE 754r quadruple
		 * decimal precision format: 34 digit precision and
		 * {@link RoundingMode#HALF_EVEN} rounding.
		 */
		public static readonly MathContext Decimal128 = new MathContext(34, RoundingMode.HalfEven);

		/**
		 * A {@code MathContext} which corresponds to the IEEE 754r single decimal
		 * precision format: 7 digit precision and {@link RoundingMode#HALF_EVEN}
		 * rounding.
		 */
		public static readonly MathContext Decimal32 = new MathContext(7, RoundingMode.HalfEven);

		/**
		 * A {@code MathContext} which corresponds to the IEEE 754r double decimal
		 * precision format: 16 digit precision and {@link RoundingMode#HALF_EVEN}
		 * rounding.
		 */
		public static readonly MathContext Decimal64 = new MathContext(16, RoundingMode.HalfEven);

		/**
		 * A {@code MathContext} for unlimited precision with
		 * {@link RoundingMode#HALF_UP} rounding.
		 */
		public static readonly MathContext Unlimited = new MathContext(0, RoundingMode.HalfUp);

		/**
		 * The number of digits to be used for an operation; results are rounded to
		 * this precision.
		 */
		private readonly int precision;

		/**
		 * A {@code RoundingMode} object which specifies the algorithm to be used
		 * for rounding.
		 */
		private readonly RoundingMode roundingMode;

		/**
		 * An array of {@code char} containing: {@code
		 * 'p','r','e','c','i','s','i','o','n','='}. It's used to improve the
		 * methods related to {@code String} conversion.
		 *
		 * @see #MathContext(String)
		 * @see #ToString()
		 */
		private static readonly char[] chPrecision = {'p', 'r', 'e', 'c', 'i', 's', 'i', 'o', 'n', '='};

		/**
		 * An array of {@code char} containing: {@code
		 * 'r','o','u','n','d','i','n','g','M','o','d','e','='}. It's used to
		 * improve the methods related to {@code String} conversion.
		 *
		 * @see #MathContext(String)
		 * @see #ToString()
		 */
		private static readonly char[] chRoundingMode = {'r', 'o', 'u', 'n', 'd', 'i', 'n', 'g', 'M', 'o', 'd', 'e', '='};

		/**
		 * Constructs a new {@code MathContext} with the specified precision and
		 * with the rounding mode {@link RoundingMode#HALF_UP HALF_UP}. If the
		 * precision passed is zero, then this implies that the computations have to
		 * be performed exact, the rounding mode in this case is irrelevant.
		 *
		 * @param precision
		 *            the precision for the new {@code MathContext}.
		 * @throws IllegalArgumentException
		 *             if {@code precision < 0}.
		 */
		public MathContext(int precision)
			: this(precision, RoundingMode.HalfUp) {
		}

		/**
		 * Constructs a new {@code MathContext} with the specified precision and
		 * with the specified rounding mode. If the precision passed is zero, then
		 * this implies that the computations have to be performed exact, the
		 * rounding mode in this case is irrelevant.
		 *
		 * @param precision
		 *            the precision for the new {@code MathContext}.
		 * @param roundingMode
		 *            the rounding mode for the new {@code MathContext}.
		 * @throws IllegalArgumentException
		 *             if {@code precision < 0}.
		 * @throws NullPointerException
		 *             if {@code roundingMode} is {@code null}.
		 */
		public MathContext(int precision, RoundingMode roundingMode) {
			if (precision < 0) {
				// math.0C=Digits < 0
				throw new ArgumentException(SR.GetString("math.0C")); //$NON-NLS-1$
			}
			this.precision = precision;
			this.roundingMode = roundingMode;
		}

		/**
		 * Constructs a new {@code MathContext} from a string. The string has to
		 * specify the precision and the rounding mode to be used and has to follow
		 * the following syntax: "precision=&lt;precision&gt; roundingMode=&lt;roundingMode&gt;"
		 * This is the same form as the one returned by the {@link #ToString}
		 * method.
		 *
		 * @param val
		 *            a string describing the precision and rounding mode for the
		 *            new {@code MathContext}.
		 * @throws IllegalArgumentException
		 *             if the string is not in the correct format or if the
		 *             precision specified is < 0.
		 */
		public MathContext(String val) {
			char[] charVal = val.ToCharArray();
			int i; // Index of charVal
			int j; // Index of chRoundingMode
			int digit; // It will contain the digit parsed

			if ((charVal.Length < 27) || (charVal.Length > 45)) {
				// math.0E=bad string format
				throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
			}
			// Parsing "precision=" String
			for (i = 0; (i < chPrecision.Length) && (charVal[i] == chPrecision[i]); i++) {
				;
			}

			if (i < chPrecision.Length) {
				// math.0E=bad string format
				throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
			}
			// Parsing the value for "precision="...
            digit = CharHelper.toDigit(charVal[i], 10);
			if (digit == -1) {
				// math.0E=bad string format
				throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
			}
			this.precision = this.precision * 10 + digit;
			i++;

			do {
                digit = CharHelper.toDigit(charVal[i], 10);
				if (digit == -1) {
					if (charVal[i] == ' ') {
						// It parsed all the digits
						i++;
						break;
					}
					// It isn't  a valid digit, and isn't a white space
					// math.0E=bad string format
					throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
				}
				// Accumulating the value parsed
				this.precision = this.precision * 10 + digit;
				if (this.precision < 0) {
					// math.0E=bad string format
					throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
				}
				i++;
			} while (true);
			// Parsing "roundingMode="
			for (j = 0; (j < chRoundingMode.Length)
					&& (charVal[i] == chRoundingMode[j]); i++, j++) {
				;
			}

			if (j < chRoundingMode.Length) {
				// math.0E=bad string format
				throw new ArgumentException(SR.GetString("math.0E")); //$NON-NLS-1$
			}
			// Parsing the value for "roundingMode"...
			this.roundingMode = (RoundingMode)Enum.Parse(typeof(RoundingMode), new string(charVal, i, charVal.Length - i), true);
		}

		/* Public Methods */

		/**
		 * Returns the precision. The precision is the number of digits used for an
		 * operation. Results are rounded to this precision. The precision is
		 * guaranteed to be non negative. If the precision is zero, then the
		 * computations have to be performed exact, results are not rounded in this
		 * case.
		 *
		 * @return the precision.
		 */

		public int Precision {
			get { return precision; }
		}

		/**
		 * Returns the rounding mode. The rounding mode is the strategy to be used
		 * to round results.
		 * <p>
		 * The rounding mode is one of
		 * {@link RoundingMode#UP},
		 * {@link RoundingMode#DOWN},
		 * {@link RoundingMode#CEILING},
		 * {@link RoundingMode#FLOOR},
		 * {@link RoundingMode#HALF_UP},
		 * {@link RoundingMode#HALF_DOWN},
		 * {@link RoundingMode#HALF_EVEN}, or
		 * {@link RoundingMode#UNNECESSARY}.
		 *
		 * @return the rounding mode.
		 */
		public RoundingMode RoundingMode {
			get { return roundingMode; }
		}

		/**
		 * Returns true if x is a {@code MathContext} with the same precision
		 * setting and the same rounding mode as this {@code MathContext} instance.
		 *
		 * @param x
		 *            object to be compared.
		 * @return {@code true} if this {@code MathContext} instance is equal to the
		 *         {@code x} argument; {@code false} otherwise.
		 */
		public override bool Equals(Object x) {
			return ((x is MathContext) &&
			        (((MathContext) x).Precision == precision) &&
			        (((MathContext) x).RoundingMode == roundingMode));
		}

		/**
		 * Returns the hash code for this {@code MathContext} instance.
		 *
		 * @return the hash code for this {@code MathContext}.
		 */
		public override int GetHashCode() {
			// Make place for the necessary bits to represent 8 rounding modes
			return ((precision << 3) | (int)roundingMode);
		}

		/**
		 * Returns the string representation for this {@code MathContext} instance.
		 * The string has the form
		 * {@code
		 * "precision=&lt;precision&gt; roundingMode=&lt;roundingMode&gt;"
		 * } where {@code &lt;precision&gt;} is an integer describing the number
		 * of digits used for operations and {@code &lt;roundingMode&gt;} is the
		 * string representation of the rounding mode.
		 *
		 * @return a string representation for this {@code MathContext} instance
		 */
		public override string ToString() {
			StringBuilder sb = new StringBuilder(45);

			sb.Append(chPrecision);
			sb.Append(precision);
			sb.Append(' ');
			sb.Append(chRoundingMode);
			sb.Append(roundingMode);
			return sb.ToString();
		}

		/**
		 * Makes checks upon deserialization of a {@code MathContext} instance.
		 * Checks whether {@code precision >= 0} and {@code roundingMode != null}
		 *
		 * @throws StreamCorruptedException
		 *             if {@code precision < 0}
		 * @throws StreamCorruptedException
		 *             if {@code roundingMode == null}
		 */
		/*
		private void readObject(ObjectInputStream s) throws IOException,
				ClassNotFoundException {
			s.defaultReadObject();
			if (precision < 0) {
				// math.0F=bad precision value
				throw new StreamCorruptedException(SR.getString("math.0F")); //$NON-NLS-1$
			}
			if (roundingMode == null) {
				// math.10=null roundingMode
				throw new StreamCorruptedException(SR.getString("math.10")); //$NON-NLS-1$
			}
		}
		*/
	}
}