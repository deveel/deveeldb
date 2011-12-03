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
using System.Runtime.Serialization;

namespace Deveel.Math {
	// since the ToString() method is overriden by this class and it changes the state of the object causing Heisenbugs
	// for debuggability we add the attribute DebuggerDisplay that points to a method that doesn't change it
	/// <summary>
	/// This class represents immutable arbitrary precision decimal numbers.
	/// </summary>
	/// <remarks>
	/// Each <see cref="BigDecimal"/> instance is represented with a unscaled 
	/// arbitrary precision mantissa (the unscaled value) and a scale. The value 
	/// of the <see cref="BigDecimal"/> is <see cref="UnscaledValue"/> 10^(-<see cref="Scale"/>).
	/// </remarks>
	[Serializable, System.Diagnostics.DebuggerDisplay("{ToStringInternal()}")]
	public class BigDecimal : IComparable<BigDecimal>, IConvertible, ISerializable {

		/// <summary>
		/// The constant zero as a <see cref="BigDecimal"/>.
		/// </summary>
		public static readonly BigDecimal Zero = new BigDecimal(0, 0);

		/// <summary>
		/// The constant one as a <see cref="BigDecimal"/>.
		/// </summary>
		public static readonly BigDecimal One = new BigDecimal(1, 0);

		/// <summary>
		/// The constant ten as a <see cref="BigDecimal"/>.
		/// </summary>
		public static readonly BigDecimal Ten = new BigDecimal(10, 0);

		/// <summary>
		/// The double closer to <c>Log10(2)</c>.
		/// </summary>
		private const double Log10_2 = 0.3010299956639812;

		/// <summary>
		/// The <see cref="string"/> representation is cached.
		/// </summary>
		[NonSerialized]
		private string toStringImage;

		/// <summary>
		/// Cache for the hash code.
		/// </summary>
		[NonSerialized]
		private int hashCode;

		/// <summary>
		/// An array with powers of five that fit in the type <see cref="long"/>
		/// (<c>5^0,5^1,...,5^27</c>).
		/// </summary>
		private static readonly BigInteger[] FivePow;

		/// <summary>
		/// An array with powers of ten that fit in the type <see cref="long"/> 
		/// (<c>10^0,10^1,...,10^18</c>).
		/// </summary>
		private static readonly BigInteger[] TenPow;

		/// <summary>
		/// An array with powers of ten that fit in the type <see cref="long"/> 
		/// (<c>10^0,10^1,...,10^18</c>).
		/// </summary>
		private static readonly long[] LongTenPow = new long[] {
		                                                         	1L,
		                                                         	10L,
		                                                         	100L,
		                                                         	1000L,
		                                                         	10000L,
		                                                         	100000L,
		                                                         	1000000L,
		                                                         	10000000L,
		                                                         	100000000L,
		                                                         	1000000000L,
		                                                         	10000000000L,
		                                                         	100000000000L,
		                                                         	1000000000000L,
		                                                         	10000000000000L,
		                                                         	100000000000000L,
		                                                         	1000000000000000L,
		                                                         	10000000000000000L,
		                                                         	100000000000000000L,
		                                                         	1000000000000000000L,
		                                                         };


		private static readonly long[] LongFivePow = new long[] {
		                                                          	1L,
		                                                          	5L,
		                                                          	25L,
		                                                          	125L,
		                                                          	625L,
		                                                          	3125L,
		                                                          	15625L,
		                                                          	78125L,
		                                                          	390625L,
		                                                          	1953125L,
		                                                          	9765625L,
		                                                          	48828125L,
		                                                          	244140625L,
		                                                          	1220703125L,
		                                                          	6103515625L,
		                                                          	30517578125L,
		                                                          	152587890625L,
		                                                          	762939453125L,
		                                                          	3814697265625L,
		                                                          	19073486328125L,
		                                                          	95367431640625L,
		                                                          	476837158203125L,
		                                                          	2384185791015625L,
		                                                          	11920928955078125L,
		                                                          	59604644775390625L,
		                                                          	298023223876953125L,
		                                                          	1490116119384765625L,
		                                                          	7450580596923828125L,
		                                                          };

		private static readonly int[] LongFivePowBitLength = new int[LongFivePow.Length];
		private static readonly int[] LongTenPowBitLength = new int[LongTenPow.Length];

		private const int BiScaledByZeroLength = 11;

		/// <summary>
		/// An array with the first <see cref="BigInteger"/> scaled by zero.
		/// (<c>[0,0],[1,0],...,[10,0]</c>).
		/// </summary>
		private static readonly BigDecimal[] BiScaledByZero = new BigDecimal[BiScaledByZeroLength];

		/// <summary>
		/// An array with the zero number scaled by the first positive scales.
		/// (<c>0*10^0, 0*10^1, ..., 0*10^10</c>).
		/// </summary>
		private static readonly BigDecimal[] ZeroScaledBy = new BigDecimal[11];

		/// <summary>
		/// An array filled with character <c>'0'</c>.
		/// </summary>
		private static readonly char[] ChZeros = new char[100];

		static BigDecimal() {
			// To fill all static arrays.
			int i = 0;

			for (; i < ZeroScaledBy.Length; i++) {
				BiScaledByZero[i] = new BigDecimal(i, 0);
				ZeroScaledBy[i] = new BigDecimal(0, i);
				ChZeros[i] = '0';
			}

			for (; i < ChZeros.Length; i++) {
				ChZeros[i] = '0';
			}
			for (int j = 0; j < LongFivePowBitLength.Length; j++) {
				LongFivePowBitLength[j] = BitLength(LongFivePow[j]);
			}
			for (int j = 0; j < LongTenPowBitLength.Length; j++) {
				LongTenPowBitLength[j] = BitLength(LongTenPow[j]);
			}

			// Taking the references of useful powers.
			TenPow = Multiplication.bigTenPows;
			FivePow = Multiplication.bigFivePows;
		}

		/// <summary>
		/// The arbitrary precision integer (unscaled value) in the internal
		/// representation of <see cref="BigDecimal"/>.
		/// </summary>
		private BigInteger intVal;

		[NonSerialized]
		private int _bitLength;

		[NonSerialized]
		private long smallValue;

		/// <summary>
		/// The 32-bit integer scale in the internal representation 
		/// of <see cref="BigDecimal"/>.
		/// </summary>
		private int _scale;

		/// <summary>
		/// Represent the number of decimal digits in the unscaled value.
		/// </summary>
		/// <remarks>
		/// This precision is calculated the first time, and used in the following 
		/// calls of method <see cref="Precision"/>. Note that some call to the private 
		/// method <see cref="InplaceRound"/> could update this field.
		/// </remarks>
		/// <seealso cref="Precision"/>
		/// <seealso cref="InplaceRound"/>
		[NonSerialized]
		private int _precision = 0;

		#region .ctor

		private BigDecimal(long smallValue, int scale) {
			this.smallValue = smallValue;
			_scale = scale;
			_bitLength = BitLength(smallValue);
		}

		private BigDecimal(int smallValue, int scale) {
			this.smallValue = smallValue;
			_scale = scale;
			_bitLength = BitLength(smallValue);
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a string 
		/// representation given as a <see cref="char">character</see> array.
		/// </summary>
		/// <param name="inData">Array of <see cref="char"/> containing the string 
		/// representation of this <see cref="BigDecimal"/>.</param>
		/// <param name="offset">The first index to be copied.</param>
		/// <param name="len">The number of <see cref="char"/> to be used.</param>
		/// <exception cref="ArgumentNullException">
		/// If the parameter <paramref name="inData"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="FormatException">
		/// If <paramref name="offset"/> is less than 0 or <paramref name="len"/> is
		/// less or equal to 0 or if <see cref="offset"/> + (<paramref name="len"/>-1) is 
		/// less than 0 or if <paramref name="offset"/> + (<paramref name="len"/>-1) is
		/// greater or equal to the length of <paramref name="inData"/>.
		/// <para>
		/// It is also thrown if <paramref name="inData"/> does not contain a valid string 
		/// representation of a big decimal.
		/// </para>
		/// </exception>
		public BigDecimal(char[] inData, int offset, int len) {
			int begin = offset; // first index to be copied
			int last = offset + (len - 1); // last index to be copied

			if (inData == null)
				throw new ArgumentNullException("inData");

			if ((last >= inData.Length) || (offset < 0) || (len <= 0) || (last < 0))
				throw new FormatException();

			StringBuilder unscaledBuffer = new StringBuilder(len);
			int bufLength = 0;
			// To skip a possible '+' symbol
			if ((offset <= last) && (inData[offset] == '+')) {
				offset++;
				begin++;
			}
			int counter = 0;
			bool wasNonZero = false;
			// Accumulating all digits until a possible decimal point
			for (; (offset <= last) && (inData[offset] != '.')
			&& (inData[offset] != 'e') && (inData[offset] != 'E'); offset++) {
				if (!wasNonZero) {
					if (inData[offset] == '0') {
						counter++;
					} else {
						wasNonZero = true;
					}
				}

			}
			unscaledBuffer.Append(inData, begin, offset - begin);
			bufLength += offset - begin;
			// A decimal point was found
			if ((offset <= last) && (inData[offset] == '.')) {
				offset++;
				// Accumulating all digits until a possible exponent
				begin = offset;
				for (; (offset <= last) && (inData[offset] != 'e') && (inData[offset] != 'E'); offset++) {
					if (!wasNonZero) {
						if (inData[offset] == '0') {
							counter++;
						} else {
							wasNonZero = true;
						}
					}
				}
				_scale = offset - begin;
				bufLength += _scale;
				unscaledBuffer.Append(inData, begin, _scale);
			} else {
				_scale = 0;
			}
			// An exponent was found
			if ((offset <= last) && ((inData[offset] == 'e') || (inData[offset] == 'E'))) {
				offset++;
				// Checking for a possible sign of scale
				begin = offset;
				if ((offset <= last) && (inData[offset] == '+')) {
					offset++;
					if ((offset <= last) && (inData[offset] != '-')) {
						begin++;
					}
				}
				// Accumulating all remaining digits
				String scaleString = new String(inData, begin, last + 1 - begin); // buffer for scale
				// Checking if the scale is defined            
				long newScale = (long)_scale - Int32.Parse(scaleString); // the new scale
				_scale = (int)newScale;
				if (newScale != _scale) {
					// math.02=Scale out of range.
					throw new FormatException(SR.GetString("math.02")); //$NON-NLS-1$
				}
			}
			// Parsing the unscaled value
			if (bufLength < 19) {
				smallValue = Int64.Parse(unscaledBuffer.ToString());
				_bitLength = BitLength(smallValue);
			} else {
				SetUnscaledValue(new BigInteger(unscaledBuffer.ToString()));
			}
			_precision = unscaledBuffer.Length - counter;
			if (unscaledBuffer[0] == '-') {
				_precision--;
			}
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a string 
		/// representation given as a <see cref="char">character</see> array.
		/// </summary>
		/// <param name="inData">Array of <see cref="char"/> containing the string 
		/// representation of this <see cref="BigDecimal"/>.</param>
		/// <param name="offset">The first index to be copied.</param>
		/// <param name="len">The number of <see cref="char"/> to be used.</param>
		/// <param name="mc">The rounding mode and precision for the result of 
		/// this operation.</param>
		/// <exception cref="ArgumentNullException">
		/// If the parameter <paramref name="inData"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="FormatException">
		/// If <paramref name="offset"/> is less than 0 or <paramref name="len"/> is
		/// less or equal to 0 or if <see cref="offset"/> + (<paramref name="len"/>-1) is 
		/// less than 0 or if <paramref name="offset"/> + (<paramref name="len"/>-1) is
		/// greater or equal to the length of <paramref name="inData"/>.
		/// <para>
		/// It is also thrown if <paramref name="inData"/> does not contain a valid string 
		/// representation of a big decimal.
		/// </para>
		/// </exception>
		/// <exception cref="ArithmeticException">
		/// if <see cref="MathContext.Precision"/> of <paramref name="mc"/> is greater than 0
		/// and <see cref="MathContext.RoundingMode"/> is equal to <see cref="RoundingMode.Unnecessary"/>
		/// and the new big decimal cannot be represented within the given precision without rounding.
		/// </exception>
		public BigDecimal(char[] inData, int offset, int len, MathContext mc)
			: this(inData, offset, len) {
			InplaceRound(mc);
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a string 
		/// representation given as a <see cref="char">character</see> array.
		/// </summary>
		/// <param name="inData">Array of <see cref="char"/> containing the string 
		/// representation of this <see cref="BigDecimal"/>.</param>
		/// <exception cref="ArgumentNullException">
		/// If the parameter <paramref name="inData"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="FormatException">
		/// If <paramref name="inData"/> does not contain a valid string representation 
		/// of a big decimal.
		/// </exception>
		public BigDecimal(char[] inData)
			: this(inData, 0, inData.Length) {
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a string 
		/// representation given as a <see cref="char">character</see> array.
		/// The result is rounded according to the specified math context.
		/// </summary>
		/// <param name="inData">Array of <see cref="char"/> containing the string 
		/// representation of this <see cref="BigDecimal"/>.</param>
		/// <param name="mc">The rounding mode and precision for the result of 
		/// this operation.</param>
		/// <exception cref="FormatException">
		/// If <paramref name="inData"/> does not contain a valid string representation 
		/// of a big decimal.
		/// </exception>
		/// <exception cref="ArithmeticException">
		/// if <see cref="MathContext.Precision"/> of <paramref name="mc"/> is greater than 0
		/// and <see cref="MathContext.RoundingMode"/> is equal to <see cref="RoundingMode.Unnecessary"/>
		/// and the new big decimal cannot be represented within the given precision without rounding.
		/// </exception>
		public BigDecimal(char[] inData, MathContext mc)
			: this(inData, 0, inData.Length) {
			InplaceRound(mc);
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a 
		/// string representation.
		/// </summary>
		/// <param name="value">The string containing the representation of this 
		/// <see cref="BigDecimal"/>.</param>
		/// <exception cref="FormatException">
		/// If <paramref name="value"/> does not contain a valid string representation 
		/// of a big decimal.
		/// </exception>
		public BigDecimal(string value)
			: this(value.ToCharArray(), 0, value.Length) {
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from a 
		/// string representation. The result is rounded according to the 
		/// specified math context.
		/// </summary>
		/// <param name="value">The string containing the representation of this 
		/// <see cref="BigDecimal"/>.</param>
		/// <param name="mc">The rounding mode and precision for the result of 
		/// this operation.</param>
		/// <exception cref="FormatException">
		/// If <paramref name="value"/> does not contain a valid string representation 
		/// of a big decimal.
		/// </exception>
		/// <exception cref="ArithmeticException">
		/// if <see cref="MathContext.Precision"/> of <paramref name="mc"/> is greater than 0
		/// and <see cref="MathContext.RoundingMode"/> is equal to <see cref="RoundingMode.Unnecessary"/>
		/// and the new big decimal cannot be represented within the given precision without rounding.
		/// </exception>
		public BigDecimal(string value, MathContext mc)
			: this(value.ToCharArray(), 0, value.Length) {
			InplaceRound(mc);
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from the 64bit 
		/// double <paramref name="val"/>. The constructed big decimal is 
		/// equivalent to the given double.
		/// </summary>
		/// <param name="val">The double value to be converted to a 
		/// <see cref="BigDecimal"/> instance.</param>
		/// <remarks>
		/// For example, <c>new BigDecimal(0.1)</c> is equal to <c>0.1000000000000000055511151231257827021181583404541015625</c>. 
		/// This happens as <c>0.1</c> cannot be represented exactly in binary.
		/// <para>
		/// To generate a big decimal instance which is equivalent to <c>0.1</c> use the
		/// <see cref="BigDecimal(string)"/> constructor.
		/// </para>
		/// </remarks>
		/// <exception cref="FormatException">
		/// If <paramref name="val"/> is infinity or not a number.
		/// </exception>
		public BigDecimal(double val) {
			if (Double.IsInfinity(val) || Double.IsNaN(val)) {
				// math.03=Infinity or NaN
				throw new FormatException(SR.GetString("math03")); //$NON-NLS-1$
			}
			long bits = BitConverter.DoubleToInt64Bits(val); // IEEE-754

			System.Diagnostics.Debug.Assert(bits == doubleToLongBits(val));
			long mantisa;
			int trailingZeros;
			// Extracting the exponent, note that the bias is 1023
			_scale = 1075 - (int)((bits >> 52) & 0x7FFL);
			// Extracting the 52 bits of the mantisa.
			mantisa = (_scale == 1075) ? (bits & 0xFFFFFFFFFFFFFL) << 1 : (bits & 0xFFFFFFFFFFFFFL) | 0x10000000000000L;
			if (mantisa == 0) {
				_scale = 0;
				_precision = 1;
			}
			// To simplify all factors '2' in the mantisa 
			if (_scale > 0) {
				trailingZeros = System.Math.Min(_scale, Utils.numberOfTrailingZeros(mantisa));
				long mantisa2 = (long)(((ulong)mantisa) >> trailingZeros);
				mantisa = Utils.URShift(mantisa, trailingZeros);
				_scale -= trailingZeros;
			}
			// Calculating the new unscaled value and the new scale
			if ((bits >> 63) != 0) {
				mantisa = -mantisa;
			}
			int mantisaBits = BitLength(mantisa);
			if (_scale < 0) {
				_bitLength = mantisaBits == 0 ? 0 : mantisaBits - _scale;
				if (_bitLength < 64) {
					smallValue = mantisa << (-_scale);
				} else {
					intVal = BigInteger.ValueOf(mantisa).ShiftLeft(-_scale);
				}
				_scale = 0;
			} else if (_scale > 0) {
				// m * 2^e =  (m * 5^(-e)) * 10^e
				if (_scale < LongFivePow.Length && mantisaBits + LongFivePowBitLength[_scale] < 64) {
					smallValue = mantisa * LongFivePow[_scale];
					_bitLength = BitLength(smallValue);
				} else {
					SetUnscaledValue(Multiplication.multiplyByFivePow(BigInteger.ValueOf(mantisa), _scale));
				}
			} else { // scale == 0
				smallValue = mantisa;
				_bitLength = mantisaBits;
			}
		}

		/// <summary>
		/// Constructs a new <see cref="BigDecimal"/> instance from the 64bit 
		/// double <paramref name="val"/>. The constructed big decimal is 
		/// equivalent to the given double.
		/// </summary>
		/// <param name="val">The double value to be converted to a 
		/// <see cref="BigDecimal"/> instance.</param>
		/// <param name="mc">The rounding mode and precision for the result of 
		/// this operation.</param>
		/// <remarks>
		/// For example, <c>new BigDecimal(0.1)</c> is equal to <c>0.1000000000000000055511151231257827021181583404541015625</c>. 
		/// This happens as <c>0.1</c> cannot be represented exactly in binary.
		/// <para>
		/// To generate a big decimal instance which is equivalent to <c>0.1</c> use the
		/// <see cref="BigDecimal(string)"/> constructor.
		/// </para>
		/// </remarks>
		/// <exception cref="FormatException">
		/// If <paramref name="val"/> is infinity or not a number.
		/// </exception>
		/// <exception cref="ArithmeticException">
		/// if <see cref="MathContext.Precision"/> of <paramref name="mc"/> is greater than 0
		/// and <see cref="MathContext.RoundingMode"/> is equal to <see cref="RoundingMode.Unnecessary"/>
		/// and the new big decimal cannot be represented within the given precision without rounding.
		/// </exception>
		public BigDecimal(double val, MathContext mc)
			: this(val) {
			InplaceRound(mc);
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given big integer
		 * {@code val}. The scale of the result is {@code 0}.
		 *
		 * @param val
		 *            {@code BigInteger} value to be converted to a {@code
		 *            BigDecimal} instance.
		 */
		public BigDecimal(BigInteger val)
			: this(val, 0) {
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given big integer
		 * {@code val}. The scale of the result is {@code 0}.
		 *
		 * @param val
		 *            {@code BigInteger} value to be converted to a {@code
		 *            BigDecimal} instance.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @throws ArithmeticException
		 *             if {@code mc.precision > 0} and {@code mc.roundingMode ==
		 *             UNNECESSARY} and the new big decimal cannot be represented
		 *             within the given precision without rounding.
		 */
		public BigDecimal(BigInteger val, MathContext mc)
			: this(val) {
			InplaceRound(mc);
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from a given unscaled value
		 * {@code unscaledVal} and a given scale. The value of this instance is
		 * {@code unscaledVal} 10^(-{@code scale}).
		 *
		 * @param unscaledVal
		 *            {@code BigInteger} representing the unscaled value of this
		 *            {@code BigDecimal} instance.
		 * @param scale
		 *            scale of this {@code BigDecimal} instance.
		 * @throws NullPointerException
		 *             if {@code unscaledVal == null}.
		 */
		public BigDecimal(BigInteger unscaledVal, int scale) {
			if (unscaledVal == null) {
				throw new NullReferenceException();
			}
			_scale = scale;
			SetUnscaledValue(unscaledVal);
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from a given unscaled value
		 * {@code unscaledVal} and a given scale. The value of this instance is
		 * {@code unscaledVal} 10^(-{@code scale}). The result is rounded according
		 * to the specified math context.
		 *
		 * @param unscaledVal
		 *            {@code BigInteger} representing the unscaled value of this
		 *            {@code BigDecimal} instance.
		 * @param scale
		 *            scale of this {@code BigDecimal} instance.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @throws ArithmeticException
		 *             if {@code mc.precision > 0} and {@code mc.roundingMode ==
		 *             UNNECESSARY} and the new big decimal cannot be represented
		 *             within the given precision without rounding.
		 * @throws NullPointerException
		 *             if {@code unscaledVal == null}.
		 */
		public BigDecimal(BigInteger unscaledVal, int scale, MathContext mc)
			: this(unscaledVal, scale) {
			InplaceRound(mc);
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given int
		 * {@code val}. The scale of the result is 0.
		 *
		 * @param val
		 *            int value to be converted to a {@code BigDecimal} instance.
		 */
		public BigDecimal(int val)
			: this(val, 0) {
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given int {@code
		 * val}. The scale of the result is {@code 0}. The result is rounded
		 * according to the specified math context.
		 *
		 * @param val
		 *            int value to be converted to a {@code BigDecimal} instance.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @throws ArithmeticException
		 *             if {@code mc.precision > 0} and {@code c.roundingMode ==
		 *             UNNECESSARY} and the new big decimal cannot be represented
		 *             within the given precision without rounding.
		 */
		public BigDecimal(int val, MathContext mc)
			: this(val, 0) {
			InplaceRound(mc);
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given long {@code
		 * val}. The scale of the result is {@code 0}.
		 *
		 * @param val
		 *            long value to be converted to a {@code BigDecimal} instance.
		 */
		public BigDecimal(long val)
			: this(val, 0) {
		}

		/**
		 * Constructs a new {@code BigDecimal} instance from the given long {@code
		 * val}. The scale of the result is {@code 0}. The result is rounded
		 * according to the specified math context.
		 *
		 * @param val
		 *            long value to be converted to a {@code BigDecimal} instance.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @throws ArithmeticException
		 *             if {@code mc.precision > 0} and {@code mc.roundingMode ==
		 *             UNNECESSARY} and the new big decimal cannot be represented
		 *             within the given precision without rounding.
		 */
		public BigDecimal(long val, MathContext mc)
			: this(val) {
			InplaceRound(mc);
		}

		#endregion

		internal static bool __isNegativeZero(double d) {
			return ((1.0 / d) == double.NegativeInfinity);
		}

		private static bool isNaN(double f) {
			return (f != f);
		}

		private static long doubleToLongBits(double d) {
			long num;
			long num4;
			if (isNaN(d)) {
				return 0x7ff8000000000000L;
			}
			if (d == double.PositiveInfinity) {
				return 0x7ff0000000000000L;
			}
			if (d == double.NegativeInfinity) {
				return -4503599627370496L;
			}
			if (d == 0.0) {
				if (__isNegativeZero(d)) {
					return -9223372036854775808L;
				}
				return 0L;
			}
			if (d > 0.0) {
				num = 0L;
			} else {
				num = -9223372036854775808L;
				d = -d;
			}
			double num2 = d;
			long num3 = 0L;
			while (d >= 2.0) {
				d /= 2.0;
				num3 += 1L;
			}
			while (d < 1.0) {
				d *= 2.0;
				num3 += -1L;
			}
			num3 += 0x3ffL;
			if (num3 < 0L) {
				num3 = 0L;
			}
			num3 = num3 << 0x34;
			if (num2 <= System.Math.Pow(2.0, -1022.0)) {
				for (int i = 0; i < 0x432; i++) {
					num2 *= 2.0;
				}
				num4 = Utils.doubleToLong(num2);
			} else {
				num4 = 0xfffffffffffffL & Utils.doubleToLong(d * System.Math.Pow(2.0, 52.0));
			}
			return ((num | num3) | num4);
		}


		/* Public Methods */

		#region Public Methods

		/// <summary>
		/// Returns a new <see cref="BigDecimal"/> instance whose value is equal to 
		/// <paramref name="unscaledVal"/> 10^(-<paramref name="scale"/>). The scale 
		/// of the result is <see cref="scale"/>, and its unscaled value is <see cref="unscaledVal"/>.
		/// </summary>
		/// <param name="unscaledVal">The unscaled value to be used to construct 
		/// the new <see cref="BigDecimal"/>.</param>
		/// <param name="scale">The scale to be used to construct the new <see cref="BigDecimal"/>.</param>
		/// <returns>
		/// Returns a <see cref="BigDecimal"/> instance with the value <c><see cref="unscaledVal"/> 
		/// * 10^(-<see cref="scale"/>)</c>.
		/// </returns>
		public static BigDecimal ValueOf(long unscaledVal, int scale) {
			if (scale == 0)
				return ValueOf(unscaledVal);
			if ((unscaledVal == 0) && (scale >= 0) && 
				(scale < ZeroScaledBy.Length)) {
				return ZeroScaledBy[scale];
			}

			return new BigDecimal(unscaledVal, scale);
		}

		/// <summary>
		/// Returns a new <see cref="BigDecimal"/> instance whose value is equal 
		/// to <paramref name="unscaledVal"/>. The scale of the result is <c>0</c>, 
		/// and its unscaled value is <paramref name="unscaledVal"/>.
		/// </summary>
		/// <param name="unscaledVal">The value to be converted to a <see cref="BigDecimal"/>.</param>
		/// <returns>
		/// Returns a <see cref="BigDecimal"/> instance with the value <paramref name="unscaledVal"/>.
		/// </returns>
		public static BigDecimal ValueOf(long unscaledVal) {
			if ((unscaledVal >= 0) && (unscaledVal < BiScaledByZeroLength)) {
				return BiScaledByZero[(int)unscaledVal];
			}
			return new BigDecimal(unscaledVal, 0);
		}

		/**
		 * Returns a new {@code BigDecimal} instance whose value is equal to {@code
		 * val}. The new decimal is constructed as if the {@code BigDecimal(String)}
		 * constructor is called with an argument which is equal to {@code
		 * Double.toString(val)}. For example, {@code valueOf("0.1")} is converted to
		 * (unscaled=1, scale=1), although the double {@code 0.1} cannot be
		 * represented exactly as a double value. In contrast to that, a new {@code
		 * BigDecimal(0.1)} instance has the value {@code
		 * 0.1000000000000000055511151231257827021181583404541015625} with an
		 * unscaled value {@code 1000000000000000055511151231257827021181583404541015625}
		 * and the scale {@code 55}.
		 *
		 * @param val
		 *            double value to be converted to a {@code BigDecimal}.
		 * @return {@code BigDecimal} instance with the value {@code val}.
		 * @throws NumberFormatException
		 *             if {@code val} is infinite or {@code val} is not a number
		 */
		public static BigDecimal ValueOf(double val) {
			if (Double.IsInfinity(val) || Double.IsNaN(val)) {
				// math.03=Infinity or NaN
				throw new FormatException(SR.GetString("math.03")); //$NON-NLS-1$
			}
			return new BigDecimal(Convert.ToString(val));
		}

		/// <summary>
		/// Adds a value to the current instance of <see cref="BigDecimal"/>.
		/// The scale of the result is the maximum of the scales of the two arguments.
		/// </summary>
		/// <param name="augend">The value to be added to this instance.</param>
		/// <returns>
		/// Returns a new {@code BigDecimal} whose value is <c>this + <paramref name="augend"/></c>.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="augend"/> is <c>null</c>.
		/// </exception>
		public BigDecimal Add(BigDecimal augend) {
			int diffScale = _scale - augend._scale;
			// Fast return when some operand is zero
			if (isZero()) {
				if (diffScale <= 0)
					return augend;
				if (augend.isZero())
					return this;
			} else if (augend.isZero()) {
				if (diffScale >= 0)
					return this;
			}
			// Let be:  this = [u1,s1]  and  augend = [u2,s2]
			if (diffScale == 0) {
				// case s1 == s2: [u1 + u2 , s1]
				if (System.Math.Max(_bitLength, augend._bitLength) + 1 < 64) {
					return ValueOf(smallValue + augend.smallValue, _scale);
				}
				return new BigDecimal(GetUnscaledValue().Add(augend.GetUnscaledValue()), _scale);
			}
			if (diffScale > 0)
				// case s1 > s2 : [(u1 + u2) * 10 ^ (s1 - s2) , s1]
				return AddAndMult10(this, augend, diffScale);
			
			// case s2 > s1 : [(u2 + u1) * 10 ^ (s2 - s1) , s2]
			return AddAndMult10(augend, this, -diffScale);
		}

		private static BigDecimal AddAndMult10(BigDecimal thisValue, BigDecimal augend, int diffScale) {
			if (diffScale < LongTenPow.Length &&
					System.Math.Max(thisValue._bitLength, augend._bitLength + LongTenPowBitLength[diffScale]) + 1 < 64) {
				return ValueOf(thisValue.smallValue + augend.smallValue * LongTenPow[diffScale], thisValue._scale);
			}
			return new BigDecimal(
					thisValue.GetUnscaledValue().Add(Multiplication.multiplyByTenPow(augend.GetUnscaledValue(), diffScale)),
					thisValue._scale);
		}

		/// <summary>
		/// Adds a value to the current instance of <see cref="BigDecimal"/>.
		/// The result is rounded according to the passed context.
		/// </summary>
		/// <param name="augend">The value to be added to this instance.</param>
		/// <param name="mc">The rounding mode and precision for the result of 
		/// this operation.</param>
		/// <returns>
		/// Returns a new {@code BigDecimal} whose value is <c>this + <paramref name="augend"/></c>.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="augend"/> or <paramref name="mc"/> is <c>null</c>.
		/// </exception>
		public BigDecimal Add(BigDecimal augend, MathContext mc) {
			BigDecimal larger; // operand with the largest unscaled value
			BigDecimal smaller; // operand with the smallest unscaled value
			BigInteger tempBI;
			long diffScale = (long)this._scale - augend._scale;
			int largerSignum;
			// Some operand is zero or the precision is infinity  
			if ((augend.isZero()) || (isZero()) || (mc.Precision == 0)) {
				return Add(augend).Round(mc);
			}
			// Cases where there is room for optimizations
			if (AproxPrecision() < diffScale - 1) {
				larger = augend;
				smaller = this;
			} else if (augend.AproxPrecision() < -diffScale - 1) {
				larger = this;
				smaller = augend;
			} else {// No optimization is done 
				return Add(augend).Round(mc);
			}
			if (mc.Precision >= larger.AproxPrecision()) {
				// No optimization is done
				return Add(augend).Round(mc);
			}
			// Cases where it's unnecessary to add two numbers with very different scales 
			largerSignum = larger.Signum();
			if (largerSignum == smaller.Signum()) {
				tempBI = Multiplication.multiplyByPositiveInt(larger.GetUnscaledValue(), 10)
				.Add(BigInteger.ValueOf(largerSignum));
			} else {
				tempBI = larger.GetUnscaledValue().Subtract(
						BigInteger.ValueOf(largerSignum));
				tempBI = Multiplication.multiplyByPositiveInt(tempBI, 10)
				.Add(BigInteger.ValueOf(largerSignum * 9));
			}
			// Rounding the improved adding 
			larger = new BigDecimal(tempBI, larger._scale + 1);
			return larger.Round(mc);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this - subtrahend}.
		 * The scale of the result is the maximum of the scales of the two arguments.
		 *
		 * @param subtrahend
		 *            value to be subtracted from {@code this}.
		 * @return {@code this - subtrahend}.
		 * @throws NullPointerException
		 *             if {@code subtrahend == null}.
		 */
		public BigDecimal Subtract(BigDecimal subtrahend) {
			int diffScale = _scale - subtrahend._scale;
			// Fast return when some operand is zero
			if (isZero()) {
				if (diffScale <= 0) {
					return subtrahend.Negate();
				}
				if (subtrahend.isZero()) {
					return this;
				}
			} else if (subtrahend.isZero()) {
				if (diffScale >= 0) {
					return this;
				}
			}
			// Let be: this = [u1,s1] and subtrahend = [u2,s2] so:
			if (diffScale == 0) {
				// case s1 = s2 : [u1 - u2 , s1]
				if (System.Math.Max(_bitLength, subtrahend._bitLength) + 1 < 64) {
					return ValueOf(smallValue - subtrahend.smallValue, _scale);
				}
				return new BigDecimal(GetUnscaledValue().Subtract(subtrahend.GetUnscaledValue()), _scale);
			}
			if (diffScale > 0) {
				// case s1 > s2 : [ u1 - u2 * 10 ^ (s1 - s2) , s1 ]
				if (diffScale < LongTenPow.Length &&
				    System.Math.Max(_bitLength, subtrahend._bitLength + LongTenPowBitLength[diffScale]) + 1 < 64) {
					return ValueOf(smallValue - subtrahend.smallValue*LongTenPow[diffScale], _scale);
				}
				return new BigDecimal(
						GetUnscaledValue().Subtract(Multiplication.multiplyByTenPow(subtrahend.GetUnscaledValue(), diffScale)),
						_scale);
			}
			
			// case s2 > s1 : [ u1 * 10 ^ (s2 - s1) - u2 , s2 ]
			diffScale = -diffScale;
			if (diffScale < LongTenPow.Length &&
			    System.Math.Max(_bitLength + LongTenPowBitLength[diffScale], subtrahend._bitLength) + 1 < 64) {
				return ValueOf(smallValue*LongTenPow[diffScale] - subtrahend.smallValue, subtrahend._scale);
			}
			return new BigDecimal(Multiplication.multiplyByTenPow(GetUnscaledValue(), diffScale)
			                      	.Subtract(subtrahend.GetUnscaledValue()), subtrahend._scale);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this - subtrahend}.
		 * The result is rounded according to the passed context {@code mc}.
		 *
		 * @param subtrahend
		 *            value to be subtracted from {@code this}.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this - subtrahend}.
		 * @throws NullPointerException
		 *             if {@code subtrahend == null} or {@code mc == null}.
		 */
		public BigDecimal Subtract(BigDecimal subtrahend, MathContext mc) {
			long diffScale = subtrahend._scale - (long)_scale;
			int thisSignum;
			BigDecimal leftOperand; // it will be only the left operand (this) 
			BigInteger tempBI;
			// Some operand is zero or the precision is infinity  
			if ((subtrahend.isZero()) || (isZero()) || (mc.Precision == 0)) {
				return Subtract(subtrahend).Round(mc);
			}
			// Now:   this != 0   and   subtrahend != 0
			if (subtrahend.AproxPrecision() < diffScale - 1) {
				// Cases where it is unnecessary to subtract two numbers with very different scales
				if (mc.Precision < AproxPrecision()) {
					thisSignum = Signum();
					if (thisSignum != subtrahend.Signum()) {
						tempBI = Multiplication.multiplyByPositiveInt(this.GetUnscaledValue(), 10)
						.Add(BigInteger.ValueOf(thisSignum));
					} else {
						tempBI = GetUnscaledValue().Subtract(BigInteger.ValueOf(thisSignum));
						tempBI = Multiplication.multiplyByPositiveInt(tempBI, 10)
						.Add(BigInteger.ValueOf(thisSignum * 9));
					}
					// Rounding the improved subtracting
					leftOperand = new BigDecimal(tempBI, _scale + 1);
					return leftOperand.Round(mc);
				}
			}
			// No optimization is done
			return Subtract(subtrahend).Round(mc);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this *
		 * multiplicand}. The scale of the result is the sum of the scales of the
		 * two arguments.
		 *
		 * @param multiplicand
		 *            value to be multiplied with {@code this}.
		 * @return {@code this * multiplicand}.
		 * @throws NullPointerException
		 *             if {@code multiplicand == null}.
		 */
		public BigDecimal Multiply(BigDecimal multiplicand) {
			long newScale = (long)_scale + multiplicand._scale;

			if ((isZero()) || (multiplicand.isZero())) {
				return GetZeroScaledBy(newScale);
			}
			/* Let be: this = [u1,s1] and multiplicand = [u2,s2] so:
			 * this x multiplicand = [ s1 * s2 , s1 + s2 ] */
			if (_bitLength + multiplicand._bitLength < 64) {
				return ValueOf(smallValue * multiplicand.smallValue, ToIntScale(newScale));
			}
			return new BigDecimal(GetUnscaledValue().Multiply(multiplicand.GetUnscaledValue()), ToIntScale(newScale));
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this *
		 * multiplicand}. The result is rounded according to the passed context
		 * {@code mc}.
		 *
		 * @param multiplicand
		 *            value to be multiplied with {@code this}.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this * multiplicand}.
		 * @throws NullPointerException
		 *             if {@code multiplicand == null} or {@code mc == null}.
		 */
		public BigDecimal Multiply(BigDecimal multiplicand, MathContext mc) {
			BigDecimal result = Multiply(multiplicand);

			result.InplaceRound(mc);
			return result;
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * As scale of the result the parameter {@code scale} is used. If rounding
		 * is required to meet the specified scale, then the specified rounding mode
		 * {@code roundingMode} is applied.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param scale
		 *            the scale of the result returned.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return {@code this / divisor} rounded according to the given rounding
		 *         mode.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws IllegalArgumentException
		 *             if {@code roundingMode} is not a valid rounding mode.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == ROUND_UNNECESSARY} and rounding is
		 *             necessary according to the given scale.
		 */
		public BigDecimal Divide(BigDecimal divisor, int scale, int roundingMode) {
			return Divide(divisor, scale, (RoundingMode)roundingMode);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * As scale of the result the parameter {@code scale} is used. If rounding
		 * is required to meet the specified scale, then the specified rounding mode
		 * {@code roundingMode} is applied.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param scale
		 *            the scale of the result returned.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return {@code this / divisor} rounded according to the given rounding
		 *         mode.
		 * @throws NullPointerException
		 *             if {@code divisor == null} or {@code roundingMode == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == RoundingMode.UNNECESSAR}Y and
		 *             rounding is necessary according to the given scale and given
		 *             precision.
		 */
		public BigDecimal Divide(BigDecimal divisor, int scale, RoundingMode roundingMode) {
			// Let be: this = [u1,s1]  and  divisor = [u2,s2]
			if (divisor.isZero()) {
				// math.04=Division by zero
				throw new ArithmeticException(SR.GetString("math.04")); //$NON-NLS-1$
			}

			long diffScale = ((long)_scale - divisor._scale) - scale;
			if (_bitLength < 64 && divisor._bitLength < 64) {
				if (diffScale == 0)
					return DividePrimitiveLongs(smallValue, divisor.smallValue, scale, roundingMode);
				if (diffScale > 0) {
					if (diffScale < LongTenPow.Length &&
							divisor._bitLength + LongTenPowBitLength[(int)diffScale] < 64) {
						return DividePrimitiveLongs(smallValue, divisor.smallValue*LongTenPow[(int) diffScale], scale, roundingMode);
					}
				} else { // diffScale < 0
					if (-diffScale < LongTenPow.Length &&
						_bitLength + LongTenPowBitLength[(int)-diffScale] < 64) {
						return DividePrimitiveLongs(smallValue*LongTenPow[(int) -diffScale], divisor.smallValue, scale, roundingMode);
					}

				}
			}
			BigInteger scaledDividend = GetUnscaledValue();
			BigInteger scaledDivisor = divisor.GetUnscaledValue(); // for scaling of 'u2'

			if (diffScale > 0) {
				// Multiply 'u2'  by:  10^((s1 - s2) - scale)
				scaledDivisor = Multiplication.multiplyByTenPow(scaledDivisor, (int)diffScale);
			} else if (diffScale < 0) {
				// Multiply 'u1'  by:  10^(scale - (s1 - s2))
				scaledDividend = Multiplication.multiplyByTenPow(scaledDividend, (int)-diffScale);
			}
			return DivideBigIntegers(scaledDividend, scaledDivisor, scale, roundingMode);
		}

		#endregion

		private static BigDecimal DivideBigIntegers(BigInteger scaledDividend, BigInteger scaledDivisor, int scale, RoundingMode roundingMode) {

			BigInteger[] quotAndRem = scaledDividend.DivideAndRemainder(scaledDivisor);  // quotient and remainder
			// If after division there is a remainder...
			BigInteger quotient = quotAndRem[0];
			BigInteger remainder = quotAndRem[1];
			if (remainder.Signum() == 0) {
				return new BigDecimal(quotient, scale);
			}
			int sign = scaledDividend.Signum() * scaledDivisor.Signum();
			int compRem;                                      // 'compare to remainder'
			if (scaledDivisor.BitLength < 63) { // 63 in order to avoid out of long after <<1
				long rem = remainder.ToInt64();
				long divisor = scaledDivisor.ToInt64();
				compRem = LongCompareTo(System.Math.Abs(rem) << 1, System.Math.Abs(divisor));
				// To look if there is a carry
				compRem = RoundingBehavior(quotient.TestBit(0) ? 1 : 0,
						sign * (5 + compRem), roundingMode);

			} else {
				// Checking if:  remainder * 2 >= scaledDivisor 
				compRem = remainder.Abs().ShiftLeftOneBit().CompareTo(scaledDivisor.Abs());
				compRem = RoundingBehavior(quotient.TestBit(0) ? 1 : 0,
						sign * (5 + compRem), roundingMode);
			}
			if (compRem != 0) {
				if (quotient.BitLength < 63) {
					return ValueOf(quotient.ToInt64() + compRem, scale);
				}
				quotient = quotient.Add(BigInteger.ValueOf(compRem));
				return new BigDecimal(quotient, scale);
			}
			// Constructing the result with the appropriate unscaled value
			return new BigDecimal(quotient, scale);
		}

		private static BigDecimal DividePrimitiveLongs(long scaledDividend, long scaledDivisor, int scale, RoundingMode roundingMode) {
			long quotient = scaledDividend / scaledDivisor;
			long remainder = scaledDividend % scaledDivisor;
			int sign = System.Math.Sign(scaledDividend) * System.Math.Sign(scaledDivisor);
			if (remainder != 0) {
				// Checking if:  remainder * 2 >= scaledDivisor
				int compRem;                                      // 'compare to remainder'
				compRem = LongCompareTo(System.Math.Abs(remainder) << 1, System.Math.Abs(scaledDivisor));
				// To look if there is a carry
				quotient += RoundingBehavior(((int)quotient) & 1, sign * (5 + compRem), roundingMode);
			}
			// Constructing the result with the appropriate unscaled value
			return ValueOf(quotient, scale);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * The scale of the result is the scale of {@code this}. If rounding is
		 * required to meet the specified scale, then the specified rounding mode
		 * {@code roundingMode} is applied.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return {@code this / divisor} rounded according to the given rounding
		 *         mode.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws IllegalArgumentException
		 *             if {@code roundingMode} is not a valid rounding mode.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == ROUND_UNNECESSARY} and rounding is
		 *             necessary according to the scale of this.
		 */
		public BigDecimal Divide(BigDecimal divisor, int roundingMode) {
			return Divide(divisor, _scale, (RoundingMode)roundingMode);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * The scale of the result is the scale of {@code this}. If rounding is
		 * required to meet the specified scale, then the specified rounding mode
		 * {@code roundingMode} is applied.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return {@code this / divisor} rounded according to the given rounding
		 *         mode.
		 * @throws NullPointerException
		 *             if {@code divisor == null} or {@code roundingMode == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == RoundingMode.UNNECESSARY} and
		 *             rounding is necessary according to the scale of this.
		 */
		public BigDecimal Divide(BigDecimal divisor, RoundingMode roundingMode) {
			return Divide(divisor, _scale, roundingMode);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * The scale of the result is the difference of the scales of {@code this}
		 * and {@code divisor}. If the exact result requires more digits, then the
		 * scale is adjusted accordingly. For example, {@code 1/128 = 0.0078125}
		 * which has a scale of {@code 7} and precision {@code 5}.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @return {@code this / divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if the result cannot be represented exactly.
		 */
		public BigDecimal Divide(BigDecimal divisor) {
			BigInteger p = this.GetUnscaledValue();
			BigInteger q = divisor.GetUnscaledValue();
			BigInteger gcd; // greatest common divisor between 'p' and 'q'
			BigInteger[] quotAndRem;
			long diffScale = (long)_scale - divisor._scale;
			int newScale; // the new scale for final quotient
			int k; // number of factors "2" in 'q'
			int l = 0; // number of factors "5" in 'q'
			int i = 1;
			int lastPow = FivePow.Length - 1;

			if (divisor.isZero()) {
				// math.04=Division by zero
				throw new ArithmeticException(SR.GetString("math.04")); //$NON-NLS-1$
			}
			if (p.Signum() == 0) {
				return GetZeroScaledBy(diffScale);
			}
			// To divide both by the GCD
			gcd = p.Gcd(q);
			p = p.Divide(gcd);
			q = q.Divide(gcd);
			// To simplify all "2" factors of q, dividing by 2^k
			k = q.LowestSetBit;
			q = q.ShiftRight(k);
			// To simplify all "5" factors of q, dividing by 5^l
			do {
				quotAndRem = q.DivideAndRemainder(FivePow[i]);
				if (quotAndRem[1].Signum() == 0) {
					l += i;
					if (i < lastPow) {
						i++;
					}
					q = quotAndRem[0];
				} else {
					if (i == 1) {
						break;
					}
					i = 1;
				}
			} while (true);
			// If  abs(q) != 1  then the quotient is periodic
			if (!q.Abs().Equals(BigInteger.One)) {
				// math.05=Non-terminating decimal expansion; no exact representable decimal result.
				throw new ArithmeticException(SR.GetString("math.05")); //$NON-NLS-1$
			}
			// The sign of the is fixed and the quotient will be saved in 'p'
			if (q.Signum() < 0) {
				p = p.Negate();
			}
			// Checking if the new scale is out of range
			newScale = ToIntScale(diffScale + System.Math.Max(k, l));
			// k >= 0  and  l >= 0  implies that  k - l  is in the 32-bit range
			i = k - l;

			p = (i > 0) ? Multiplication.multiplyByFivePow(p, i)
			: p.ShiftLeft(-i);
			return new BigDecimal(p, newScale);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this / divisor}.
		 * The result is rounded according to the passed context {@code mc}. If the
		 * passed math context specifies precision {@code 0}, then this call is
		 * equivalent to {@code this.divide(divisor)}.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this / divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null} or {@code mc == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code mc.getRoundingMode() == UNNECESSARY} and rounding
		 *             is necessary according {@code mc.getPrecision()}.
		 */
		public BigDecimal Divide(BigDecimal divisor, MathContext mc) {
			/* Calculating how many zeros must be append to 'dividend'
			 * to obtain a  quotient with at least 'mc.precision()' digits */
			long traillingZeros = mc.Precision + 2L
					+ divisor.AproxPrecision() - AproxPrecision();
			long diffScale = (long)_scale - divisor._scale;
			long newScale = diffScale; // scale of the final quotient
			int compRem; // to compare the remainder
			int i = 1; // index   
			int lastPow = TenPow.Length - 1; // last power of ten
			BigInteger integerQuot; // for temporal results
			BigInteger[] quotAndRem = { GetUnscaledValue() };
			// In special cases it reduces the problem to call the dual method
			if ((mc.Precision == 0) || (isZero()) || (divisor.isZero()))
				return Divide(divisor);

			if (traillingZeros > 0) {
				// To append trailing zeros at end of dividend
				quotAndRem[0] = GetUnscaledValue().Multiply(Multiplication.powerOf10(traillingZeros));
				newScale += traillingZeros;
			}
			quotAndRem = quotAndRem[0].DivideAndRemainder(divisor.GetUnscaledValue());
			integerQuot = quotAndRem[0];
			// Calculating the exact quotient with at least 'mc.precision()' digits
			if (quotAndRem[1].Signum() != 0) {
				// Checking if:   2 * remainder >= divisor ?
				compRem = quotAndRem[1].ShiftLeftOneBit().CompareTo(divisor.GetUnscaledValue());
				// quot := quot * 10 + r;     with 'r' in {-6,-5,-4, 0,+4,+5,+6}
				integerQuot = integerQuot.Multiply(BigInteger.Ten)
				.Add(BigInteger.ValueOf(quotAndRem[0].Signum() * (5 + compRem)));
				newScale++;
			} else {
				// To strip trailing zeros until the preferred scale is reached
				while (!integerQuot.TestBit(0)) {
					quotAndRem = integerQuot.DivideAndRemainder(TenPow[i]);
					if ((quotAndRem[1].Signum() == 0)
							&& (newScale - i >= diffScale)) {
						newScale -= i;
						if (i < lastPow) {
							i++;
						}
						integerQuot = quotAndRem[0];
					} else {
						if (i == 1) {
							break;
						}
						i = 1;
					}
				}
			}
			// To perform rounding
			return new BigDecimal(integerQuot, ToIntScale(newScale), mc);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the integral part of
		 * {@code this / divisor}. The quotient is rounded down towards zero to the
		 * next integer. For example, {@code 0.5/0.2 = 2}.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @return integral part of {@code this / divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 */
		public BigDecimal DivideToIntegralValue(BigDecimal divisor) {
			BigInteger integralValue; // the integer of result
			BigInteger powerOfTen; // some power of ten
			BigInteger[] quotAndRem = { GetUnscaledValue() };
			long newScale = (long)_scale - divisor._scale;
			long tempScale = 0;
			int i = 1;
			int lastPow = TenPow.Length - 1;

			if (divisor.isZero()) {
				// math.04=Division by zero
				throw new ArithmeticException(SR.GetString("math.04")); //$NON-NLS-1$
			}
			if ((divisor.AproxPrecision() + newScale > this.AproxPrecision() + 1L)
			|| (this.isZero())) {
				/* If the divisor's integer part is greater than this's integer part,
				 * the result must be zero with the appropriate scale */
				integralValue = BigInteger.Zero;
			} else if (newScale == 0) {
				integralValue = GetUnscaledValue().Divide(divisor.GetUnscaledValue());
			} else if (newScale > 0) {
				powerOfTen = Multiplication.powerOf10(newScale);
				integralValue = GetUnscaledValue().Divide(divisor.GetUnscaledValue().Multiply(powerOfTen));
				integralValue = integralValue.Multiply(powerOfTen);
			} else {// (newScale < 0)
				powerOfTen = Multiplication.powerOf10(-newScale);
				integralValue = GetUnscaledValue().Multiply(powerOfTen).Divide(divisor.GetUnscaledValue());
				// To strip trailing zeros approximating to the preferred scale
				while (!integralValue.TestBit(0)) {
					quotAndRem = integralValue.DivideAndRemainder(TenPow[i]);
					if ((quotAndRem[1].Signum() == 0)
							&& (tempScale - i >= newScale)) {
						tempScale -= i;
						if (i < lastPow) {
							i++;
						}
						integralValue = quotAndRem[0];
					} else {
						if (i == 1) {
							break;
						}
						i = 1;
					}
				}
				newScale = tempScale;
			}
			return ((integralValue.Signum() == 0)
			? GetZeroScaledBy(newScale)
					: new BigDecimal(integralValue, ToIntScale(newScale)));
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the integral part of
		 * {@code this / divisor}. The quotient is rounded down towards zero to the
		 * next integer. The rounding mode passed with the parameter {@code mc} is
		 * not considered. But if the precision of {@code mc > 0} and the integral
		 * part requires more digits, then an {@code ArithmeticException} is thrown.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param mc
		 *            math context which determines the maximal precision of the
		 *            result.
		 * @return integral part of {@code this / divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null} or {@code mc == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code mc.getPrecision() > 0} and the result requires more
		 *             digits to be represented.
		 */
		public BigDecimal DivideToIntegralValue(BigDecimal divisor, MathContext mc) {
			int mcPrecision = mc.Precision;
			int diffPrecision = Precision - divisor.Precision;
			int lastPow = TenPow.Length - 1;
			long diffScale = (long)_scale - divisor._scale;
			long newScale = diffScale;
			long quotPrecision = diffPrecision - diffScale + 1;
			BigInteger[] quotAndRem = new BigInteger[2];
			// In special cases it call the dual method
			if ((mcPrecision == 0) || (isZero()) || (divisor.isZero())) {
				return DivideToIntegralValue(divisor);
			}
			// Let be:   this = [u1,s1]   and   divisor = [u2,s2]
			if (quotPrecision <= 0) {
				quotAndRem[0] = BigInteger.Zero;
			} else if (diffScale == 0) {
				// CASE s1 == s2:  to calculate   u1 / u2 
				quotAndRem[0] = GetUnscaledValue().Divide(divisor.GetUnscaledValue());
			} else if (diffScale > 0) {
				// CASE s1 >= s2:  to calculate   u1 / (u2 * 10^(s1-s2)  
				quotAndRem[0] = GetUnscaledValue().Divide(divisor.GetUnscaledValue().Multiply(Multiplication.powerOf10(diffScale)));
				// To chose  10^newScale  to get a quotient with at least 'mc.precision()' digits
				newScale = System.Math.Min(diffScale, System.Math.Max(mcPrecision - quotPrecision + 1, 0));
				// To calculate: (u1 / (u2 * 10^(s1-s2)) * 10^newScale
				quotAndRem[0] = quotAndRem[0].Multiply(Multiplication.powerOf10(newScale));
			} else {// CASE s2 > s1:   
				/* To calculate the minimum power of ten, such that the quotient 
				 *   (u1 * 10^exp) / u2   has at least 'mc.precision()' digits. */
				long exp = System.Math.Min(-diffScale, System.Math.Max((long)mcPrecision - diffPrecision, 0));
				long compRemDiv;
				// Let be:   (u1 * 10^exp) / u2 = [q,r]  
				quotAndRem = GetUnscaledValue().Multiply(Multiplication.powerOf10(exp)).
						DivideAndRemainder(divisor.GetUnscaledValue());
				newScale += exp; // To fix the scale
				exp = -newScale; // The remaining power of ten
				// If after division there is a remainder...
				if ((quotAndRem[1].Signum() != 0) && (exp > 0)) {
					// Log10(r) + ((s2 - s1) - exp) > mc.precision ?
					compRemDiv = (new BigDecimal(quotAndRem[1])).Precision
					+ exp - divisor.Precision;
					if (compRemDiv == 0) {
						// To calculate:  (r * 10^exp2) / u2
						quotAndRem[1] = quotAndRem[1].Multiply(Multiplication.powerOf10(exp)).
								Divide(divisor.GetUnscaledValue());
						compRemDiv = System.Math.Abs(quotAndRem[1].Signum());
					}
					if (compRemDiv > 0) {
						// The quotient won't fit in 'mc.precision()' digits
						// math.06=Division impossible
						throw new ArithmeticException(SR.GetString("math.06")); //$NON-NLS-1$
					}
				}
			}
			// Fast return if the quotient is zero
			if (quotAndRem[0].Signum() == 0) {
				return GetZeroScaledBy(diffScale);
			}
			BigInteger strippedBI = quotAndRem[0];
			BigDecimal integralValue = new BigDecimal(quotAndRem[0]);
			long resultPrecision = integralValue.Precision;
			int i = 1;
			// To strip trailing zeros until the specified precision is reached
			while (!strippedBI.TestBit(0)) {
				quotAndRem = strippedBI.DivideAndRemainder(TenPow[i]);
				if ((quotAndRem[1].Signum() == 0) &&
						((resultPrecision - i >= mcPrecision)
						|| (newScale - i >= diffScale))) {
					resultPrecision -= i;
					newScale -= i;
					if (i < lastPow) {
						i++;
					}
					strippedBI = quotAndRem[0];
				} else {
					if (i == 1) {
						break;
					}
					i = 1;
				}
			}
			// To check if the result fit in 'mc.precision()' digits
			if (resultPrecision > mcPrecision) {
				// math.06=Division impossible
				throw new ArithmeticException(SR.GetString("math.06")); //$NON-NLS-1$
			}
			integralValue._scale = ToIntScale(newScale);
			integralValue.SetUnscaledValue(strippedBI);
			return integralValue;
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this % divisor}.
		 * <p>
		 * The remainder is defined as {@code this -
		 * this.divideToIntegralValue(divisor) * divisor}.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @return {@code this % divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 */
		public BigDecimal Remainder(BigDecimal divisor) {
			return DivideAndRemainder(divisor)[1];
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this % divisor}.
		 * <p>
		 * The remainder is defined as {@code this -
		 * this.divideToIntegralValue(divisor) * divisor}.
		 * <p>
		 * The specified rounding mode {@code mc} is used for the division only.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param mc
		 *            rounding mode and precision to be used.
		 * @return {@code this % divisor}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @throws ArithmeticException
		 *             if {@code mc.getPrecision() > 0} and the result of {@code
		 *             this.divideToIntegralValue(divisor, mc)} requires more digits
		 *             to be represented.
		 */
		public BigDecimal Remainder(BigDecimal divisor, MathContext mc) {
			return DivideAndRemainder(divisor, mc)[1];
		}

		/**
		 * Returns a {@code BigDecimal} array which contains the integral part of
		 * {@code this / divisor} at index 0 and the remainder {@code this %
		 * divisor} at index 1. The quotient is rounded down towards zero to the
		 * next integer.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @return {@code [this.divideToIntegralValue(divisor),
		 *         this.remainder(divisor)]}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @see #divideToIntegralValue
		 * @see #remainder
		 */
		public BigDecimal[] DivideAndRemainder(BigDecimal divisor) {
			BigDecimal[] quotAndRem = new BigDecimal[2];

			quotAndRem[0] = DivideToIntegralValue(divisor);
			quotAndRem[1] = Subtract(quotAndRem[0].Multiply(divisor));
			return quotAndRem;
		}

		/**
		 * Returns a {@code BigDecimal} array which contains the integral part of
		 * {@code this / divisor} at index 0 and the remainder {@code this %
		 * divisor} at index 1. The quotient is rounded down towards zero to the
		 * next integer. The rounding mode passed with the parameter {@code mc} is
		 * not considered. But if the precision of {@code mc > 0} and the integral
		 * part requires more digits, then an {@code ArithmeticException} is thrown.
		 *
		 * @param divisor
		 *            value by which {@code this} is divided.
		 * @param mc
		 *            math context which determines the maximal precision of the
		 *            result.
		 * @return {@code [this.divideToIntegralValue(divisor),
		 *         this.remainder(divisor)]}.
		 * @throws NullPointerException
		 *             if {@code divisor == null}.
		 * @throws ArithmeticException
		 *             if {@code divisor == 0}.
		 * @see #divideToIntegralValue
		 * @see #remainder
		 */
		public BigDecimal[] DivideAndRemainder(BigDecimal divisor, MathContext mc) {
			BigDecimal[] quotAndRem = new BigDecimal[2];

			quotAndRem[0] = DivideToIntegralValue(divisor, mc);
			quotAndRem[1] = Subtract(quotAndRem[0].Multiply(divisor));
			return quotAndRem;
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this ^ n}. The
		 * scale of the result is {@code n} times the scales of {@code this}.
		 * <p>
		 * {@code x.pow(0)} returns {@code 1}, even if {@code x == 0}.
		 * <p>
		 * Implementation Note: The implementation is based on the ANSI standard
		 * X3.274-1996 algorithm.
		 *
		 * @param n
		 *            exponent to which {@code this} is raised.
		 * @return {@code this ^ n}.
		 * @throws ArithmeticException
		 *             if {@code n < 0} or {@code n > 999999999}.
		 */
		public BigDecimal Pow(int n) {
			if (n == 0) {
				return One;
			}
			if ((n < 0) || (n > 999999999)) {
				// math.07=Invalid Operation
				throw new ArithmeticException(SR.GetString("math.07")); //$NON-NLS-1$
			}
			long newScale = _scale * (long)n;
			// Let be: this = [u,s]   so:  this^n = [u^n, s*n]
			return ((isZero())
			? GetZeroScaledBy(newScale)
			: new BigDecimal(GetUnscaledValue().Pow(n), ToIntScale(newScale)));
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this ^ n}. The
		 * result is rounded according to the passed context {@code mc}.
		 * <p>
		 * Implementation Note: The implementation is based on the ANSI standard
		 * X3.274-1996 algorithm.
		 *
		 * @param n
		 *            exponent to which {@code this} is raised.
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this ^ n}.
		 * @throws ArithmeticException
		 *             if {@code n < 0} or {@code n > 999999999}.
		 */
		public BigDecimal Pow(int n, MathContext mc) {
			// The ANSI standard X3.274-1996 algorithm
			int m = System.Math.Abs(n);
			int mcPrecision = mc.Precision;
			int elength = (int)System.Math.Log10(m) + 1;   // decimal digits in 'n'
			int oneBitMask; // mask of bits
			BigDecimal accum; // the single accumulator
			MathContext newPrecision = mc; // MathContext by default

			// In particular cases, it reduces the problem to call the other 'pow()'
			if ((n == 0) || ((isZero()) && (n > 0))) {
				return Pow(n);
			}
			if ((m > 999999999) || ((mcPrecision == 0) && (n < 0))
					|| ((mcPrecision > 0) && (elength > mcPrecision))) {
				// math.07=Invalid Operation
				throw new ArithmeticException(SR.GetString("math.07")); //$NON-NLS-1$
			}
			if (mcPrecision > 0) {
				newPrecision = new MathContext(mcPrecision + elength + 1,
						mc.RoundingMode);
			}
			// The result is calculated as if 'n' were positive        
			accum = Round(newPrecision);
			oneBitMask = Utils.highestOneBit(m) >> 1;

			while (oneBitMask > 0) {
				accum = accum.Multiply(accum, newPrecision);
				if ((m & oneBitMask) == oneBitMask) {
					accum = accum.Multiply(this, newPrecision);
				}
				oneBitMask >>= 1;
			}
			// If 'n' is negative, the value is divided into 'ONE'
			if (n < 0) {
				accum = One.Divide(accum, newPrecision);
			}
			// The final value is rounded to the destination precision
			accum.InplaceRound(mc);
			return accum;
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the absolute value of
		 * {@code this}. The scale of the result is the same as the scale of this.
		 *
		 * @return {@code abs(this)}
		 */
		public BigDecimal Abs() {
			return ((Signum() < 0) ? Negate() : this);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the absolute value of
		 * {@code this}. The result is rounded according to the passed context
		 * {@code mc}.
		 *
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code abs(this)}
		 */
		public BigDecimal Abs(MathContext mc) {
			return Round(mc).Abs();
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the {@code -this}. The
		 * scale of the result is the same as the scale of this.
		 *
		 * @return {@code -this}
		 */
		public BigDecimal Negate() {
			if (_bitLength < 63 || (_bitLength == 63 && smallValue != Int64.MinValue)) {
				return ValueOf(-smallValue, _scale);
			}
			return new BigDecimal(GetUnscaledValue().Negate(), _scale);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is the {@code -this}. The
		 * result is rounded according to the passed context {@code mc}.
		 *
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code -this}
		 */
		public BigDecimal Negate(MathContext mc) {
			return Round(mc).Negate();
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code +this}. The scale
		 * of the result is the same as the scale of this.
		 *
		 * @return {@code this}
		 */
		public BigDecimal Plus() {
			return this;
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code +this}. The result
		 * is rounded according to the passed context {@code mc}.
		 *
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this}, rounded
		 */
		public BigDecimal Plus(MathContext mc) {
			return Round(mc);
		}

		/**
		 * Returns the sign of this {@code BigDecimal}.
		 *
		 * @return {@code -1} if {@code this < 0},
		 *         {@code 0} if {@code this == 0},
		 *         {@code 1} if {@code this > 0}.     */
		public int Signum() {
			if (_bitLength < 64) {
				return System.Math.Sign(smallValue);
			}
			return GetUnscaledValue().Signum();
		}

		private bool isZero() {
			//Watch out: -1 has a bitLength=0
			return _bitLength == 0 && this.smallValue != -1;
		}

		/**
		 * Returns the scale of this {@code BigDecimal}. The scale is the number of
		 * digits behind the decimal point. The value of this {@code BigDecimal} is
		 * the unsignedValue * 10^(-scale). If the scale is negative, then this
		 * {@code BigDecimal} represents a big integer.
		 *
		 * @return the scale of this {@code BigDecimal}.
		 */

		public int Scale {
			get { return _scale; }
		}

		/**
		 * Returns the precision of this {@code BigDecimal}. The precision is the
		 * number of decimal digits used to represent this decimal. It is equivalent
		 * to the number of digits of the unscaled value. The precision of {@code 0}
		 * is {@code 1} (independent of the scale).
		 *
		 * @return the precision of this {@code BigDecimal}.
		 */

		public int Precision {
			get {
				// Checking if the precision already was calculated
				if (_precision > 0) {
					return _precision;
				}
				int bitLength = this._bitLength;
				int decimalDigits = 1; // the precision to be calculated
				double doubleUnsc = 1; // intVal in 'double'

				if (bitLength < 1024) {
					// To calculate the precision for small numbers
					if (bitLength >= 64) {
						doubleUnsc = GetUnscaledValue().ToDouble();
					} else if (bitLength >= 1) {
						doubleUnsc = smallValue;
					}
					decimalDigits += (int) System.Math.Log10(System.Math.Abs(doubleUnsc));
				} else {
// (bitLength >= 1024)
					/* To calculate the precision for large numbers
				 * Note that: 2 ^(bitlength() - 1) <= intVal < 10 ^(precision()) */
					decimalDigits += (int) ((bitLength - 1)*Log10_2);
					// If after division the number isn't zero, exists an aditional digit
					if (GetUnscaledValue().Divide(Multiplication.powerOf10(decimalDigits)).Signum() != 0) {
						decimalDigits++;
					}
				}
				_precision = decimalDigits;
				return _precision;
			}
		}

		/**
		 * Returns the unscaled value (mantissa) of this {@code BigDecimal} instance
		 * as a {@code BigInteger}. The unscaled value can be computed as {@code
		 * this} 10^(scale).
		 *
		 * @return unscaled value (this * 10^(scale)).
		 */

		public BigInteger UnscaledValue {
			get { return GetUnscaledValue(); }
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this}, rounded
		 * according to the passed context {@code mc}.
		 * <p>
		 * If {@code mc.precision = 0}, then no rounding is performed.
		 * <p>
		 * If {@code mc.precision > 0} and {@code mc.roundingMode == UNNECESSARY},
		 * then an {@code ArithmeticException} is thrown if the result cannot be
		 * represented exactly within the given precision.
		 *
		 * @param mc
		 *            rounding mode and precision for the result of this operation.
		 * @return {@code this} rounded according to the passed context.
		 * @throws ArithmeticException
		 *             if {@code mc.precision > 0} and {@code mc.roundingMode ==
		 *             UNNECESSARY} and this cannot be represented within the given
		 *             precision.
		 */
		public BigDecimal Round(MathContext mc) {
			BigDecimal thisBD = new BigDecimal(GetUnscaledValue(), _scale);

			thisBD.InplaceRound(mc);
			return thisBD;
		}

		/**
		 * Returns a new {@code BigDecimal} instance with the specified scale.
		 * <p>
		 * If the new scale is greater than the old scale, then additional zeros are
		 * added to the unscaled value. In this case no rounding is necessary.
		 * <p>
		 * If the new scale is smaller than the old scale, then trailing digits are
		 * removed. If these trailing digits are not zero, then the remaining
		 * unscaled value has to be rounded. For this rounding operation the
		 * specified rounding mode is used.
		 *
		 * @param newScale
		 *            scale of the result returned.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return a new {@code BigDecimal} instance with the specified scale.
		 * @throws NullPointerException
		 *             if {@code roundingMode == null}.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == ROUND_UNNECESSARY} and rounding is
		 *             necessary according to the given scale.
		 */
		public BigDecimal SetScale(int newScale, RoundingMode roundingMode) {
			long diffScale = newScale - (long)_scale;
			// Let be:  'this' = [u,s]        
			if (diffScale == 0) {
				return this;
			}
			if (diffScale > 0) {
				// return  [u * 10^(s2 - s), newScale]
				if (diffScale < LongTenPow.Length &&
						(this._bitLength + LongTenPowBitLength[(int)diffScale]) < 64) {
					return ValueOf(this.smallValue * LongTenPow[(int)diffScale], newScale);
				}
				return new BigDecimal(Multiplication.multiplyByTenPow(GetUnscaledValue(), (int)diffScale), newScale);
			}
			// diffScale < 0
			// return  [u,s] / [1,newScale]  with the appropriate scale and rounding
			if (this._bitLength < 64 && -diffScale < LongTenPow.Length) {
				return DividePrimitiveLongs(this.smallValue, LongTenPow[(int)-diffScale], newScale, roundingMode);
			}
			return DivideBigIntegers(this.GetUnscaledValue(), Multiplication.powerOf10(-diffScale), newScale, roundingMode);
		}

		/**
		 * Returns a new {@code BigDecimal} instance with the specified scale.
		 * <p>
		 * If the new scale is greater than the old scale, then additional zeros are
		 * added to the unscaled value. In this case no rounding is necessary.
		 * <p>
		 * If the new scale is smaller than the old scale, then trailing digits are
		 * removed. If these trailing digits are not zero, then the remaining
		 * unscaled value has to be rounded. For this rounding operation the
		 * specified rounding mode is used.
		 *
		 * @param newScale
		 *            scale of the result returned.
		 * @param roundingMode
		 *            rounding mode to be used to round the result.
		 * @return a new {@code BigDecimal} instance with the specified scale.
		 * @throws IllegalArgumentException
		 *             if {@code roundingMode} is not a valid rounding mode.
		 * @throws ArithmeticException
		 *             if {@code roundingMode == ROUND_UNNECESSARY} and rounding is
		 *             necessary according to the given scale.
		 */
		public BigDecimal SetScale(int newScale, int roundingMode) {
			RoundingMode rm = (RoundingMode)roundingMode;
			if ((roundingMode < (int)RoundingMode.Up) || 
				(roundingMode > (int)RoundingMode.Unnecessary)) {
				throw new ArgumentException("roundingMode");
			}
			return SetScale(newScale, (RoundingMode)roundingMode);
		}

		/**
		 * Returns a new {@code BigDecimal} instance with the specified scale. If
		 * the new scale is greater than the old scale, then additional zeros are
		 * added to the unscaled value. If the new scale is smaller than the old
		 * scale, then trailing zeros are removed. If the trailing digits are not
		 * zeros then an ArithmeticException is thrown.
		 * <p>
		 * If no exception is thrown, then the following equation holds: {@code
		 * x.setScale(s).compareTo(x) == 0}.
		 *
		 * @param newScale
		 *            scale of the result returned.
		 * @return a new {@code BigDecimal} instance with the specified scale.
		 * @throws ArithmeticException
		 *             if rounding would be necessary.
		 */
		public BigDecimal SetScale(int newScale) {
			return SetScale(newScale, RoundingMode.Unnecessary);
		}

		/**
		 * Returns a new {@code BigDecimal} instance where the decimal point has
		 * been moved {@code n} places to the left. If {@code n < 0} then the
		 * decimal point is moved {@code -n} places to the right.
		 * <p>
		 * The result is obtained by changing its scale. If the scale of the result
		 * becomes negative, then its precision is increased such that the scale is
		 * zero.
		 * <p>
		 * Note, that {@code movePointLeft(0)} returns a result which is
		 * mathematically equivalent, but which has {@code scale >= 0}.
		 *
		 * @param n
		 *            number of placed the decimal point has to be moved.
		 * @return {@code this * 10^(-n}).
		 */
		public BigDecimal MovePointLeft(int n) {
			return MovePoint(_scale + (long)n);
		}

		private BigDecimal MovePoint(long newScale) {
			if (isZero()) {
				return GetZeroScaledBy(System.Math.Max(newScale, 0));
			}
			/* When:  'n'== Integer.MIN_VALUE  isn't possible to call to movePointRight(-n)  
			 * since  -Integer.MIN_VALUE == Integer.MIN_VALUE */
			if (newScale >= 0) {
				if (_bitLength < 64) {
					return ValueOf(smallValue, ToIntScale(newScale));
				}
				return new BigDecimal(GetUnscaledValue(), ToIntScale(newScale));
			}
			if (-newScale < LongTenPow.Length &&
					_bitLength + LongTenPowBitLength[(int)-newScale] < 64) {
				return ValueOf(smallValue * LongTenPow[(int)-newScale], 0);
			}
			return new BigDecimal(Multiplication.multiplyByTenPow(GetUnscaledValue(), (int)-newScale), 0);
		}

		/**
		 * Returns a new {@code BigDecimal} instance where the decimal point has
		 * been moved {@code n} places to the right. If {@code n < 0} then the
		 * decimal point is moved {@code -n} places to the left.
		 * <p>
		 * The result is obtained by changing its scale. If the scale of the result
		 * becomes negative, then its precision is increased such that the scale is
		 * zero.
		 * <p>
		 * Note, that {@code movePointRight(0)} returns a result which is
		 * mathematically equivalent, but which has scale >= 0.
		 *
		 * @param n
		 *            number of placed the decimal point has to be moved.
		 * @return {@code this * 10^n}.
		 */
		public BigDecimal MovePointRight(int n) {
			return MovePoint(_scale - (long)n);
		}

		/**
		 * Returns a new {@code BigDecimal} whose value is {@code this} 10^{@code n}.
		 * The scale of the result is {@code this.scale()} - {@code n}.
		 * The precision of the result is the precision of {@code this}.
		 * <p>
		 * This method has the same effect as {@link #movePointRight}, except that
		 * the precision is not changed.
		 *
		 * @param n
		 *            number of places the decimal point has to be moved.
		 * @return {@code this * 10^n}
		 */
		public BigDecimal ScaleByPowerOfTen(int n) {
			long newScale = _scale - (long)n;
			if (_bitLength < 64) {
				//Taking care when a 0 is to be scaled
				if (smallValue == 0) {
					return GetZeroScaledBy(newScale);
				}
				return ValueOf(smallValue, ToIntScale(newScale));
			}
			return new BigDecimal(GetUnscaledValue(), ToIntScale(newScale));
		}

		/**
		 * Returns a new {@code BigDecimal} instance with the same value as {@code
		 * this} but with a unscaled value where the trailing zeros have been
		 * removed. If the unscaled value of {@code this} has n trailing zeros, then
		 * the scale and the precision of the result has been reduced by n.
		 *
		 * @return a new {@code BigDecimal} instance equivalent to this where the
		 *         trailing zeros of the unscaled value have been removed.
		 */
		public BigDecimal StripTrailingZeros() {
			int i = 1; // 1 <= i <= 18
			int lastPow = TenPow.Length - 1;
			long newScale = _scale;

			if (isZero()) {
				return new BigDecimal("0");
			}
			BigInteger strippedBI = GetUnscaledValue();
			BigInteger[] quotAndRem;

			// while the number is even...
			while (!strippedBI.TestBit(0)) {
				// To divide by 10^i
				quotAndRem = strippedBI.DivideAndRemainder(TenPow[i]);
				// To look the remainder
				if (quotAndRem[1].Signum() == 0) {
					// To adjust the scale
					newScale -= i;
					if (i < lastPow) {
						// To set to the next power
						i++;
					}
					strippedBI = quotAndRem[0];
				} else {
					if (i == 1) {
						// 'this' has no more trailing zeros
						break;
					}
					// To set to the smallest power of ten
					i = 1;
				}
			}
			return new BigDecimal(strippedBI, ToIntScale(newScale));
		}

		/**
		 * Compares this {@code BigDecimal} with {@code val}. Returns one of the
		 * three values {@code 1}, {@code 0}, or {@code -1}. The method behaves as
		 * if {@code this.subtract(val)} is computed. If this difference is > 0 then
		 * 1 is returned, if the difference is < 0 then -1 is returned, and if the
		 * difference is 0 then 0 is returned. This means, that if two decimal
		 * instances are compared which are equal in value but differ in scale, then
		 * these two instances are considered as equal.
		 *
		 * @param val
		 *            value to be compared with {@code this}.
		 * @return {@code 1} if {@code this > val}, {@code -1} if {@code this < val},
		 *         {@code 0} if {@code this == val}.
		 * @throws NullPointerException
		 *             if {@code val == null}.
		 */
		public int CompareTo(BigDecimal val) {
			int thisSign = Signum();
			int valueSign = val.Signum();

			if (thisSign == valueSign) {
				if (this._scale == val._scale && this._bitLength < 64 && val._bitLength < 64) {
					return (smallValue < val.smallValue) ? -1 : (smallValue > val.smallValue) ? 1 : 0;
				}
				long diffScale = (long)this._scale - val._scale;
				int diffPrecision = this.AproxPrecision() - val.AproxPrecision();
				if (diffPrecision > diffScale + 1) {
					return thisSign;
				} else if (diffPrecision < diffScale - 1) {
					return -thisSign;
				} else {// thisSign == val.signum()  and  diffPrecision is aprox. diffScale
					BigInteger thisUnscaled = this.GetUnscaledValue();
					BigInteger valUnscaled = val.GetUnscaledValue();
					// If any of both precision is bigger, append zeros to the shorter one
					if (diffScale < 0) {
						thisUnscaled = thisUnscaled.Multiply(Multiplication.powerOf10(-diffScale));
					} else if (diffScale > 0) {
						valUnscaled = valUnscaled.Multiply(Multiplication.powerOf10(diffScale));
					}
					return thisUnscaled.CompareTo(valUnscaled);
				}
			} else if (thisSign < valueSign) {
				return -1;
			} else {
				return 1;
			}
		}

		/**
		 * Returns {@code true} if {@code x} is a {@code BigDecimal} instance and if
		 * this instance is equal to this big decimal. Two big decimals are equal if
		 * their unscaled value and their scale is equal. For example, 1.0
		 * (10*10^(-1)) is not equal to 1.00 (100*10^(-2)). Similarly, zero
		 * instances are not equal if their scale differs.
		 *
		 * @param x
		 *            object to be compared with {@code this}.
		 * @return true if {@code x} is a {@code BigDecimal} and {@code this == x}.
		 */
		public override bool Equals(Object x) {
			if (this == x) {
				return true;
			}
			if (x is BigDecimal) {
				BigDecimal x1 = (BigDecimal)x;
				return x1._scale == _scale
					   && (_bitLength < 64 ? (x1.smallValue == smallValue)
						: intVal.Equals(x1.intVal));


			}
			return false;
		}

		/**
		 * Returns the minimum of this {@code BigDecimal} and {@code val}.
		 *
		 * @param val
		 *            value to be used to compute the minimum with this.
		 * @return {@code min(this, val}.
		 * @throws NullPointerException
		 *             if {@code val == null}.
		 */
		public BigDecimal Min(BigDecimal val) {
			return ((CompareTo(val) <= 0) ? this : val);
		}

		/**
		 * Returns the maximum of this {@code BigDecimal} and {@code val}.
		 *
		 * @param val
		 *            value to be used to compute the maximum with this.
		 * @return {@code max(this, val}.
		 * @throws NullPointerException
		 *             if {@code val == null}.
		 */
		public BigDecimal Max(BigDecimal val) {
			return ((CompareTo(val) >= 0) ? this : val);
		}

		/**
		 * Returns a hash code for this {@code BigDecimal}.
		 *
		 * @return hash code for {@code this}.
		 */
		public override int GetHashCode() {
			if (hashCode != 0) {
				return hashCode;
			}
			if (_bitLength < 64) {
				hashCode = (int)(smallValue & 0xffffffff);
				hashCode = 33 * hashCode + (int)((smallValue >> 32) & 0xffffffff);
				hashCode = 17 * hashCode + _scale;
				return hashCode;
			}
			hashCode = 17 * intVal.GetHashCode() + _scale;
			return hashCode;
		}

		/**
		 * Returns a canonical string representation of this {@code BigDecimal}. If
		 * necessary, scientific notation is used. This representation always prints
		 * all significant digits of this value.
		 * <p>
		 * If the scale is negative or if {@code scale - precision >= 6} then
		 * scientific notation is used.
		 *
		 * @return a string representation of {@code this} in scientific notation if
		 *         necessary.
		 */
		public override String ToString() {
			if (toStringImage != null) {
				return toStringImage;
			}
			return ToStringInternal();
		}

		private string ToStringInternal() {
			if (_bitLength < 32) {
				toStringImage = Conversion.toDecimalScaledString(smallValue, _scale);
				return toStringImage;
			}
			String intString = GetUnscaledValue().ToString();
			if (_scale == 0) {
				return intString;
			}
			int begin = (GetUnscaledValue().Signum() < 0) ? 2 : 1;
			int end = intString.Length;
			long exponent = -(long)_scale + end - begin;
			StringBuilder result = new StringBuilder();

			result.Append(intString);
			if ((_scale > 0) && (exponent >= -6)) {
				if (exponent >= 0) {
					result.Insert(end - _scale, '.');
				} else {
					result.Insert(begin - 1, "0."); //$NON-NLS-1$
					result.Insert(begin + 1, ChZeros, 0, -(int)exponent - 1);
				}
			} else {
				if (end - begin >= 1) {
					result.Insert(begin, '.');
					end++;
				}
				result.Insert(end, 'E');
				if (exponent > 0) {
					result.Insert(++end, '+');
				}
				result.Insert(++end, Convert.ToString(exponent));
			}
			toStringImage = result.ToString();
			return toStringImage;
		}

		/**
		 * Returns a string representation of this {@code BigDecimal}. This
		 * representation always prints all significant digits of this value.
		 * <p>
		 * If the scale is negative or if {@code scale - precision >= 6} then
		 * engineering notation is used. Engineering notation is similar to the
		 * scientific notation except that the exponent is made to be a multiple of
		 * 3 such that the integer part is >= 1 and < 1000.
		 *
		 * @return a string representation of {@code this} in engineering notation
		 *         if necessary.
		 */
		public String ToEngineeringString() {
			String intString = GetUnscaledValue().ToString();
			if (_scale == 0) {
				return intString;
			}
			int begin = (GetUnscaledValue().Signum() < 0) ? 2 : 1;
			int end = intString.Length;
			long exponent = -(long)_scale + end - begin;
			StringBuilder result = new StringBuilder(intString);

			if ((_scale > 0) && (exponent >= -6)) {
				if (exponent >= 0) {
					result.Insert(end - _scale, '.');
				} else {
					result.Insert(begin - 1, "0."); //$NON-NLS-1$
					result.Insert(begin + 1, ChZeros, 0, -(int)exponent - 1);
				}
			} else {
				int delta = end - begin;
				int rem = (int)(exponent % 3);

				if (rem != 0) {
					// adjust exponent so it is a multiple of three
					if (GetUnscaledValue().Signum() == 0) {
						// zero value
						rem = (rem < 0) ? -rem : 3 - rem;
						exponent += rem;
					} else {
						// nonzero value
						rem = (rem < 0) ? rem + 3 : rem;
						exponent -= rem;
						begin += rem;
					}
					if (delta < 3) {
						for (int i = rem - delta; i > 0; i--) {
							result.Insert(end++, '0');
						}
					}
				}
				if (end - begin >= 1) {
					result.Insert(begin, '.');
					end++;
				}
				if (exponent != 0) {
					result.Insert(end, 'E');
					if (exponent > 0) {
						result.Insert(++end, '+');
					}
					result.Insert(++end, Convert.ToString(exponent));
				}
			}
			return result.ToString();
		}

		/**
		 * Returns a string representation of this {@code BigDecimal}. No scientific
		 * notation is used. This methods adds zeros where necessary.
		 * <p>
		 * If this string representation is used to create a new instance, this
		 * instance is generally not identical to {@code this} as the precision
		 * changes.
		 * <p>
		 * {@code x.equals(new BigDecimal(x.toPlainString())} usually returns
		 * {@code false}.
		 * <p>
		 * {@code x.compareTo(new BigDecimal(x.toPlainString())} returns {@code 0}.
		 *
		 * @return a string representation of {@code this} without exponent part.
		 */
		public String ToPlainString() {
			String intStr = GetUnscaledValue().ToString();
			if ((_scale == 0) || ((isZero()) && (_scale < 0))) {
				return intStr;
			}
			int begin = (Signum() < 0) ? 1 : 0;
			int delta = _scale;
			// We take space for all digits, plus a possible decimal point, plus 'scale'
			StringBuilder result = new StringBuilder(intStr.Length + 1 + System.Math.Abs(_scale));

			if (begin == 1) {
				// If the number is negative, we insert a '-' CharHelper at front 
				result.Append('-');
			}
			if (_scale > 0) {
				delta -= (intStr.Length - begin);
				if (delta >= 0) {
					result.Append("0."); //$NON-NLS-1$
					// To append zeros after the decimal point
					for (; delta > ChZeros.Length; delta -= ChZeros.Length) {
						result.Append(ChZeros);
					}
					result.Append(ChZeros, 0, delta);
					result.Append(intStr.Substring(begin));
				} else {
					delta = begin - delta;
					result.Append(intStr.Substring(begin, delta - begin));
					result.Append('.');
					result.Append(intStr.Substring(delta));
				}
			} else {// (scale <= 0)
				result.Append(intStr.Substring(begin));
				// To append trailing zeros
				for (; delta < -ChZeros.Length; delta += ChZeros.Length) {
					result.Append(ChZeros);
				}
				result.Append(ChZeros, 0, -delta);
			}
			return result.ToString();
		}

		/**
		 * Returns this {@code BigDecimal} as a big integer instance. A fractional
		 * part is discarded.
		 *
		 * @return this {@code BigDecimal} as a big integer instance.
		 */
		public BigInteger ToBigInteger() {
			if ((_scale == 0) || (isZero())) {
				return GetUnscaledValue();
			} else if (_scale < 0) {
				return GetUnscaledValue().Multiply(Multiplication.powerOf10(-(long)_scale));
			} else {// (scale > 0)
				return GetUnscaledValue().Divide(Multiplication.powerOf10(_scale));
			}
		}

		/**
		 * Returns this {@code BigDecimal} as a big integer instance if it has no
		 * fractional part. If this {@code BigDecimal} has a fractional part, i.e.
		 * if rounding would be necessary, an {@code ArithmeticException} is thrown.
		 *
		 * @return this {@code BigDecimal} as a big integer value.
		 * @throws ArithmeticException
		 *             if rounding is necessary.
		 */
		public BigInteger ToBigIntegerExact() {
			if ((_scale == 0) || (isZero())) {
				return GetUnscaledValue();
			} else if (_scale < 0) {
				return GetUnscaledValue().Multiply(Multiplication.powerOf10(-(long)_scale));
			} else {// (scale > 0)
				BigInteger[] integerAndFraction;
				// An optimization before do a heavy division
				if ((_scale > AproxPrecision()) || (_scale > GetUnscaledValue().LowestSetBit)) {
					// math.08=Rounding necessary
					throw new ArithmeticException(SR.GetString("math.08")); //$NON-NLS-1$
				}
				integerAndFraction = GetUnscaledValue().DivideAndRemainder(Multiplication.powerOf10(_scale));
				if (integerAndFraction[1].Signum() != 0) {
					// It exists a non-zero fractional part 
					// math.08=Rounding necessary
					throw new ArithmeticException(SR.GetString("math.08")); //$NON-NLS-1$
				}
				return integerAndFraction[0];
			}
		}

		/**
		 * Returns this {@code BigDecimal} as an long value. Any fractional part is
		 * discarded. If the integral part of {@code this} is too big to be
		 * represented as an long, then {@code this} % 2^64 is returned.
		 *
		 * @return this {@code BigDecimal} as a long value.
		 */
		public long ToInt64() {
			/* If scale <= -64 there are at least 64 trailing bits zero in 10^(-scale).
			 * If the scale is positive and very large the long value could be zero. */
			return ((_scale <= -64) || (_scale > AproxPrecision()) ? 0L : ToBigInteger().ToInt64());
		}

		/**
		 * Returns this {@code BigDecimal} as a long value if it has no fractional
		 * part and if its value fits to the int range ([-2^{63}..2^{63}-1]). If
		 * these conditions are not met, an {@code ArithmeticException} is thrown.
		 *
		 * @return this {@code BigDecimal} as a long value.
		 * @throws ArithmeticException
		 *             if rounding is necessary or the number doesn't fit in a long.
		 */
		public long ToInt64Exact() {
			return ValueExact(64);
		}

		/**
		 * Returns this {@code BigDecimal} as an int value. Any fractional part is
		 * discarded. If the integral part of {@code this} is too big to be
		 * represented as an int, then {@code this} % 2^32 is returned.
		 *
		 * @return this {@code BigDecimal} as a int value.
		 */
		public int ToInt32() {
			/* If scale <= -32 there are at least 32 trailing bits zero in 10^(-scale).
			 * If the scale is positive and very large the long value could be zero. */
			return ((_scale <= -32) || (_scale > AproxPrecision()) ? 0 : ToBigInteger().ToInt32());
		}

		/**
		 * Returns this {@code BigDecimal} as a int value if it has no fractional
		 * part and if its value fits to the int range ([-2^{31}..2^{31}-1]). If
		 * these conditions are not met, an {@code ArithmeticException} is thrown.
		 *
		 * @return this {@code BigDecimal} as a int value.
		 * @throws ArithmeticException
		 *             if rounding is necessary or the number doesn't fit in a int.
		 */
		public int ToInt32Exact() {
			return (int)ValueExact(32);
		}

		/**
		 * Returns this {@code BigDecimal} as a short value if it has no fractional
		 * part and if its value fits to the short range ([-2^{15}..2^{15}-1]). If
		 * these conditions are not met, an {@code ArithmeticException} is thrown.
		 *
		 * @return this {@code BigDecimal} as a short value.
		 * @throws ArithmeticException
		 *             if rounding is necessary of the number doesn't fit in a
		 *             short.
		 */
		public short ToInt16Exact() {
			return (short)ValueExact(16);
		}

		/**
		 * Returns this {@code BigDecimal} as a byte value if it has no fractional
		 * part and if its value fits to the byte range ([-128..127]). If these
		 * conditions are not met, an {@code ArithmeticException} is thrown.
		 *
		 * @return this {@code BigDecimal} as a byte value.
		 * @throws ArithmeticException
		 *             if rounding is necessary or the number doesn't fit in a byte.
		 */
		public byte ToByteExact() {
			return (byte)ValueExact(8);
		}

		/**
		 * Returns this {@code BigDecimal} as a float value. If {@code this} is too
		 * big to be represented as an float, then {@code Float.POSITIVE_INFINITY}
		 * or {@code Float.NEGATIVE_INFINITY} is returned.
		 * <p>
		 * Note, that if the unscaled value has more than 24 significant digits,
		 * then this decimal cannot be represented exactly in a float variable. In
		 * this case the result is rounded.
		 * <p>
		 * For example, if the instance {@code x1 = new BigDecimal("0.1")} cannot be
		 * represented exactly as a float, and thus {@code x1.equals(new
		 * BigDecimal(x1.folatValue())} returns {@code false} for this case.
		 * <p>
		 * Similarly, if the instance {@code new BigDecimal(16777217)} is converted
		 * to a float, the result is {@code 1.6777216E}7.
		 *
		 * @return this {@code BigDecimal} as a float value.
		 */
		public float ToSingle() {
			/* A similar code like in ToDouble() could be repeated here,
			 * but this simple implementation is quite efficient. */
			float floatResult = Signum();
			long powerOfTwo = this._bitLength - (long)(_scale / Log10_2);
			if ((powerOfTwo < -149) || (floatResult == 0.0f)) {
				// Cases which 'this' is very small
				floatResult *= 0.0f;
			} else if (powerOfTwo > 129) {
				// Cases which 'this' is very large
				floatResult *= Single.PositiveInfinity;
			} else {
				floatResult = (float)ToDouble();
			}
			return floatResult;
		}

		/**
		 * Returns this {@code BigDecimal} as a double value. If {@code this} is too
		 * big to be represented as an float, then {@code Double.POSITIVE_INFINITY}
		 * or {@code Double.NEGATIVE_INFINITY} is returned.
		 * <p>
		 * Note, that if the unscaled value has more than 53 significant digits,
		 * then this decimal cannot be represented exactly in a double variable. In
		 * this case the result is rounded.
		 * <p>
		 * For example, if the instance {@code x1 = new BigDecimal("0.1")} cannot be
		 * represented exactly as a double, and thus {@code x1.equals(new
		 * BigDecimal(x1.ToDouble())} returns {@code false} for this case.
		 * <p>
		 * Similarly, if the instance {@code new BigDecimal(9007199254740993L)} is
		 * converted to a double, the result is {@code 9.007199254740992E15}.
		 * <p>
		 *
		 * @return this {@code BigDecimal} as a double value.
		 */
		public double ToDouble() {
			int sign = Signum();
			int exponent = 1076; // bias + 53
			int lowestSetBit;
			int discardedSize;
			long powerOfTwo = this._bitLength - (long)(_scale / Log10_2);
			long bits; // IEEE-754 Standard
			long tempBits; // for temporal calculations     
			BigInteger mantisa;

			if ((powerOfTwo < -1074) || (sign == 0)) {
				// Cases which 'this' is very small            
				return (sign * 0.0d);
			} else if (powerOfTwo > 1025) {
				// Cases which 'this' is very large            
				return (sign * Double.PositiveInfinity);
			}
			mantisa = GetUnscaledValue().Abs();
			// Let be:  this = [u,s], with s > 0
			if (_scale <= 0) {
				// mantisa = abs(u) * 10^s
				mantisa = mantisa.Multiply(Multiplication.powerOf10(-_scale));
			} else {
				// (scale > 0)
				BigInteger[] quotAndRem;
				BigInteger powerOfTen = Multiplication.powerOf10(_scale);
				int k = 100 - (int)powerOfTwo;
				int compRem;

				if (k > 0) {
					/* Computing (mantisa * 2^k) , where 'k' is a enough big
					 * power of '2' to can divide by 10^s */
					mantisa = mantisa.ShiftLeft(k);
					exponent -= k;
				}
				// Computing (mantisa * 2^k) / 10^s
				quotAndRem = mantisa.DivideAndRemainder(powerOfTen);
				// To check if the fractional part >= 0.5
				compRem = quotAndRem[1].ShiftLeftOneBit().CompareTo(powerOfTen);
				// To add two rounded bits at end of mantisa
				mantisa = quotAndRem[0].ShiftLeft(2).Add(
					BigInteger.ValueOf((compRem * (compRem + 3)) / 2 + 1));
				exponent -= 2;
			}
			lowestSetBit = mantisa.LowestSetBit;
			discardedSize = mantisa.BitLength - 54;
			if (discardedSize > 0) {
				// (n > 54)
				// mantisa = (abs(u) * 10^s) >> (n - 54)
				bits = mantisa.ShiftRight(discardedSize).ToInt64();
				tempBits = bits;
				// #bits = 54, to check if the discarded fraction produces a carry             
				if ((((bits & 1) == 1) && (lowestSetBit < discardedSize))
					|| ((bits & 3) == 3)) {
					bits += 2;
				}
			} else {
				// (n <= 54)
				// mantisa = (abs(u) * 10^s) << (54 - n)                
				bits = mantisa.ToInt64() << -discardedSize;
				tempBits = bits;
				// #bits = 54, to check if the discarded fraction produces a carry:
				if ((bits & 3) == 3) {
					bits += 2;
				}
			}
			// Testing bit 54 to check if the carry creates a new binary digit
			if ((bits & 0x40000000000000L) == 0) {
				// To drop the last bit of mantisa (first discarded)
				bits >>= 1;
				// exponent = 2^(s-n+53+bias)
				exponent += discardedSize;
			} else {
				// #bits = 54
				bits >>= 2;
				exponent += discardedSize + 1;
			}
			// To test if the 53-bits number fits in 'double'            
			if (exponent > 2046) {
				// (exponent - bias > 1023)
				return (sign * Double.PositiveInfinity);
			}
			if (exponent <= 0) {
				// (exponent - bias <= -1023)
				// Denormalized numbers (having exponent == 0)
				if (exponent < -53) {
					// exponent - bias < -1076
					return (sign * 0.0d);
				}
				// -1076 <= exponent - bias <= -1023 
				// To discard '- exponent + 1' bits
				bits = tempBits >> 1;
				tempBits = bits & Utils.URShift(-1L, (63 + exponent));
				bits >>= (-exponent);
				// To test if after discard bits, a new carry is generated
				if (((bits & 3) == 3) || (((bits & 1) == 1) && (tempBits != 0)
										  && (lowestSetBit < discardedSize))) {
					bits += 1;
				}
				exponent = 0;
				bits >>= 1;
			}
			// Construct the 64 double bits: [sign(1), exponent(11), mantisa(52)]
			// bits = (long)((ulong)sign & 0x8000000000000000L) | ((long)exponent << 52) | (bits & 0xFFFFFFFFFFFFFL);
			bits = sign & Int64.MinValue | ((long)exponent << 52) | (bits & 0xFFFFFFFFFFFFFL);
			return BitConverter.Int64BitsToDouble(bits);
		}

		/**
		 * Returns the unit in the last place (ULP) of this {@code BigDecimal}
		 * instance. An ULP is the distance to the nearest big decimal with the same
		 * precision.
		 * <p>
		 * The amount of a rounding error in the evaluation of a floating-point
		 * operation is often expressed in ULPs. An error of 1 ULP is often seen as
		 * a tolerable error.
		 * <p>
		 * For class {@code BigDecimal}, the ULP of a number is simply 10^(-scale).
		 * <p>
		 * For example, {@code new BigDecimal(0.1).ulp()} returns {@code 1E-55}.
		 *
		 * @return unit in the last place (ULP) of this {@code BigDecimal} instance.
		 */
		public BigDecimal Ulp() {
			return ValueOf(1, _scale);
		}

		/* Private Methods */

		/**
		 * It does all rounding work of the public method
		 * {@code round(MathContext)}, performing an inplace rounding
		 * without creating a new object.
		 *
		 * @param mc
		 *            the {@code MathContext} for perform the rounding.
		 * @see #round(MathContext)
		 */
		private void InplaceRound(MathContext mc) {
			int mcPrecision = mc.Precision;
			if (AproxPrecision() - mcPrecision <= 0 || mcPrecision == 0) {
				return;
			}
			int discardedPrecision = Precision - mcPrecision;
			// If no rounding is necessary it returns immediately
			if ((discardedPrecision <= 0)) {
				return;
			}
			// When the number is small perform an efficient rounding
			if (this._bitLength < 64) {
				SmallRound(mc, discardedPrecision);
				return;
			}
			// Getting the integer part and the discarded fraction
			BigInteger sizeOfFraction = Multiplication.powerOf10(discardedPrecision);
			BigInteger[] integerAndFraction = GetUnscaledValue().DivideAndRemainder(sizeOfFraction);
			long newScale = (long)_scale - discardedPrecision;
			int compRem;
			BigDecimal tempBD;
			// If the discarded fraction is non-zero, perform rounding
			if (integerAndFraction[1].Signum() != 0) {
				// To check if the discarded fraction >= 0.5
				compRem = (integerAndFraction[1].Abs().ShiftLeftOneBit().CompareTo(sizeOfFraction));
				// To look if there is a carry
				compRem = RoundingBehavior(integerAndFraction[0].TestBit(0) ? 1 : 0,
						integerAndFraction[1].Signum() * (5 + compRem),
						mc.RoundingMode);
				if (compRem != 0) {
					integerAndFraction[0] = integerAndFraction[0].Add(BigInteger.ValueOf(compRem));
				}
				tempBD = new BigDecimal(integerAndFraction[0]);
				// If after to add the increment the precision changed, we normalize the size
				if (tempBD.Precision > mcPrecision) {
					integerAndFraction[0] = integerAndFraction[0].Divide(BigInteger.Ten);
					newScale--;
				}
			}
			// To update all internal fields
			_scale = ToIntScale(newScale);
			_precision = mcPrecision;
			SetUnscaledValue(integerAndFraction[0]);
		}

		private static int LongCompareTo(long value1, long value2) {
			return value1 > value2 ? 1 : (value1 < value2 ? -1 : 0);
		}
		/**
		 * This method implements an efficient rounding for numbers which unscaled
		 * value fits in the type {@code long}.
		 *
		 * @param mc
		 *            the context to use
		 * @param discardedPrecision
		 *            the number of decimal digits that are discarded
		 * @see #round(MathContext)
		 */
		private void SmallRound(MathContext mc, int discardedPrecision) {
			long sizeOfFraction = LongTenPow[discardedPrecision];
			long newScale = (long)_scale - discardedPrecision;
			long unscaledVal = smallValue;
			// Getting the integer part and the discarded fraction
			long integer = unscaledVal / sizeOfFraction;
			long fraction = unscaledVal % sizeOfFraction;
			int compRem;
			// If the discarded fraction is non-zero perform rounding
			if (fraction != 0) {
				// To check if the discarded fraction >= 0.5
				compRem = LongCompareTo(System.Math.Abs(fraction) << 1, sizeOfFraction);
				// To look if there is a carry
				integer += RoundingBehavior(((int)integer) & 1, System.Math.Sign(fraction) * (5 + compRem),
						mc.RoundingMode);
				// If after to add the increment the precision changed, we normalize the size
				if (System.Math.Log10(System.Math.Abs(integer)) >= mc.Precision) {
					integer /= 10;
					newScale--;
				}
			}
			// To update all internal fields
			_scale = ToIntScale(newScale);
			_precision = mc.Precision;
			smallValue = integer;
			_bitLength = BitLength(integer);
			intVal = null;
		}

		/**
		 * Return an increment that can be -1,0 or 1, depending of
		 * {@code roundingMode}.
		 *
		 * @param parityBit
		 *            can be 0 or 1, it's only used in the case
		 *            {@code HALF_EVEN}
		 * @param fraction
		 *            the mantisa to be analyzed
		 * @param roundingMode
		 *            the type of rounding
		 * @return the carry propagated after rounding
		 */
		private static int RoundingBehavior(int parityBit, int fraction, RoundingMode roundingMode) {
			int increment = 0; // the carry after rounding

			switch (roundingMode) {
				case RoundingMode.Unnecessary:
					if (fraction != 0) {
						// math.08=Rounding necessary
						throw new ArithmeticException(SR.GetString("math.08")); //$NON-NLS-1$
					}
					break;
				case RoundingMode.Up:
					increment = System.Math.Sign(fraction);
					break;
				case RoundingMode.Down:
					break;
				case RoundingMode.Ceiling:
					increment = System.Math.Max(System.Math.Sign(fraction), 0);
					break;
				case RoundingMode.Floor:
					increment = System.Math.Min(System.Math.Sign(fraction), 0);
					break;
				case RoundingMode.HalfUp:
					if (System.Math.Abs(fraction) >= 5) {
						increment = System.Math.Sign(fraction);
					}
					break;
				case RoundingMode.HalfDown:
					if (System.Math.Abs(fraction) > 5) {
						increment = System.Math.Sign(fraction);
					}
					break;
				case RoundingMode.HalfEven:
					if (System.Math.Abs(fraction) + parityBit > 5) {
						increment = System.Math.Sign(fraction);
					}
					break;
			}
			return increment;
		}

		/**
		 * If {@code intVal} has a fractional part throws an exception,
		 * otherwise it counts the number of bits of value and checks if it's out of
		 * the range of the primitive type. If the number fits in the primitive type
		 * returns this number as {@code long}, otherwise throws an
		 * exception.
		 *
		 * @param bitLengthOfType
		 *            number of bits of the type whose value will be calculated
		 *            exactly
		 * @return the exact value of the integer part of {@code BigDecimal}
		 *         when is possible
		 * @throws ArithmeticException when rounding is necessary or the
		 *             number don't fit in the primitive type
		 */
		private long ValueExact(int bitLengthOfType) {
			BigInteger bigInteger = ToBigIntegerExact();

			if (bigInteger.BitLength < bitLengthOfType) {
				// It fits in the primitive type
				return bigInteger.ToInt64();
			}
			// math.08=Rounding necessary
			throw new ArithmeticException(SR.GetString("math.08")); //$NON-NLS-1$
		}

		/**
		 * If the precision already was calculated it returns that value, otherwise
		 * it calculates a very good approximation efficiently . Note that this
		 * value will be {@code precision()} or {@code precision()-1}
		 * in the worst case.
		 *
		 * @return an approximation of {@code precision()} value
		 */
		private int AproxPrecision() {
			return ((_precision > 0) ? _precision : (int)((_bitLength - 1) * Log10_2)) + 1;
		}

		/**
	 * It tests if a scale of type {@code long} fits in 32 bits. It
	 * returns the same scale being casted to {@code int} type when is
	 * possible, otherwise throws an exception.
	 *
	 * @param longScale
	 *            a 64 bit scale
	 * @return a 32 bit scale when is possible
	 * @throws ArithmeticException when {@code scale} doesn't
	 *             fit in {@code int} type
	 * @see #scale
	 */
		private static int ToIntScale(long longScale) {
			if (longScale < Int32.MinValue) {
				// math.09=Overflow
				throw new ArithmeticException(SR.GetString("math.09")); //$NON-NLS-1$
			} else if (longScale > Int32.MaxValue) {
				// math.0A=Underflow
				throw new ArithmeticException(SR.GetString("math.0A")); //$NON-NLS-1$
			} else {
				return (int)longScale;
			}
		}

		/**
		 * It returns the value 0 with the most approximated scale of type
		 * {@code int}. if {@code longScale > Integer.MAX_VALUE} the
		 * scale will be {@code Integer.MAX_VALUE}; if
		 * {@code longScale < Integer.MIN_VALUE} the scale will be
		 * {@code Integer.MIN_VALUE}; otherwise {@code longScale} is
		 * casted to the type {@code int}.
		 *
		 * @param longScale
		 *            the scale to which the value 0 will be scaled.
		 * @return the value 0 scaled by the closer scale of type {@code int}.
		 * @see #scale
		 */
		private static BigDecimal GetZeroScaledBy(long longScale) {
			if (longScale == (int)longScale) {
				return ValueOf(0, (int)longScale);
			}
			if (longScale >= 0) {
				return new BigDecimal(0, Int32.MaxValue);
			}
			return new BigDecimal(0, Int32.MinValue);
		}

		/**
		 * Assignes all transient fields upon deserialization of a
		 * {@code BigDecimal} instance (bitLength and smallValue). The transient
		 * field precision is assigned lazily.
		 */
		/*
	private void readObject(ObjectInputStream in) {
		in.defaultReadObject();

		this.bitLength = intVal.bitLength();
		if (this.bitLength < 64) {
			this.smallValue = intVal.ToInt64();
		}
	}
		 */

		/**
		 * Prepares this {@code BigDecimal} for serialization, i.e. the
		 * non-transient field {@code intVal} is assigned.
		 */
		/*
	private void writeObject(ObjectOutputStream out) {
		getUnscaledValue();
		out.defaultWriteObject();
	}
		 */

		private BigInteger GetUnscaledValue() {
			if (intVal == null)
				intVal = BigInteger.ValueOf(smallValue);
			return intVal;
		}

		private void SetUnscaledValue(BigInteger unscaledValue) {
			intVal = unscaledValue;
			_bitLength = unscaledValue.BitLength;
			if (_bitLength < 64) {
				smallValue = unscaledValue.ToInt64();
			}
		}

		private static int BitLength(long smallValue) {
			if (smallValue < 0) {
				smallValue = ~smallValue;
			}
			return 64 - Utils.numberOfLeadingZeros(smallValue);
		}

		private static int BitLength(int smallValue) {
			if (smallValue < 0) {
				smallValue = ~smallValue;
			}
			return 32 - Utils.numberOfLeadingZeros(smallValue);
		}

		/*
		[OnSerializing]
		internal void BeforeSerialization(StreamingContext context) {
			GetUnscaledValue();
		}

		[OnDeserialized]
		internal void AfterDeserialization(StreamingContext context) {
			intVal.AfterDeserialization(context);
			_bitLength = intVal.BitLength;
			if (_bitLength < 64) {
				smallValue = intVal.ToInt64();
			}
		}
		*/

		private BigDecimal(SerializationInfo info, StreamingContext context) {
			intVal = (BigInteger) info.GetValue("intVal", typeof(BigInteger));
			_scale = info.GetInt32("scale");
			_bitLength = intVal.BitLength;
			if (_bitLength < 64) {
				smallValue = intVal.ToInt64();
			}
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			GetUnscaledValue();
			info.AddValue("intVal", intVal, typeof(BigInteger));
			info.AddValue("scale", _scale);
		}

		#region Implementation of IConvertible

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			int value = ToInt32();
			if (value == 1)
				return true;
			if (value == 0)
				return false;
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			short value = ToInt16Exact();
			return (char) value;
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			int value = ToInt32();
			if (value > Byte.MaxValue || value < Byte.MinValue)
				throw new InvalidCastException();
			return (byte) value;
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16Exact();
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
				return (this as IConvertible).ToByte(provider);
			if (conversionType == typeof(short))
				return ToInt16Exact();
			if (conversionType == typeof(int))
				return ToInt32();
			if (conversionType == typeof(long))
				return ToInt64();
			if (conversionType == typeof(float))
				return ToSingle();
			if (conversionType == typeof(double))
				return ToDouble();

			if (conversionType == typeof(BigInteger))
				return ToBigInteger();

			throw new NotSupportedException();
		}

		#endregion

		#region Operators

		public static BigDecimal operator +(BigDecimal a, BigDecimal b) {
			return a.Add(b);
		}

		public static BigDecimal operator -(BigDecimal a, BigDecimal b) {
			return a.Subtract(b);
		}

		public static BigDecimal operator /(BigDecimal a, BigDecimal b) {
			return a.Divide(b);
		}

		public static BigDecimal operator %(BigDecimal a, BigDecimal b) {
			return a.Remainder(b);
		}

		public static BigDecimal operator *(BigDecimal a, BigDecimal b) {
			return a.Multiply(b);
		}

		public static BigDecimal operator +(BigDecimal a) {
			return a.Plus();
		}

		public static BigDecimal operator -(BigDecimal a) {
			return a.Negate();
		}

		public static bool operator ==(BigDecimal a, BigDecimal b) {
			if ((object)a == null && (object)b == null)
				return true;
			if ((object)a == null)
				return false;
			return a.Equals(b);
		}

		public static bool operator !=(BigDecimal a, BigDecimal b) {
			return !(a == b);
		}

		public static bool operator >(BigDecimal a, BigDecimal b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator <(BigDecimal a, BigDecimal b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator >=(BigDecimal a, BigDecimal b) {
			return a == b || a > b;
		}

		public static bool operator <=(BigDecimal a, BigDecimal b) {
			return a == b || a < b;
		}

		#endregion

		#region Implicit Operators

		public static implicit operator Int16(BigDecimal d) {
			return d.ToInt16Exact();
		}

		public static implicit operator Int32(BigDecimal d) {
			return d.ToInt32();
		}

		public static implicit operator Int64(BigDecimal d) {
			return d.ToInt64();
		}

		public static implicit operator Single(BigDecimal d) {
			return d.ToSingle();
		}

		public static implicit operator Double(BigDecimal d) {
			return d.ToDouble();
		}

		public static implicit operator BigInteger(BigDecimal d) {
			return d.ToBigInteger();
		}

		public static implicit operator String(BigDecimal d) {
			return d.ToString();
		}

		public static implicit operator BigDecimal(long value) {
			return new BigDecimal(value);
		}

		public static implicit operator BigDecimal(double value) {
			return new BigDecimal(value);
		}

		public static implicit operator BigDecimal(int value) {
			return new BigDecimal(value);
		}

		public static implicit operator BigDecimal(BigInteger value) {
			return new BigDecimal(value);
		}

		#endregion
	}
}