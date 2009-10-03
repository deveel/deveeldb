// 
//  BigInteger.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Per Bothner <per@bothner.com>
//       Warren Levy <warrenl@cygnus.com>
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
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Math {
    /// <summary>
    /// 
    /// </summary>
    [Serializable]
    public sealed class BigInteger : Number {
        #region ctors
        private BigInteger() {
        }

        private BigInteger(int value) {
            ival = value;
        }

        public BigInteger(string value, int radix) {
            BigInteger result = ValueOf(value, radix);
            ival = result.ival;
            words = result.words;
        }

        public BigInteger(string value)
            : this(value, 10) {
        }

        /* Create a new (non-shared) BigInteger, and initialize from a byte array. */
        public BigInteger(byte[] value) {
            if (value == null || value.Length < 1)
                throw new ArgumentNullException();

            words = ConvertToIntArray(value, value[0] < 0 ? -1 : 0);
            BigInteger result = Make(words, words.Length);
            this.ival = result.ival;
            this.words = result.words;
        }

        public BigInteger(int signum, byte[] magnitude) {
            if (magnitude == null || signum > 1 || signum < -1)
                throw new ArgumentNullException();

            if (signum == 0) {
                int i;
                for (i = magnitude.Length - 1; i >= 0 && magnitude[i] == 0; --i) ;
                if (i >= 0)
                    throw new ArgumentException();
                return;
            }

            // Magnitude is always positive, so don't ever pass a sign of -1.
            words = ConvertToIntArray(magnitude, 0);
            BigInteger result = Make(words, words.Length);
            this.ival = result.ival;
            this.words = result.words;

            if (signum < 0)
                SetNegative();
        }

        public BigInteger(int numBits, Random rnd) {
            if (numBits < 0)
                throw new ArgumentException();

            Init(numBits, rnd);
        }

        public BigInteger(int bitLength, int certainty, Random rnd)
            : this(bitLength, rnd) {
            // Keep going until we find a probable prime.
            while (true) {
                if (IsProbablePrime(certainty))
                    return;

                Init(bitLength, rnd);
            }
        }

        static BigInteger() {
            for (int i = numFixNum; --i >= 0; )
                smallFixNums[i] = new BigInteger(i + minFixNum);
        }
        #endregion

        #region Fields
        /** All integers are stored in 2's-complement form.
   * If words == null, the ival is the value of this BigInteger.
   * Otherwise, the first ival elements of words make the value
   * of this BigInteger, stored in little-endian order, 2's-complement form. */
        private volatile int ival;
        private volatile int[] words;

        // Serialization fields.
        private int bitCount = -1;
        private int bitLength = -1;
        private int firstNonzeroByteNum = -2;
        private int lowestSetBit = -2;
        private byte[] magnitude;
        private int signum;

        /** We pre-allocate integers in the range minFixNum..maxFixNum. */
        private static int minFixNum = -100;
        private static int maxFixNum = 1024;
        private static int numFixNum = maxFixNum - minFixNum + 1;
        private static BigInteger[] smallFixNums = new BigInteger[numFixNum];

        /* When checking the probability of primes, it is most efficient to
         * first check the factoring of small primes, so we'll use this array.
         */
        private static readonly int[] primes =
			{   2,   3,   5,   7,  11,  13,  17,  19,  23,  29,  31,  37,  41,  43,
			    47,  53,  59,  61,  67,  71,  73,  79,  83,  89,  97, 101, 103, 107,
			    109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181,
			    191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251 };

        /** HAC (Handbook of Applied Cryptography), Alfred Menezes & al. Table 4.4. */
        private static readonly int[] k = { 100, 150, 200, 250, 300, 350, 400, 500, 600, 800, 1250, Int32.MaxValue };
        private static readonly int[] t = { 27, 18, 15, 12, 9, 8, 7, 6, 5, 4, 3, 2 };
        // bit4count[I] is number of '1' bits in I.
        private static byte[] bit4_count = { 0, 1, 1, 2,  1, 2, 2, 3,
		                                     1, 2, 2, 3,  2, 3, 3, 4};

        /* Rounding modes: */
        private const int FLOOR = 1;
        private const int CEILING = 2;
        private const int TRUNCATE = 3;
        private const int ROUND = 4;

        public const int CharMinRadix = 2;
        public const int CharMaxRadix = 36;

        public static readonly BigInteger Zero = smallFixNums[-minFixNum];
        public static readonly BigInteger One = smallFixNums[1 - minFixNum];
        #endregion

        #region Properties
        private bool IsNegative {
            get { return (words == null ? ival : words[ival - 1]) < 0; }
        }

        private bool IsZero {
            get { return words == null && ival == 0; }
        }

        private bool IsOne {
            get { return words == null && ival == 1; }
        }

        /** Calculates ceiling(log2(this < 0 ? -this : this+1))
         * See Common Lisp: the Language, 2nd ed, p. 361.
         */
        public int BitLength {
            get {
                if (words == null)
                    return MPN.GetIntLength(ival);
                return MPN.GetIntLength(words, ival);
            }
        }

        public int LowestSetBit {
            get {
                if (IsZero)
                    return -1;
                if (words == null)
                    return MPN.FindLowestBit(ival);
                return MPN.FindLowestBit(words);
            }
        }

        /** Count one bits in a BigInteger.
         * If argument is negative, count zero bits instead. */
        public int BitCount {
            get {
                int i, x_len;
                int[] x_words = words;
                if (x_words == null) {
                    x_len = 1;
                    i = GetBitCount(ival);
                } else {
                    x_len = ival;
                    i = GetBitCount(x_words, x_len);
                }
                return IsNegative ? x_len * 32 - i : i;
            }
        }
        #endregion

        #region Private Methods
        private void Init(int numBits, Random rnd) {
            int highbits = numBits & 31;
            if (highbits > 0)
                highbits = ByteBuffer.URShift(rnd.Next(), (32 - highbits));
            int nwords = numBits / 32;

            while (highbits == 0 && nwords > 0) {
                highbits = rnd.Next();
                --nwords;
            }
            if (nwords == 0 && highbits >= 0) {
                ival = highbits;
            } else {
                ival = highbits < 0 ? nwords + 2 : nwords + 1;
                words = new int[ival];
                words[nwords] = highbits;
                while (--nwords >= 0)
                    words[nwords] = rnd.Next();
            }
        }

        private BigInteger Canonicalize() {
            if (words != null
                && (ival = WordsNeeded(words, ival)) <= 1) {
                if (ival == 1)
                    ival = words[0];
                words = null;
            }
            if (words == null && ival >= minFixNum && ival <= maxFixNum)
                return smallFixNums[ival - minFixNum];
            return this;
        }

        /** Set this to the sum of x and y.
        * OK if x==this. */
        private void SetAdd(BigInteger x, int y) {
            if (x.words == null) {
                Set((long)x.ival + (long)y);
                return;
            }
            int len = x.ival;
            Realloc(len + 1);
            long carry = y;
            for (int i = 0; i < len; i++) {
                carry += ((long)x.words[i] & 0xffffffffL);
                words[i] = (int)carry;
                carry >>= 32;
            }
            if (x.words[len - 1] < 0)
                carry--;
            words[len] = (int)carry;
            ival = WordsNeeded(words, len + 1);
        }

        /** Destructively add an int to this. */
        private void SetAdd(int y) {
            SetAdd(this, y);
        }

        /** Destructively set the value of this to a long. */
        private void Set(long y) {
            int i = (int)y;
            if ((long)i == y) {
                ival = i;
                words = null;
            } else {
                Realloc(2);
                words[0] = i;
                words[1] = (int)(y >> 32);
                ival = 2;
            }
        }

        /** Destructively set the value of this to the given words.
        * The words array is reused, not copied. */
        private void Set(int[] words, int length) {
            ival = length;
            this.words = words;
        }

        /** Destructively set the value of this to that of y. */
        private void Set(BigInteger y) {
            if (y.words == null)
                Set(y.ival);
            else if (this != y) {
                Realloc(y.ival);
                Array.Copy(y.words, 0, words, 0, y.ival);
                ival = y.ival;
            }
        }

        private void SetInvert() {
            if (words == null)
                ival = ~ival;
            else {
                for (int i = ival; --i >= 0; )
                    words[i] = ~words[i];
            }
        }

        private void SetShiftLeft(BigInteger x, int count) {
            int[] xwords;
            int xlen;
            if (x.words == null) {
                if (count < 32) {
                    Set((long)x.ival << count);
                    return;
                }
                xwords = new int[1];
                xwords[0] = x.ival;
                xlen = 1;
            } else {
                xwords = x.words;
                xlen = x.ival;
            }
            int word_count = count >> 5;
            count &= 31;
            int new_len = xlen + word_count;
            if (count == 0) {
                Realloc(new_len);
                for (int i = xlen; --i >= 0; )
                    words[i + word_count] = xwords[i];
            } else {
                new_len++;
                Realloc(new_len);
                int shift_out = MPN.LShift(words, word_count, xwords, xlen, count);
                count = 32 - count;
                words[new_len - 1] = (shift_out << count) >> count;  // sign-extend.
            }
            ival = new_len;
            for (int i = word_count; --i >= 0; )
                words[i] = 0;
        }

        private void SetShiftRight(BigInteger x, int count) {
            if (x.words == null)
                Set(count < 32 ? x.ival >> count : x.ival < 0 ? -1 : 0);
            else if (count == 0)
                Set(x);
            else {
                bool neg = x.IsNegative;
                int word_count = count >> 5;
                count &= 31;
                int d_len = x.ival - word_count;
                if (d_len <= 0)
                    Set(neg ? -1 : 0);
                else {
                    if (words == null || words.Length < d_len)
                        Realloc(d_len);
                    MPN.RShift0(words, x.words, word_count, d_len, count);
                    ival = d_len;
                    if (neg)
                        words[d_len - 1] |= -2 << (31 - count);
                }
            }
        }

        private void SetShift(BigInteger x, int count) {
            if (count > 0)
                SetShiftLeft(x, count);
            else
                SetShiftRight(x, -count);
        }

        /** Return true if any of the lowest n bits are one.
        * (false if n is negative).  */
        private bool CheckBits(int n) {
            if (n <= 0)
                return false;
            if (words == null)
                return n > 31 || ((ival & ((1 << n) - 1)) != 0);
            int i;
            for (i = 0; i < (n >> 5); i++)
                if (words[i] != 0)
                    return true;
            return (n & 31) != 0 && (words[i] & ((1 << (n & 31)) - 1)) != 0;
        }

        /** Convert a semi-processed BigInteger to double.
         * Number must be non-negative.  Multiplies by a power of two, applies sign,
         * and converts to double, with the usual java rounding.
         * @param exp power of two, positive or negative, by which to multiply
         * @param neg true if negative
         * @param remainder true if the BigInteger is the result of a truncating
         * division that had non-zero remainder.  To ensure proper rounding in
         * this case, the BigInteger must have at least 54 bits.  */
        private double RoundToDouble(int exp, bool neg, bool remainder) {
            // Compute length.
            int il = BitLength;

            // Exponent when normalized to have decimal point directly after
            // leading one.  This is stored excess 1023 in the exponent bit field.
            exp += il - 1;

            // Gross underflow.  If exp == -1075, we let the rounding
            // computation determine whether it is minval or 0 (which are just
            // 0x0000 0000 0000 0001 and 0x0000 0000 0000 0000 as bit
            // patterns).
            if (exp < -1075)
                return neg ? -0.0 : 0.0;

            // gross overflow
            if (exp > 1023)
                return neg ? Double.NegativeInfinity : Double.PositiveInfinity;

            // number of bits in mantissa, including the leading one.
            // 53 unless it's denormalized
            int ml = (exp >= -1022 ? 53 : 53 + exp + 1022);

            // Get top ml + 1 bits.  The extra one is for rounding.
            long m;
            int excess_bits = il - (ml + 1);
            if (excess_bits > 0)
                m = ((words == null) ? ival >> excess_bits
                        : MPN.RShiftLong(words, ival, excess_bits));
            else
                m = ToInt64() << (-excess_bits);

            // Special rounding for maxval.  If the number exceeds maxval by
            // any amount, even if it's less than half a step, it overflows.
            if (exp == 1023 && ((m >> 1) == (1L << 53) - 1)) {
                if (remainder || CheckBits(il - ml))
                    return neg ? Double.NegativeInfinity : Double.PositiveInfinity;
                return neg ? -Double.MaxValue : Double.MaxValue;
            }

            // Normal round-to-even rule: round up if the bit dropped is a one, and
            // the bit above it or any of the bits below it is a one.
            if ((m & 1) == 1
                && ((m & 2) == 2 || remainder || CheckBits(excess_bits))) {
                m += 2;
                // Check if we overflowed the mantissa
                if ((m & (1L << 54)) != 0) {
                    exp++;
                    // renormalize
                    m >>= 1;
                }
                    // Check if a denormalized mantissa was just rounded up to a
                    // normalized one.
                else if (ml == 52 && (m & (1L << 53)) != 0)
                    exp++;
            }

            // Discard the rounding bit
            m >>= 1;

            long bits_sign = neg ? (1L << 63) : 0;
            exp += 1023;
            long bits_exp = (exp <= 0) ? 0 : ((long)exp) << 52;
            long bits_mant = m & ~(1L << 52);
            return Convert.ToDouble(bits_sign | bits_exp | bits_mant);
        }

        /** Copy the abolute value of this into an array of words.
         * Assumes words.length >= (this.words == null ? 1 : this.ival).
         * Result is zero-extended, but need not be a valid 2's complement number.
         */
        private void GetAbsolute(int[] words) {
            int len;
            if (this.words == null) {
                len = 1;
                words[0] = this.ival;
            } else {
                len = this.ival;
                for (int i = len; --i >= 0; )
                    words[i] = this.words[i];
            }
            if (words[len - 1] < 0)
                Negate(words, words, len);
            for (int i = words.Length; --i > len; )
                words[i] = 0;
        }

        /** Destructively set this to the negative of x.
         * It is OK if x==this.*/
        private void SetNegative(BigInteger x) {
            int len = x.ival;
            if (x.words == null) {
                if (len == Int32.MinValue)
                    Set(-(long)len);
                else
                    Set(-len);
                return;
            }
            Realloc(len + 1);
            if (Negate(words, x.words, len))
                words[len++] = 0;
            ival = len;
        }

        /** Destructively negate this. */
        private void SetNegative() {
            SetNegative(this);
        }

        /*
        TODO:
        private void ReadObject(Stream s) {
            s.defaultReadObject();
            words = byteArrayToIntArray(magnitude, signum < 0 ? -1 : 0);
            BigInteger result = make(words, words.length);
            this.ival = result.ival;
            this.words = result.words;
        }

        private void WriteObject(Stream s) {
            signum = signum();
            magnitude = ToByteArray();
            s.defaultWriteObject();
        }
        */
        #endregion

        #region Public Methods
        public override int CompareTo(object obj) {
            if (!(obj is BigInteger))
                throw new InvalidCastException();
            return CompareTo(this, (BigInteger)obj);
        }

        public int CompareTo(BigInteger obj) {
            return CompareTo(this, obj);
        }

        public BigInteger Min(BigInteger obj) {
            return CompareTo(this, obj) < 0 ? this : obj;
        }

        public BigInteger Max(BigInteger obj) {
            return CompareTo(this, obj) > 0 ? this : obj;
        }

        public int Sign() {
            int top = words == null ? ival : words[ival - 1];
            if (top == 0 && words == null)
                return 0;
            return top < 0 ? -1 : 1;
        }

        public BigInteger Add(BigInteger obj) {
            return Add(this, obj, 1);
        }

        public BigInteger Subtract(BigInteger obj) {
            return Add(this, obj, -1);
        }

        public BigInteger Multiply(BigInteger y) {
            return Times(this, y);
        }

        public BigInteger Divide(BigInteger value) {
            if (value.IsZero)
                throw new DivideByZeroException();

            BigInteger quot = new BigInteger();
            Divide(this, value, quot, null, TRUNCATE);
            return quot.Canonicalize();
        }

        public BigInteger Remainder(BigInteger value) {
            if (value.IsZero)
                throw new DivideByZeroException();

            BigInteger rem = new BigInteger();
            Divide(this, value, null, rem, TRUNCATE);
            return rem.Canonicalize();
        }

        public BigInteger[] DivideAndRemainder(BigInteger value) {
            if (value.IsZero)
                throw new DivideByZeroException();

            BigInteger[] result = new BigInteger[2];
            result[0] = new BigInteger();
            result[1] = new BigInteger();
            Divide(this, value, result[0], result[1], TRUNCATE);
            result[0].Canonicalize();
            result[1].Canonicalize();
            return result;
        }

        public BigInteger Mod(BigInteger mod) {
            if (mod.IsNegative || mod.IsZero)
                throw new ArithmeticException("non-positive modulus");

            BigInteger rem = new BigInteger();
            Divide(this, mod, null, rem, FLOOR);
            return rem.Canonicalize();
        }

        /** Calculate the integral power of a BigInteger.
        * @param exponent the exponent (must be non-negative)
        */
        public BigInteger Pow(int exponent) {
            if (exponent <= 0) {
                if (exponent == 0)
                    return One;
                throw new ArithmeticException("negative exponent");
            }
            if (IsZero)
                return this;
            int plen = words == null ? 1 : ival;  // Length of pow2.
            int blen = ((BitLength * exponent) >> 5) + 2 * plen;
            bool negative = IsNegative && (exponent & 1) != 0;
            int[] pow2 = new int[blen];
            int[] rwords = new int[blen];
            int[] work = new int[blen];
            GetAbsolute(pow2);	// pow2 = abs(this);
            int rlen = 1;
            rwords[0] = 1; // rwords = 1;
            for (; ; )  // for (i = 0;  ; i++)
			{
                // pow2 == this**(2**i)
                // prod = this**(sum(j=0..i-1, (exponent>>j)&1))
                if ((exponent & 1) != 0) { // r *= pow2
                    MPN.Multiply(work, pow2, plen, rwords, rlen);
                    int[] temp = work; work = rwords; rwords = temp;
                    rlen += plen;
                    while (rwords[rlen - 1] == 0) rlen--;
                }
                exponent >>= 1;
                if (exponent == 0)
                    break;
                // pow2 *= pow2;
                MPN.Multiply(work, pow2, plen, pow2, plen);
                int[] temp2 = work; work = pow2;
                pow2 = temp2;  // swap to avoid a copy
                plen *= 2;
                while (pow2[plen - 1] == 0) plen--;
            }
            if (rwords[rlen - 1] < 0)
                rlen++;
            if (negative)
                Negate(rwords, rwords, rlen);
            return BigInteger.Make(rwords, rlen);
        }

        public BigInteger ModInverse(BigInteger y) {
            if (y.IsNegative || y.IsZero)
                throw new ArithmeticException("non-positive modulo");

            // Degenerate cases.
            if (y.IsOne)
                return Zero;
            if (IsOne)
                return One;

            // Use Euclid's algorithm as in gcd() but do this recursively
            // rather than in a loop so we can use the intermediate results as we
            // unwind from the recursion.
            // Used http://www.math.nmsu.edu/~crypto/EuclideanAlgo.html as reference.
            BigInteger result = new BigInteger();
            bool swapped = false;

            if (y.words == null) {
                // The result is guaranteed to be less than the modulus, y (which is
                // an int), so simplify this by working with the int result of this
                // modulo y.  Also, if this is negative, make it positive via modulo
                // math.  Note that BigInteger.mod() must be used even if this is
                // already an int as the % operator would provide a negative result if
                // this is negative, BigInteger.mod() never returns negative values.
                int xval = (words != null || IsNegative) ? Mod(y).ival : ival;
                int yval = y.ival;

                // Swap values so x > y.
                if (yval > xval) {
                    int tmp = xval; xval = yval; yval = tmp;
                    swapped = true;
                }
                // Normally, the result is in the 2nd element of the array, but
                // if originally x < y, then x and y were swapped and the result
                // is in the 1st element of the array.
                result.ival =
                    EuclidInv(yval, xval % yval, xval / yval)[swapped ? 0 : 1];

                // Result can't be negative, so make it positive by adding the
                // original modulus, y.ival (not the possibly "swapped" yval).
                if (result.ival < 0)
                    result.ival += y.ival;
            } else {
                // As above, force this to be a positive value via modulo math.
                BigInteger x = IsNegative ? this.Mod(y) : this;

                // Swap values so x > y.
                if (x.CompareTo(y) < 0) {
                    result = x; x = y; y = result; // use 'result' as a work var
                    swapped = true;
                }
                // As above (for ints), result will be in the 2nd element unless
                // the original x and y were swapped.
                BigInteger rem = new BigInteger();
                BigInteger quot = new BigInteger();
                Divide(x, y, quot, rem, FLOOR);
                // quot and rem may not be in canonical form. ensure
                rem.Canonicalize();
                quot.Canonicalize();
                BigInteger[] xy = new BigInteger[2];
                EuclidInv(y, rem, quot, xy);
                result = swapped ? xy[0] : xy[1];

                // Result can't be negative, so make it positive by adding the
                // original modulus, y (which is now x if they were swapped).
                if (result.IsNegative)
                    result = Add(result, swapped ? x : y, 1);
            }

            return result;
        }

        public BigInteger ModPow(BigInteger exponent, BigInteger mod) {
            if (mod.IsNegative || mod.IsZero)
                throw new ArithmeticException("non-positive modulo");

            if (exponent.IsNegative)
                return ModInverse(mod);
            if (exponent.IsOne)
                return Mod(mod);

            // To do this naively by first raising this to the power of exponent
            // and then performing modulo m would be extremely expensive, especially
            // for very large numbers.  The solution is found in Number Theory
            // where a combination of partial powers and moduli can be done easily.
            //
            // We'll use the algorithm for Additive Chaining which can be found on
            // p. 244 of "Applied Cryptography, Second Edition" by Bruce Schneier.
            BigInteger s = One;
            BigInteger t = this;
            BigInteger u = exponent;

            while (!u.IsZero) {
                if (u.And(One).IsOne)
                    s = Times(s, t).Mod(mod);
                u = u.ShiftRight(1);
                t = Times(t, t).Mod(mod);
            }

            return s;
        }

        public BigInteger GCD(BigInteger y) {
            int xval = ival;
            int yval = y.ival;
            if (words == null) {
                if (xval == 0)
                    return Abs(y);
                if (y.words == null
                    && xval != Int32.MinValue && yval != Int32.MinValue) {
                    if (xval < 0)
                        xval = -xval;
                    if (yval < 0)
                        yval = -yval;
                    return ValueOf(GCD(xval, yval));
                }
                xval = 1;
            }
            if (y.words == null) {
                if (yval == 0)
                    return Abs(this);
                yval = 1;
            }
            int len = (xval > yval ? xval : yval) + 1;
            int[] xwords = new int[len];
            int[] ywords = new int[len];
            GetAbsolute(xwords);
            y.GetAbsolute(ywords);
            len = MPN.GCD(xwords, ywords, len);
            BigInteger result = new BigInteger(0);
            result.ival = len;
            result.words = xwords;
            return result.Canonicalize();
        }

        /**
        * <p>Returns <code>true</code> if this BigInteger is probably prime,
        * <code>false</code> if it's definitely composite. If <code>certainty</code>
        * is <code><= 0</code>, <code>true</code> is returned.</p>
        *
        * @param certainty a measure of the uncertainty that the caller is willing
        * to tolerate: if the call returns <code>true</code> the probability that
        * this BigInteger is prime exceeds <code>(1 - 1/2<sup>certainty</sup>)</code>.
        * The execution time of this method is proportional to the value of this
        * parameter.
        * @return <code>true</code> if this BigInteger is probably prime,
        * <code>false</code> if it's definitely composite.
        */
        public bool IsProbablePrime(int certainty) {
            if (certainty < 1)
                return true;

            /** We'll use the Rabin-Miller algorithm for doing a probabilistic
            * primality test.  It is fast, easy and has faster decreasing odds of a
            * composite passing than with other tests.  This means that this
            * method will actually have a probability much greater than the
            * 1 - .5^certainty specified in the JCL (p. 117), but I don't think
            * anyone will complain about better performance with greater certainty.
            *
            * The Rabin-Miller algorithm can be found on pp. 259-261 of "Applied
            * Cryptography, Second Edition" by Bruce Schneier.
            */

            // First rule out small prime factors
            BigInteger rem = new BigInteger();
            int i;
            for (i = 0; i < primes.Length; i++) {
                if (words == null && ival == primes[i])
                    return true;

                Divide(this, smallFixNums[primes[i] - minFixNum], null, rem, TRUNCATE);
                if (rem.Canonicalize().IsZero)
                    return false;
            }

            // Now perform the Rabin-Miller test.

            // Set b to the number of times 2 evenly divides (this - 1).
            // I.e. 2^b is the largest power of 2 that divides (this - 1).
            BigInteger pMinus1 = Add(this, -1);
            int b = pMinus1.LowestSetBit;

            // Set m such that this = 1 + 2^b * m.
            BigInteger m = pMinus1.Divide(ValueOf(2L << b - 1));

            // The HAC (Handbook of Applied Cryptography), Alfred Menezes & al. Note
            // 4.49 (controlling the error probability) gives the number of trials
            // for an error probability of 1/2**80, given the number of bits in the
            // number to test.  we shall use these numbers as is if/when 'certainty'
            // is less or equal to 80, and twice as much if it's greater.
            int bits = BitLength;
            for (i = 0; i < k.Length; i++)
                if (bits <= k[i])
                    break;
            int trials = t[i];
            if (certainty > 80)
                trials *= 2;
            BigInteger z;
            for (int w = 0; w < trials; w++) {
                // The HAC (Handbook of Applied Cryptography), Alfred Menezes & al.
                // Remark 4.28 states: "...A strategy that is sometimes employed
                // is to fix the bases a to be the first few primes instead of
                // choosing them at random.
                z = smallFixNums[primes[w] - minFixNum].ModPow(m, this);
                if (z.IsOne || z.Equals(pMinus1))
                    continue;			// Passes the test; may be prime.

                for (i = 0; i < b; ) {
                    if (z.IsOne)
                        return false;
                    i++;
                    if (z.Equals(pMinus1))
                        break;			// Passes the test; may be prime.

                    z = z.ModPow(ValueOf(2), this);
                }

                if (i == b && !z.Equals(pMinus1))
                    return false;
            }
            return true;
        }

        public BigInteger ShiftLeft(int n) {
            return Shift(this, n);
        }

        public BigInteger ShiftRight(int n) {
            return Shift(this, -n);
        }

        public BigInteger Abs() {
            return Abs(this);
        }

        public BigInteger Negate() {
            return Neg(this);
        }

        public override bool Equals(object obj) {
            if (!(obj is BigInteger))
                return false;
            return Equals(this, (BigInteger)obj);
        }

        public override int GetHashCode() {
            return words == null ? ival : (words[0] + words[ival - 1]);
        }

        public override string ToString() {
            return ToString(10);
        }

        public string ToString(int radix) {
            if (words == null)
                return ConvertInt32ToString(ival, radix);
            if (ival <= 2)
                return ConvertInt64ToString(ToInt64(), radix);
            int buf_size = ival * (MPN.GetCharsPerWord(radix) + 1);
            StringBuilder builder = new StringBuilder(buf_size);
            Format(radix, builder);
            return builder.ToString();
        }

        public override int ToInt32() {
            if (words == null)
                return ival;
            return words[0];
        }

        public override long ToInt64() {
            if (words == null)
                return ival;
            if (ival == 1)
                return words[0];
            return ((long)words[1] << 32) + ((long)words[0] & 0xffffffffL);
        }

        public override float ToSingle() {
            return (float)ToDouble();
        }

        public override double ToDouble() {
            if (words == null)
                return (double)ival;
            if (ival <= 2)
                return (double)ToInt64();
            if (IsNegative)
                return Neg(this).RoundToDouble(0, true, false);
            return RoundToDouble(0, false, false);
        }

        public byte[] ToByteArray() {
            // Determine number of bytes needed.  The method bitlength returns
            // the size without the sign bit, so add one bit for that and then
            // add 7 more to emulate the ceil function using integer math.
            byte[] bytes = new byte[(BitLength + 1 + 7) / 8];
            int nbytes = bytes.Length;

            int wptr = 0;
            int word;

            // Deal with words array until one word or less is left to process.
            // If BigInteger is an int, then it is in ival and nbytes will be <= 4.
            while (nbytes > 4) {
                word = words[wptr++];
                for (int i = 4; i > 0; --i, word >>= 8)
                    bytes[--nbytes] = (byte)word;
            }

            // Deal with the last few bytes.  If BigInteger is an int, use ival.
            word = (words == null) ? ival : words[wptr];
            for (; nbytes > 0; word >>= 8)
                bytes[--nbytes] = (byte)word;

            return bytes;
        }

        /** Return the logical (bit-wise) "and" of two BigIntegers. */
        public BigInteger And(BigInteger y) {
            if (y.words == null)
                return And(this, y.ival);
            else if (words == null)
                return And(y, ival);

            BigInteger x = this;
            if (ival < y.ival) {
                BigInteger temp = this; x = y; y = temp;
            }
            int i;
            int len = y.IsNegative ? x.ival : y.ival;
            int[] iWords = new int[len];
            for (i = 0; i < y.ival; i++)
                iWords[i] = x.words[i] & y.words[i];
            for (; i < len; i++)
                iWords[i] = x.words[i];
            return Make(iWords, len);
        }

        /** Return the logical (bit-wise) "(inclusive) or" of two BigIntegers. */
        public BigInteger Or(BigInteger y) {
            return BitOp(7, this, y);
        }

        /** Return the logical (bit-wise) "exclusive or" of two BigIntegers. */
        public BigInteger XOr(BigInteger y) {
            return BitOp(6, this, y);
        }

        /** Return the logical (bit-wise) negation of a BigInteger. */
        public BigInteger Not() {
            return BitOp(12, this, Zero);
        }

        public BigInteger AndNot(BigInteger value) {
            return And(value.Not());
        }

        public BigInteger ClearBit(int n) {
            if (n < 0)
                throw new ArithmeticException();

            return And(One.ShiftLeft(n).Not());
        }

        public BigInteger SetBit(int n) {
            if (n < 0)
                throw new ArithmeticException();

            return Or(One.ShiftLeft(n));
        }

        public bool TestBit(int n) {
            if (n < 0)
                throw new ArithmeticException();
            return !And(One.ShiftLeft(n)).IsZero;
        }

        public BigInteger FlipBit(int n) {
            if (n < 0)
                throw new ArithmeticException();

            return XOr(One.ShiftLeft(n));
        }

        #endregion

        #region Private Static Methods
        /** Convert a big-endian byte array to a little-endian array of words. */
        private static int[] ConvertToIntArray(byte[] bytes, int sign) {
            // Determine number of words needed.
            int[] words = new int[bytes.Length / 4 + 1];
            int nwords = words.Length;

            // Create a int out of modulo 4 high order bytes.
            int bptr = 0;
            int word = sign;
            for (int i = bytes.Length % 4; i > 0; --i, bptr++)
                word = (word << 8) | (bytes[bptr] & 0xff);
            words[--nwords] = word;

            // Elements remaining in byte[] are a multiple of 4.
            while (nwords > 0)
                words[--nwords] = bytes[bptr++] << 24 |
                                  (bytes[bptr++] & 0xff) << 16 |
                                  (bytes[bptr++] & 0xff) << 8 |
                                  (bytes[bptr++] & 0xff);
            return words;
        }

        /** Allocate a new non-shared BigInteger.
        * @param nwords number of words to allocate
        */
        private static BigInteger Alloc(int nwords) {
            BigInteger result = new BigInteger();
            if (nwords > 1)
                result.words = new int[nwords];
            return result;
        }

        /** Change words.length to nwords.
         * We allow words.length to be upto nwords+2 without reallocating.
        */
        private void Realloc(int nwords) {
            if (nwords == 0) {
                if (words != null) {
                    if (ival > 0)
                        ival = words[0];
                    words = null;
                }
            } else if (words == null
                       || words.Length < nwords
                       || words.Length > nwords + 2) {
                int[] new_words = new int[nwords];
                if (words == null) {
                    new_words[0] = ival;
                    ival = 1;
                } else {
                    if (nwords < ival)
                        ival = nwords;
                    Array.Copy(words, 0, new_words, 0, ival);
                }
                words = new_words;
            }
        }

        private static int CompareTo(BigInteger x, BigInteger y) {
            if (x.words == null && y.words == null)
                return x.ival < y.ival ? -1 : x.ival > y.ival ? 1 : 0;
            bool x_negative = x.IsNegative;
            bool y_negative = y.IsNegative;
            if (x_negative != y_negative)
                return x_negative ? -1 : 1;
            int x_len = x.words == null ? 1 : x.ival;
            int y_len = y.words == null ? 1 : y.ival;
            if (x_len != y_len)
                return (x_len > y_len) != x_negative ? 1 : -1;
            return MPN.Compare(x.words, y.words, x_len);
        }

        /** Make a canonicalized BigInteger from an array of words.
        * The array may be reused (without copying). */
        private static BigInteger Make(int[] words, int len) {
            if (words == null)
                return ValueOf(len);
            len = WordsNeeded(words, len);
            if (len <= 1)
                return len == 0 ? Zero : ValueOf(words[0]);
            BigInteger num = new BigInteger();
            num.words = words;
            num.ival = len;
            return num;
        }

        /** Calculate how many words are significant in words[0:len-1].
        * Returns the least value x such that x>0 && words[0:x-1]==words[0:len-1],
        * when words is viewed as a 2's complement integer.
        */
        private static int WordsNeeded(int[] words, int len) {
            int i = len;
            if (i > 0) {
                int word = words[--i];
                if (word == -1) {
                    while (i > 0 && (word = words[i - 1]) < 0) {
                        i--;
                        if (word != -1) break;
                    }
                } else {
                    while (word == 0 && i > 0 && (word = words[i - 1]) >= 0) i--;
                }
            }
            return i + 1;
        }

        /** Add two ints, yielding a BigInteger. */
        private static BigInteger Add(int x, int y) {
            return ValueOf((long)x + (long)y);
        }

        /** Add a BigInteger and an int, yielding a new BigInteger. */
        private static BigInteger Add(BigInteger x, int y) {
            if (x.words == null)
                return BigInteger.Add(x.ival, y);
            BigInteger result = new BigInteger(0);
            result.SetAdd(x, y);
            return result.Canonicalize();
        }

        /** Add two BigIntegers, yielding their sum as another BigInteger. */
        private static BigInteger Add(BigInteger x, BigInteger y, int k) {
            if (x.words == null && y.words == null)
                return ValueOf((long)k * (long)y.ival + (long)x.ival);
            if (k != 1) {
                if (k == -1)
                    y = Neg(y);
                else
                    y = Times(y, ValueOf(k));
            }
            if (x.words == null)
                return Add(y, x.ival);
            if (y.words == null)
                return Add(x, y.ival);
            // Both are big
            if (y.ival > x.ival) { // Swap so x is longer then y.
                BigInteger tmp = x; x = y; y = tmp;
            }
            BigInteger result = Alloc(x.ival + 1);
            int i = y.ival;
            long carry = MPN.AddN(result.words, x.words, y.words, i);
            long y_ext = y.words[i - 1] < 0 ? 0xffffffffL : 0;
            for (; i < x.ival; i++) {
                carry += ((long)x.words[i] & 0xffffffffL) + y_ext; ;
                result.words[i] = (int)carry;
                carry = ByteBuffer.URShift(carry, 32);
            }
            if (x.words[i - 1] < 0)
                y_ext--;
            result.words[i] = (int)(carry + y_ext);
            result.ival = i + 1;
            return result.Canonicalize();
        }

        private static BigInteger Times(BigInteger x, int y) {
            if (y == 0)
                return Zero;
            if (y == 1)
                return x;
            int[] xwords = x.words;
            int xlen = x.ival;
            if (xwords == null)
                return ValueOf((long)xlen * (long)y);
            bool negative;
            BigInteger result = Alloc(xlen + 1);
            if (xwords[xlen - 1] < 0) {
                negative = true;
                Negate(result.words, xwords, xlen);
                xwords = result.words;
            } else
                negative = false;
            if (y < 0) {
                negative = !negative;
                y = -y;
            }
            result.words[xlen] = MPN.Multiply1(result.words, xwords, xlen, y);
            result.ival = xlen + 1;
            if (negative)
                result.SetNegative();
            return result.Canonicalize();
        }

        private static BigInteger Times(BigInteger x, BigInteger y) {
            if (y.words == null)
                return Times(x, y.ival);
            if (x.words == null)
                return Times(y, x.ival);
            bool negative = false;
            int[] xwords;
            int[] ywords;
            int xlen = x.ival;
            int ylen = y.ival;
            if (x.IsNegative) {
                negative = true;
                xwords = new int[xlen];
                Negate(xwords, x.words, xlen);
            } else {
                negative = false;
                xwords = x.words;
            }
            if (y.IsNegative) {
                negative = !negative;
                ywords = new int[ylen];
                Negate(ywords, y.words, ylen);
            } else
                ywords = y.words;
            // Swap if x is shorter then y.
            if (xlen < ylen) {
                int[] twords = xwords; xwords = ywords; ywords = twords;
                int tlen = xlen; xlen = ylen; ylen = tlen;
            }
            BigInteger result = Alloc(xlen + ylen);
            MPN.Multiply(result.words, xwords, xlen, ywords, ylen);
            result.ival = xlen + ylen;
            if (negative)
                result.SetNegative();
            return result.Canonicalize();
        }

        private static void Divide(long x, long y,
                                   BigInteger quotient, BigInteger remainder,
                                   int rounding_mode) {
            bool xNegative, yNegative;
            if (x < 0) {
                xNegative = true;
                if (x == Int64.MinValue) {
                    Divide(ValueOf(x), ValueOf(y), quotient, remainder, rounding_mode);
                    return;
                }
                x = -x;
            } else
                xNegative = false;

            if (y < 0) {
                yNegative = true;
                if (y == Int64.MinValue) {
                    if (rounding_mode == TRUNCATE) { // x != Long.Min_VALUE implies abs(x) < abs(y)
                        if (quotient != null)
                            quotient.Set(0);
                        if (remainder != null)
                            remainder.Set(x);
                    } else
                        Divide(ValueOf(x), ValueOf(y), quotient, remainder, rounding_mode);
                    return;
                }
                y = -y;
            } else
                yNegative = false;

            long q = x / y;
            long r = x % y;
            bool qNegative = xNegative ^ yNegative;

            bool add_one = false;
            if (r != 0) {
                switch (rounding_mode) {
                    case TRUNCATE:
                        break;
                    case CEILING:
                    case FLOOR:
                        if (qNegative == (rounding_mode == FLOOR))
                            add_one = true;
                        break;
                    case ROUND:
                        add_one = r > ((y - (q & 1)) >> 1);
                        break;
                }
            }
            if (quotient != null) {
                if (add_one)
                    q++;
                if (qNegative)
                    q = -q;
                quotient.Set(q);
            }
            if (remainder != null) {
                // The remainder is by definition: X-Q*Y
                if (add_one) {
                    // Subtract the remainder from Y.
                    r = y - r;
                    // In this case, abs(Q*Y) > abs(X).
                    // So sign(remainder) = -sign(X).
                    xNegative = !xNegative;
                } else {
                    // If !add_one, then: abs(Q*Y) <= abs(X).
                    // So sign(remainder) = sign(X).
                }
                if (xNegative)
                    r = -r;
                remainder.Set(r);
            }
        }

        /** Divide two integers, yielding quotient and remainder.
        * @param x the numerator in the division
        * @param y the denominator in the division
        * @param quotient is set to the quotient of the result (iff quotient!=null)
        * @param remainder is set to the remainder of the result
        *  (iff remainder!=null)
        * @param rounding_mode one of FLOOR, CEILING, TRUNCATE, or ROUND.
        */
        private static void Divide(BigInteger x, BigInteger y, BigInteger quotient, BigInteger remainder, int rounding_mode) {
            if ((x.words == null || x.ival <= 2)
                && (y.words == null || y.ival <= 2)) {
                long x_l = x.ToInt64();
                long y_l = y.ToInt64();
                if (x_l != Int64.MinValue && y_l != Int64.MinValue) {
                    Divide(x_l, y_l, quotient, remainder, rounding_mode);
                    return;
                }
            }

            bool xNegative = x.IsNegative;
            bool yNegative = y.IsNegative;
            bool qNegative = xNegative ^ yNegative;

            int ylen = y.words == null ? 1 : y.ival;
            int[] ywords = new int[ylen];
            y.GetAbsolute(ywords);
            while (ylen > 1 && ywords[ylen - 1] == 0) ylen--;

            int xlen = x.words == null ? 1 : x.ival;
            int[] xwords = new int[xlen + 2];
            x.GetAbsolute(xwords);
            while (xlen > 1 && xwords[xlen - 1] == 0) xlen--;

            int qlen, rlen;

            int cmpval = MPN.Compare(xwords, xlen, ywords, ylen);
            if (cmpval < 0)  // abs(x) < abs(y)
			{ // quotient = 0;  remainder = num.
                int[] rwords = xwords; xwords = ywords; ywords = rwords;
                rlen = xlen; qlen = 1; xwords[0] = 0;
            } else if (cmpval == 0)  // abs(x) == abs(y)
			{
                xwords[0] = 1; qlen = 1;  // quotient = 1
                ywords[0] = 0; rlen = 1;  // remainder = 0;
            } else if (ylen == 1) {
                qlen = xlen;
                // Need to leave room for a word of leading zeros if dividing by 1
                // and the dividend has the high bit set.  It might be safe to
                // increment qlen in all cases, but it certainly is only necessary
                // in the following case.
                if (ywords[0] == 1 && xwords[xlen - 1] < 0)
                    qlen++;
                rlen = 1;
                ywords[0] = MPN.DivideMod1(xwords, xwords, xlen, ywords[0]);
            } else  // abs(x) > abs(y)
			{
                // Normalize the denominator, i.e. make its most significant bit set by
                // shifting it normalization_steps bits to the left.  Also shift the
                // numerator the same number of steps (to keep the quotient the same!).

                int nshift = MPN.CountLeadingZeros(ywords[ylen - 1]);
                if (nshift != 0) {
                    // Shift up the denominator setting the most significant bit of
                    // the most significant word.
                    MPN.LShift(ywords, 0, ywords, ylen, nshift);

                    // Shift up the numerator, possibly introducing a new most
                    // significant word.
                    int x_high = MPN.LShift(xwords, 0, xwords, xlen, nshift);
                    xwords[xlen++] = x_high;
                }

                if (xlen == ylen)
                    xwords[xlen++] = 0;
                MPN.Divide(xwords, xlen, ywords, ylen);
                rlen = ylen;
                MPN.RShift0(ywords, xwords, 0, rlen, nshift);

                qlen = xlen + 1 - ylen;
                if (quotient != null) {
                    for (int i = 0; i < qlen; i++)
                        xwords[i] = xwords[i + ylen];
                }
            }

            if (ywords[rlen - 1] < 0) {
                ywords[rlen] = 0;
                rlen++;
            }

            // Now the quotient is in xwords, and the remainder is in ywords.

            bool add_one = false;
            if (rlen > 1 || ywords[0] != 0) { // Non-zero remainder i.e. in-exact quotient.
                switch (rounding_mode) {
                    case TRUNCATE:
                        break;
                    case CEILING:
                    case FLOOR:
                        if (qNegative == (rounding_mode == FLOOR))
                            add_one = true;
                        break;
                    case ROUND:
                        // int cmp = compareTo(remainder<<1, abs(y));
                        BigInteger tmp = remainder == null ? new BigInteger() : remainder;
                        tmp.Set(ywords, rlen);
                        tmp = Shift(tmp, 1);
                        if (yNegative)
                            tmp.SetNegative();
                        int cmp = CompareTo(tmp, y);
                        // Now cmp == compareTo(sign(y)*(remainder<<1), y)
                        if (yNegative)
                            cmp = -cmp;
                        add_one = (cmp == 1) || (cmp == 0 && (xwords[0] & 1) != 0);
                        break;
                }
            }
            if (quotient != null) {
                quotient.Set(xwords, qlen);
                if (qNegative) {
                    if (add_one)  // -(quotient + 1) == ~(quotient)
                        quotient.SetInvert();
                    else
                        quotient.SetNegative();
                } else if (add_one)
                    quotient.SetAdd(1);
            }
            if (remainder != null) {
                // The remainder is by definition: X-Q*Y
                remainder.Set(ywords, rlen);
                if (add_one) {
                    // Subtract the remainder from Y:
                    // abs(R) = abs(Y) - abs(orig_rem) = -(abs(orig_rem) - abs(Y)).
                    BigInteger tmp;
                    if (y.words == null) {
                        tmp = remainder;
                        tmp.Set(yNegative ? ywords[0] + y.ival : ywords[0] - y.ival);
                    } else
                        tmp = Add(remainder, y, yNegative ? 1 : -1);
                    // Now tmp <= 0.
                    // In this case, abs(Q) = 1 + floor(abs(X)/abs(Y)).
                    // Hence, abs(Q*Y) > abs(X).
                    // So sign(remainder) = -sign(X).
                    if (xNegative)
                        remainder.SetNegative(tmp);
                    else
                        remainder.Set(tmp);
                } else {
                    // If !add_one, then: abs(Q*Y) <= abs(X).
                    // So sign(remainder) = sign(X).
                    if (xNegative)
                        remainder.SetNegative();
                }
            }
        }

        private static int[] EuclidInv(int a, int b, int prevDiv) {
            if (b == 0)
                throw new ArithmeticException("not invertible");

            if (b == 1)
                // Success:  values are indeed invertible!
                // Bottom of the recursion reached; start unwinding.
                return new int[] { -prevDiv, 1 };

            int[] xy = EuclidInv(b, a % b, a / b);	// Recursion happens here.
            a = xy[0]; // use our local copy of 'a' as a work var
            xy[0] = a * -prevDiv + xy[1];
            xy[1] = a;
            return xy;
        }

        private static void EuclidInv(BigInteger a, BigInteger b,
                                      BigInteger prevDiv, BigInteger[] xy) {
            if (b.IsZero)
                throw new ArithmeticException("not invertible");

            if (b.IsOne) {
                // Success:  values are indeed invertible!
                // Bottom of the recursion reached; start unwinding.
                xy[0] = Neg(prevDiv);
                xy[1] = One;
                return;
            }

            // Recursion happens in the following conditional!

            // If a just contains an int, then use integer math for the rest.
            if (a.words == null) {
                int[] xyInt = EuclidInv(b.ival, a.ival % b.ival, a.ival / b.ival);
                xy[0] = new BigInteger(xyInt[0]);
                xy[1] = new BigInteger(xyInt[1]);
            } else {
                BigInteger rem = new BigInteger();
                BigInteger quot = new BigInteger();
                Divide(a, b, quot, rem, FLOOR);
                // quot and rem may not be in canonical form. ensure
                rem.Canonicalize();
                quot.Canonicalize();
                EuclidInv(b, rem, quot, xy);
            }

            BigInteger t = xy[0];
            xy[0] = Add(xy[1], Times(t, prevDiv), -1);
            xy[1] = t;
        }

        /** Calculate Greatest Common Divisor for non-negative ints. */
        private static int GCD(int a, int b) {
            // Euclid's algorithm, copied from libg++.
            int tmp;
            if (b > a) {
                tmp = a; a = b; b = tmp;
            }
            for (; ; ) {
                if (b == 0)
                    return a;
                if (b == 1)
                    return b;
                tmp = b;
                b = a % b;
                a = tmp;
            }
        }

        private static BigInteger Shift(BigInteger x, int count) {
            if (x.words == null) {
                if (count <= 0)
                    return ValueOf(count > -32 ? x.ival >> (-count) : x.ival < 0 ? -1 : 0);
                if (count < 32)
                    return ValueOf((long)x.ival << count);
            }
            if (count == 0)
                return x;
            BigInteger result = new BigInteger(0);
            result.SetShift(x, count);
            return result.Canonicalize();
        }

        private void Format(int radix, StringBuilder buffer) {
            if (words == null)
                buffer.Append(ConvertInt32ToString(ival, radix));
            else if (ival <= 2)
                buffer.Append(ConvertInt64ToString(ToInt64(), radix));
            else {
                bool neg = IsNegative;
                int[] work;
                if (neg || radix != 16) {
                    work = new int[ival];
                    GetAbsolute(work);
                } else
                    work = words;
                int len = ival;

                if (radix == 16) {
                    if (neg)
                        buffer.Append('-');
                    int buf_start = buffer.Length;
                    for (int i = len; --i >= 0; ) {
                        int word = work[i];
                        for (int j = 8; --j >= 0; ) {
                            int hex_digit = (word >> (4 * j)) & 0xF;
                            // Suppress leading zeros:
                            if (hex_digit > 0 || buffer.Length > buf_start)
                                buffer.Append(CharForDigit(hex_digit, 16));
                        }
                    }
                } else {
                    int i = buffer.Length;
                    for (; ; ) {
                        int digit = MPN.DivideMod1(work, work, len, radix);
                        buffer.Append(CharForDigit(digit, radix));
                        while (len > 0 && work[len - 1] == 0) len--;
                        if (len == 0)
                            break;
                    }
                    if (neg)
                        buffer.Append('-');
                    /* Reverse buffer. */
                    int j = buffer.Length - 1;
                    while (i < j) {
                        char tmp = buffer[i];
                        buffer[i] = buffer[j];
                        buffer[j] = tmp;
                        i++; j--;
                    }
                }
            }
        }

        /* Assumes x and y are both canonicalized. */
        private static bool Equals(BigInteger x, BigInteger y) {
            if (x.words == null && y.words == null)
                return x.ival == y.ival;
            if (x.words == null || y.words == null || x.ival != y.ival)
                return false;
            for (int i = x.ival; --i >= 0; ) {
                if (x.words[i] != y.words[i])
                    return false;
            }
            return true;
        }

        private static BigInteger ValueOf(string s, int radix) {
            int len = s.Length;
            // Testing (len < MPN.chars_per_word(radix)) would be more accurate,
            // but slightly more expensive, for little practical gain.
            if (len <= 15 && radix <= 16)
                //return ValueOf(ParseLong(s, radix));
                return ValueOf(Int64.Parse(s));

            int byte_len = 0;
            byte[] bytes = new byte[len];
            bool negative = false;
            for (int i = 0; i < len; i++) {
                char ch = s[i];
                if (ch == '-')
                    negative = true;
                else if (ch == '_' || (byte_len == 0 && (ch == ' ' || ch == '\t')))
                    continue;
                else {
                    int digit = CharToDigit(ch, radix);
                    if (digit < 0)
                        break;
                    bytes[byte_len++] = (byte)digit;
                }
            }
            return ValueOf(bytes, byte_len, negative, radix);
        }

        private static BigInteger ValueOf(byte[] digits, int byte_len, bool negative, int radix) {
            int chars_per_word = MPN.GetCharsPerWord(radix);
            int[] words = new int[byte_len / chars_per_word + 1];
            int size = MPN.SetString(words, digits, byte_len, radix);
            if (size == 0)
                return Zero;
            if (words[size - 1] < 0)
                words[size++] = 0;
            if (negative)
                Negate(words, words, size);
            return Make(words, size);
        }

        /** Set dest[0:len-1] to the negation of src[0:len-1].
         * Return true if overflow (i.e. if src is -2**(32*len-1)).
         * Ok for src==dest. */
        private static bool Negate(int[] dest, int[] src, int len) {
            long carry = 1;
            bool negative = src[len - 1] < 0;
            for (int i = 0; i < len; i++) {
                carry += ((long)(~src[i]) & 0xffffffffL);
                dest[i] = (int)carry;
                carry >>= 32;
            }
            return (negative && dest[len - 1] < 0);
        }

        private static BigInteger Abs(BigInteger x) {
            return x.IsNegative ? Neg(x) : x;
        }

        private static BigInteger Neg(BigInteger x) {
            if (x.words == null && x.ival != Int32.MinValue)
                return ValueOf(-x.ival);
            BigInteger result = new BigInteger(0);
            result.SetNegative(x);
            return result.Canonicalize();
        }

        /** Return the boolean opcode (for bitOp) for swapped operands.
         * I.e. bitOp(swappedOp(op), x, y) == bitOp(op, y, x).
         */
        private static int SwappedOp(int op) {
            return "\000\001\004\005\002\003\006\007\010\011\014\015\012\013\016\017"[op];
        }

        /** Do one the the 16 possible bit-wise operations of two BigIntegers. */
        private static BigInteger BitOp(int op, BigInteger x, BigInteger y) {
            switch (op) {
                case 0: return Zero;
                case 1: return x.And(y);
                case 3: return x;
                case 5: return y;
                case 15: return ValueOf(-1);
            }
            BigInteger result = new BigInteger();
            SetBitOp(result, op, x, y);
            return result.Canonicalize();
        }

        /** Do one the the 16 possible bit-wise operations of two BigIntegers. */
        private static void SetBitOp(BigInteger result, int op,
                                     BigInteger x, BigInteger y) {
            if (y.words == null) {
            } else if (x.words == null || x.ival < y.ival) {
                BigInteger temp = x; x = y; y = temp;
                op = SwappedOp(op);
            }
            int xi;
            int yi;
            int xlen, ylen;
            if (y.words == null) {
                yi = y.ival;
                ylen = 1;
            } else {
                yi = y.words[0];
                ylen = y.ival;
            }
            if (x.words == null) {
                xi = x.ival;
                xlen = 1;
            } else {
                xi = x.words[0];
                xlen = x.ival;
            }
            if (xlen > 1)
                result.Realloc(xlen);
            int[] w = result.words;
            int i = 0;
            // Code for how to handle the remainder of x.
            // 0:  Truncate to length of y.
            // 1:  Copy rest of x.
            // 2:  Invert rest of x.
            int finish = 0;
            int ni;
            switch (op) {
                case 0:  // clr
                    ni = 0;
                    break;
                case 1: // and
                    for (; ; ) {
                        ni = xi & yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi < 0) finish = 1;
                    break;
                case 2: // andc2
                    for (; ; ) {
                        ni = xi & ~yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi >= 0) finish = 1;
                    break;
                case 3:  // copy x
                    ni = xi;
                    finish = 1;  // Copy rest
                    break;
                case 4: // andc1
                    for (; ; ) {
                        ni = ~xi & yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi < 0) finish = 2;
                    break;
                case 5: // copy y
                    for (; ; ) {
                        ni = yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    break;
                case 6:  // xor
                    for (; ; ) {
                        ni = xi ^ yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    finish = yi < 0 ? 2 : 1;
                    break;
                case 7:  // ior
                    for (; ; ) {
                        ni = xi | yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi >= 0) finish = 1;
                    break;
                case 8:  // nor
                    for (; ; ) {
                        ni = ~(xi | yi);
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi >= 0) finish = 2;
                    break;
                case 9:  // eqv [exclusive nor]
                    for (; ; ) {
                        ni = ~(xi ^ yi);
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    finish = yi >= 0 ? 2 : 1;
                    break;
                case 10:  // c2
                    for (; ; ) {
                        ni = ~yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    break;
                case 11:  // orc2
                    for (; ; ) {
                        ni = xi | ~yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi < 0) finish = 1;
                    break;
                case 12:  // c1
                    ni = ~xi;
                    finish = 2;
                    break;
                case 13:  // orc1
                    for (; ; ) {
                        ni = ~xi | yi;
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi >= 0) finish = 2;
                    break;
                case 14:  // nand
                    for (; ; ) {
                        ni = ~(xi & yi);
                        if (i + 1 >= ylen) break;
                        w[i++] = ni; xi = x.words[i]; yi = y.words[i];
                    }
                    if (yi < 0) finish = 2;
                    break;
                default:
                case 15:  // set
                    ni = -1;
                    break;
            }
            // Here i==ylen-1; w[0]..w[i-1] have the correct result;
            // and ni contains the correct result for w[i+1].
            if (i + 1 == xlen)
                finish = 0;
            switch (finish) {
                case 0:
                    if (i == 0 && w == null) {
                        result.ival = ni;
                        return;
                    }
                    w[i++] = ni;
                    break;
                case 1: w[i] = ni; while (++i < xlen) w[i] = x.words[i]; break;
                case 2: w[i] = ni; while (++i < xlen) w[i] = ~x.words[i]; break;
            }
            result.ival = i;
        }

        /** Return the logical (bit-wise) "and" of a BigInteger and an int. */
        private static BigInteger And(BigInteger x, int y) {
            if (x.words == null)
                return ValueOf(x.ival & y);
            if (y >= 0)
                return ValueOf(x.words[0] & y);
            int len = x.ival;
            int[] words = new int[len];
            words[0] = x.words[0] & y;
            while (--len > 0)
                words[len] = x.words[len];
            return Make(words, x.ival);
        }

        private static int GetBitCount(int i) {
            int count = 0;
            while (i != 0) {
                count += bit4_count[i & 15];
                i = ByteBuffer.URShift(i, 4);
            }
            return count;
        }

        private static int GetBitCount(int[] x, int len) {
            int count = 0;
            while (--len >= 0)
                count += GetBitCount(x[len]);
            return count;
        }

        //added by tomi@deveel.com
        private static string ConvertInt32ToString(int value, int radix) {
            if (radix < CharMinRadix || radix > CharMaxRadix)
                radix = 10;

            // For negative numbers, print out the absolute value w/ a leading '-'.
            // Use an array large enough for a binary number.
            char[] buffer = new char[33];
            int i = 33;
            bool isNeg = false;
            if (value < 0) {
                isNeg = true;
                value = -value;

                // When the value is MIN_VALUE, it overflows when made positive
                if (value < 0) {
                    buffer[--i] = Digits[(int)(-(value + radix) % radix)];
                    value = -(value / radix);
                }
            }

            do {
                buffer[--i] = Digits[value % radix];
                value /= radix;
            }
            while (value > 0);

            if (isNeg)
                buffer[--i] = '-';

            // Package constructor avoids an array copy.
            return new string(buffer, i, 33 - i);
        }

        //added by tomi@deveel.com
        public static string ConvertInt64ToString(long value, int radix) {
            // Use the Integer toString for efficiency if possible.
            if ((int)value == value)
                return ConvertInt32ToString((int)value, radix);

            if (radix < CharMinRadix || radix > CharMaxRadix)
                radix = 10;

            // For negative numbers, print out the absolute value w/ a leading '-'.
            // Use an array large enough for a binary number.
            char[] buffer = new char[65];
            int i = 65;
            bool isNeg = false;
            if (value < 0) {
                isNeg = true;
                value = -value;

                // When the value is MIN_VALUE, it overflows when made positive
                if (value < 0) {
                    buffer[--i] = Digits[(int)(-(value + radix) % radix)];
                    value = -(value / radix);
                }
            }

            do {
                buffer[--i] = Digits[(int)(value % radix)];
                value /= radix;
            }
            while (value > 0);

            if (isNeg)
                buffer[--i] = '-';

            // Package constructor avoids an array copy.
            return new string(buffer, i, 65 - i);
        }

        #region Char Utils
        private static string CharNumValue
            = "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\000\001\002\003\004\005\006\007"
              + "\010\011\uffff\uffff\012\013\014\015\016\017\020"
              + "\021\022\023\024\025\026\027\030\031\032\033"
              + "\034\035\036\037 !\"#\uffff\uffff\012"
              + "\013\014\015\016\017\020\021\022\023\024\025"
              + "\026\027\030\031\032\033\034\035\036\037 "
              + "!\"#\uffff\uffff\uffff\uffff\uffff\uffff\002\003"
              + "\uffff\001\uffff\ufffe\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\000\001\002\003\004\005"
              + "\006\007\010\011\uffff\uffff\uffff\uffff\000\001\002"
              + "\003\004\005\006\007\010\011\001\002\003\004"
              + "\uffff\020\012d\u03e8\uffff\uffff\uffff\024\036("
              + "2<FPZ\u2710\021\022\023\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff\uffff"
              + "\uffff\000\004\005\006\007\010\011\uffff\uffff\uffff"
              + "\001\001\002\003\004\005\006\007\010\011\012"
              + "\013\0142d\u01f4\u03e8\001\002\003\004\005"
              + "\006\007\010\011\012\013\0142d\u01f4\u03e8"
              + "\u03e8\u1388\u2710\uffff\012\013\014\015\016\017\020"
              + "\021\022\023\024\uffff\uffff\002\003\004\005\006"
              + "\007\010\011\012\000\001\002\003\004\005\006"
              + "\007\010\011\012\024\036\005\006\007\010\011"
              + "\uffff\uffff";

        private static char CharForDigit(int digit, int radix) {
            if (radix < CharMinRadix || radix > CharMaxRadix ||
                digit < 0 || digit >= radix)
                return '\0';
            return Number.Digits[digit];
        }

        internal static int CharToDigit(char ch, int radix) {
            if (radix < CharMinRadix || radix > CharMaxRadix)
                return -1;

            UnicodeCategory cat = Char.GetUnicodeCategory(ch);
            if (((cat & UnicodeCategory.UppercaseLetter) != 0) ||
                ((cat & UnicodeCategory.LowercaseLetter) != 0) ||
                ((cat & UnicodeCategory.DecimalDigitNumber) != 0)) {
                int digit = CharNumValue[((int)ch - 1) >> 7];
                return (digit < radix) ? digit : -1;
            }
            /*
            Java original
            if (((1 << (attr & TYPE_MASK))
                 & ((1 << UPPERCASE_LETTER)
                    | (1 << LOWERCASE_LETTER)
                    | (1 << DECIMAL_DIGIT_NUMBER))) != 0) {
                // Signedness doesn't matter; 0xffff vs. -1 are both rejected.
                int digit = numValue[attr >> 7];
                return (digit < radix) ? digit : -1;
            }
            */
            return -1;
        }
        #endregion

        /*
		private static long ParseLong(string str, int radix) {
			return ParseLong(str, radix, false);
		}

		private static long ParseLong(string str, int radix, bool decode) {
			if (!decode && str == null)
				throw new FormatException();
			int index = 0;
			int len = str.Length;
			bool isNeg = false;
			if (len == 0)
				throw new FormatException();
			int ch = str[index];
			if (ch == '-') {
				if (len == 1)
					throw new FormatException();
				isNeg = true;
				ch = str[++index];
			}
			if (decode) {
				if (ch == '0') {
					if (++index == len)
						return 0;
					if ((str[index] & ~('x' ^ 'X')) == 'X') {
						radix = 16;
						index++;
					} else
						radix = 8;
				} else if (ch == '#') {
					radix = 16;
					index++;
				}
			}
			if (index == len)
				throw new FormatException();

			long max = Int64.MaxValue / radix;
			// We can't directly Write `max = (MAX_VALUE + 1) / radix'.
			// So instead we fake it.
			if (isNeg && Int64.MaxValue % radix == radix - 1)
				++max;

			long val = 0;
			while (index < len) {
				if (val < 0 || val > max)
					throw new FormatException();

				ch = CharToDigit(str[index++], radix);
				val = val * radix + ch;
				if (ch < 0 || (val < 0 && (!isNeg || val != Int64.MinValue)))
					throw new FormatException();
			}
			return isNeg ? -val : val;
		}
		*/
        #endregion

        #region Public Static Methods
        public static BigInteger ProbablePrime(int bitLength, Random rnd) {
            if (bitLength < 2)
                throw new ArithmeticException();

            return new BigInteger(bitLength, 100, rnd);
        }

        /** Return a (possibly-shared) BigInteger with a given long value. */
        public static BigInteger ValueOf(long value) {
            if (value >= minFixNum && value <= maxFixNum)
                return smallFixNums[(int)value - minFixNum];
            int i = (int)value;
            if ((long)i == value)
                return new BigInteger(i);
            BigInteger result = Alloc(2);
            result.ival = 2;
            result.words[0] = i;
            result.words[1] = (int)(value >> 32);
            return result;
        }
        #endregion
    }
}