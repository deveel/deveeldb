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

namespace Deveel.Math {
	static class Utils {
		/// <summary>
		/// Operates a shift on the given integer by the number of bits specified.
		/// </summary>
		/// <param name="number">The number to shift.</param>
		/// <param name="bits">The number of bits to shift the given number.</param>
		/// <returns>
		/// Returns an <see cref="System.Int32">int</see> representing the shifted
		/// number.
		/// </returns>
		public static int URShift(int number, int bits) {
			if (number >= 0)
				return number >> bits;
			return (number >> bits) + (2 << ~bits);
		}

		public static int URShift(int number, long bits) {
			return URShift(number, (int)bits);
		}

		public static long URShift(long number, int bits) {
			if (number >= 0)
				return number >> bits;
			return (number >> bits) + (2L << ~bits);
		}

		public static long URShift(long number, long bits) {
			return URShift(number, (int)bits);
		}

		public static int numberOfLeadingZeros(int value) {
			value |= URShift(value, 1);
			value |= URShift(value, 2);
			value |= URShift(value, 4);
			value |= URShift(value, 8);
			value |= URShift(value, 16);
			return bitCount(~value);
		}

		public static int numberOfLeadingZeros(long value) {
			value |= URShift(value, 1);
			value |= URShift(value, 2);
			value |= URShift(value, 4);
			value |= URShift(value, 8);
			value |= URShift(value, 16);
			value |= URShift(value, 32);
			return bitCount(~value);
		}

		public static int numberOfTrailingZeros(int value) {
			return bitCount((value & -value) - 1);
		}

		public static int numberOfTrailingZeros(long value) {
			return bitCount((value & -value) - 1);
		}

		public static int bitCount(int x) {
			// Successively collapse alternating bit groups into a sum.
			x = ((x >> 1) & 0x55555555) + (x & 0x55555555);
			x = ((x >> 2) & 0x33333333) + (x & 0x33333333);
			x = ((x >> 4) & 0x0f0f0f0f) + (x & 0x0f0f0f0f);
			x = ((x >> 8) & 0x00ff00ff) + (x & 0x00ff00ff);
			return ((x >> 16) & 0x0000ffff) + (x & 0x0000ffff);
		}

		public static int bitCount(long x) {
			// Successively collapse alternating bit groups into a sum.
			x = ((x >> 1) & 0x5555555555555555L) + (x & 0x5555555555555555L);
			x = ((x >> 2) & 0x3333333333333333L) + (x & 0x3333333333333333L);
			int v = (int)(URShift(x, 32) + x);
			v = ((v >> 4) & 0x0f0f0f0f) + (v & 0x0f0f0f0f);
			v = ((v >> 8) & 0x00ff00ff) + (v & 0x00ff00ff);
			return ((v >> 16) & 0x0000ffff) + (v & 0x0000ffff);
		}

		public static int highestOneBit(int value) {
			value |= URShift(value, 1);
			value |= URShift(value, 2);
			value |= URShift(value, 4);
			value |= URShift(value, 8);
			value |= URShift(value, 16);
			return value ^ URShift(value, 1);
		}

		public static long highestOneBit(long value) {
			value |= URShift(value, 1);
			value |= URShift(value, 2);
			value |= URShift(value, 4);
			value |= URShift(value, 8);
			value |= URShift(value, 16);
			value |= URShift(value, 32);
			return value ^ URShift(value, 1);
		}

        public static long doubleToLong(double d)
        {
            if (d != d)
            {
                return 0L;
            }
            if (d >= 9.2233720368547758E+18)
            {
                return 0x7fffffffffffffffL;
            }
            if (d <= -9.2233720368547758E+18)
            {
                return -9223372036854775808L;
            }
            return (long)d;
        }

 

	}
}