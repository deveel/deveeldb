using System;

namespace Deveel.Data.Util {
	static class BytesUtil {
		public static int ReadInt4(byte[] arr, int offset) {
			return BitConverter.ToInt32(arr, offset);
		}

		public static long ReadInt8(byte[] arr, int offset) {
			return BitConverter.ToInt64(arr, offset);
		}

		public static void WriteInt8(long value, byte[] arr, int offset) {
			byte[] buff = BitConverter.GetBytes(value);
			Array.Copy(buff, 0, arr, offset, buff.Length);
		}

		/// <summary>
		/// Operates a shift on the given integer by the number of bits specified.
		/// </summary>
		/// <param name="number">The number to shift.</param>
		/// <param name="bits">The number of bits to shift the given number.</param>
		/// <returns>
		/// Returns an <see cref="int"/> representing the shifted
		/// number.
		/// </returns>
		public static int URShift(int number, int bits) {
			if (number >= 0)
				return number >> bits;
			return (number >> bits) + (2 << ~bits);
		}
	}
}
