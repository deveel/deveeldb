// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Store {
	/// <summary>
	/// Extension methods to optimize the access to the contents of an <see cref="IArea"/>
	/// of a database store.
	/// </summary>
	public static class AreaExtensions {
		/// <summary>
		/// Reads a single byte from the area.
		/// </summary>
		/// <param name="area">The source area from where to read.</param>
		/// <returns>
		/// Returns a <see cref="byte"/> read from the area.
		/// </returns>
		public static byte ReadByte(this IArea area) {
			var bytes = new byte[1];
			area.Read(bytes, 0, 1);
			return bytes[0];
		}

		/// <summary>
		/// Reads a 2-byte integer from the area.
		/// </summary>
		/// <param name="area">The source area from where to read.</param>
		/// <returns>
		/// Returns a <see cref="short"/> read from the area.
		/// </returns>
		public static short ReadInt2(this IArea area) {
			var bytes = new byte[2];
			area.Read(bytes, 0, 2);
			return BitConverter.ToInt16(bytes, 0);
		}

		/// <summary>
		/// Reads a 4-byte integer from the area.
		/// </summary>
		/// <param name="area">The source area from where to read.</param>
		/// <returns>
		/// Returns a <see cref="int"/> read from the area.
		/// </returns>
		public static int ReadInt4(this IArea area) {
			var bytes = new byte[4];
			area.Read(bytes, 0, 4);
			return BitConverter.ToInt32(bytes, 0);
		}

		/// <summary>
		/// Reads a 8-byte integer from the area.
		/// </summary>
		/// <param name="area">The source area from where to read.</param>
		/// <returns>
		/// Returns a <see cref="long"/> read from the area.
		/// </returns>
		public static long ReadInt8(this IArea area) {
			var bytes = new byte[8];
			area.Read(bytes, 0, 8);
			return BitConverter.ToInt64(bytes, 0);
		}

		/// <summary>
		/// Writes a single byte into the given area.
		/// </summary>
		/// <param name="area">The destination area where to write.</param>
		/// <param name="value">The value to be written.</param>
		public static void WriteByte(this IArea area, byte value) {
			var bytes = new byte[1] {value};
			area.Write(bytes, 0, 1);
		}

		/// <summary>
		/// Writes a 2-byte integer into the given area.
		/// </summary>
		/// <param name="area">The destination area where to write.</param>
		/// <param name="value">The value to be written.</param>
		public static void WriteInt2(this IArea area, short value) {
			var bytes = BitConverter.GetBytes(value);
			area.Write(bytes, 0, 2);
		}

		/// <summary>
		/// Writes a 4-byte integer into the given area.
		/// </summary>
		/// <param name="area">The destination area where to write.</param>
		/// <param name="value">The value to be written.</param>
		public static void WriteInt4(this IArea area, int value) {
			var bytes = BitConverter.GetBytes(value);
			area.Write(bytes, 0, 4);
		}

		/// <summary>
		/// Writes a 8-byte integer into the given area.
		/// </summary>
		/// <param name="area">The destination area where to write.</param>
		/// <param name="value">The value to be written.</param>
		public static void WriteInt8(this IArea area, long value) {
			var bytes = BitConverter.GetBytes(value);
			area.Write(bytes, 0, 8);
		}
	}
}