//  
//  ByteBuffer.cs
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

using System;

namespace Deveel.Data.Util {
	/// <summary>
	/// A wrapper for an array of <see cref="byte"/>.
	/// </summary>
	/// <remarks>
	/// This provides various functions for altering the state of 
	/// the buffer.
	/// </remarks>
	public sealed class ByteBuffer {
		/// <summary>
		/// The wrapped byte array itself.
		/// </summary>
		private readonly byte[] buf;

		/// <summary>
		/// The current position in the array.
		/// </summary>
		private int pos;

		/// <summary>
		/// The length of the buf array.
		/// </summary>
		private readonly int length;

		/// <summary>
		/// Constructs the buffer.
		/// </summary>
		/// <param name="buf"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		public ByteBuffer(byte[] buf, int offset, int length) {
			this.buf = buf;
			this.length = length;
			pos = offset;
		}

		public ByteBuffer(byte[] buf)
			: this(buf, 0, buf.Length) {
		}

		/// <summary>
		/// Gets or sets the position in to the buffer.
		/// </summary>
		public int Position {
			set { pos = value; }
			get { return pos; }
		}

		/// <summary>
		/// Returns the length of this buffer.
		/// </summary>
		public int Length {
			get { return length; }
		}

		/// <summary>
		/// Writes a byte array into the buffer.
		/// </summary>
		/// <param name="b"></param>
		/// <param name="offset"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public ByteBuffer Write(byte[] b, int offset, int count) {
			Array.Copy(b, offset, buf, pos, count);
			pos += count;
			return this;
		}

		public ByteBuffer Write(byte[] b) {
			return Write(b, 0, b.Length);
		}

		/// <summary>
		/// Writes a ByteBuffer in to this buffer.
		/// </summary>
		/// <param name="buffer"></param>
		/// <returns></returns>
		public ByteBuffer Write(ByteBuffer buffer) {
			return Write(buffer.buf, buffer.pos, buffer.length);
		}

		/// <summary>
		/// Reads a byte array from the buffer.
		/// </summary>
		/// <param name="b"></param>
		/// <param name="offset"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public ByteBuffer Read(byte[] b, int offset, int count) {
			Array.Copy(buf, pos, b, offset, count);
			pos += count;
			return this;
		}

		/// <summary>
		/// Writes an integer into the buffer at the current position.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public ByteBuffer WriteInteger(int v) {
			WriteInteger(v, buf, pos);
			pos += 4;
			return this;
		}

		/// <summary>
		/// Reads an integer from the buffer at the current position.
		/// </summary>
		/// <returns></returns>
		public int ReadInt4() {
			int v = ReadInt4(buf, pos);
			pos += 4;
			return v;
		}

		/// <summary>
		/// Writes a byte into the buffer at the current position.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public ByteBuffer WriteByte(byte v) {
			buf[pos] = v;
			++pos;
			return this;
		}

		/// <summary>
		/// Reads a byte from the buffer at the current position.
		/// </summary>
		/// <returns></returns>
		public byte ReadByte() {
			byte b = buf[pos];
			++pos;
			return b;
		}

		/// <summary>
		/// Writes a short into the buffer at the current position.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public ByteBuffer WriteInt2(short v) {
			WriteInt2(v, buf, pos);
			pos += 2;
			return this;
		}

		/// <summary>
		/// Reads a short from the buffer at the current position.
		/// </summary>
		/// <returns></returns>
		public short ReadInt2() {
			short v = ReadInt2(buf, pos);
			pos += 2;
			return v;
		}

		public static void WriteInteger(int value, byte[] arr, int offset) {
			/*
			TODO: check ...
		  arr[offset + 0] = (byte) ((value >>> 24) & 0xFF);
		  arr[offset + 1] = (byte) ((value >>> 16) & 0xFF);
		  arr[offset + 2] = (byte) ((value >>>  8) & 0xFF);
		  arr[offset + 3] = (byte) ((value >>>  0) & 0xFF);
			 */
			byte[] buff = BitConverter.GetBytes(value);
			Array.Copy(buff, 0, arr, offset, buff.Length);
		}

		public static char ReadChar(byte[] arr, int offset) {
			/*
			TODO: check ...
		  int c1 = (((int) arr[offset + 0]) & 0x0FF);
		  int c2 = (((int) arr[offset + 1]) & 0x0FF);
		  return (char) ((c1 << 8) + (c2));
			 */
			return BitConverter.ToChar(arr, offset);
		}

		public static void WriteChar(char value, byte[] arr, int offset) {
			/*
			TODO: check ...
		  arr[offset + 0] = (byte) ((value >>> 8) & 0x0FF);
		  arr[offset + 1] = (byte) ((value >>> 0) & 0x0FF);
			*/
			byte[] buff = BitConverter.GetBytes(value);
			Array.Copy(buff, 0, arr, offset, buff.Length);
		}

		public static short ReadInt2(byte[] arr, int offset) {
			/*
			TODO: check ...
		  int c1 = (((int) arr[offset + 0]) & 0x0FF);
		  int c2 = (((int) arr[offset + 1]) & 0x0FF);
		  return (short) ((c1 << 8) + (c2));
			*/
			return BitConverter.ToInt16(arr, offset);
		}

		public static void WriteInt2(short value, byte[] arr, int offset) {
			/*
			TODO: check ...
		  arr[offset + 0] = (byte) ((value >>> 8) & 0x0FF);
		  arr[offset + 1] = (byte) ((value >>> 0) & 0x0FF);
			 */
			byte[] buff = BitConverter.GetBytes(value);
			Array.Copy(buff, 0, arr, offset, buff.Length);
		}

		public static int ReadInt4(byte[] arr, int offset) {
			/*
			TODO: check ...
		  int c1 = (((int) arr[offset + 0]) & 0x0FF);
		  int c2 = (((int) arr[offset + 1]) & 0x0FF);
		  int c3 = (((int) arr[offset + 2]) & 0x0FF);
		  int c4 = (((int) arr[offset + 3]) & 0x0FF);
		  return (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);
			 */
			return BitConverter.ToInt32(arr, offset);
		}

		public static long ReadInt8(byte[] arr, int offset) {
			/*
			TODO: check ...
		  long c1 = (((int) arr[offset + 0]) & 0x0FF);
		  long c2 = (((int) arr[offset + 1]) & 0x0FF);
		  long c3 = (((int) arr[offset + 2]) & 0x0FF);
		  long c4 = (((int) arr[offset + 3]) & 0x0FF);
		  long c5 = (((int) arr[offset + 4]) & 0x0FF);
		  long c6 = (((int) arr[offset + 5]) & 0x0FF);
		  long c7 = (((int) arr[offset + 6]) & 0x0FF);
		  long c8 = (((int) arr[offset + 7]) & 0x0FF);
    
		  return (c1 << 56) + (c2 << 48) + (c3 << 40) +
				 (c4 << 32) + (c5 << 24) + (c6 << 16) + (c7 <<  8) + (c8);
			 */
			return BitConverter.ToInt64(arr, offset);
		}

		public static void WriteInt8(long value, byte[] arr, int offset) {
			/*
			TODO:
		  arr[offset + 0] = (byte) ((value >>> 56) & 0xFF);
		  arr[offset + 1] = (byte) ((value >>> 48) & 0xFF);
		  arr[offset + 2] = (byte) ((value >>> 40) & 0xFF);
		  arr[offset + 3] = (byte) ((value >>> 32) & 0xFF);
		  arr[offset + 4] = (byte) ((value >>> 24) & 0xFF);
		  arr[offset + 5] = (byte) ((value >>> 16) & 0xFF);
		  arr[offset + 6] = (byte) ((value >>>  8) & 0xFF);
		  arr[offset + 7] = (byte) ((value >>>  0) & 0xFF);
			*/
			byte[] buff = BitConverter.GetBytes(value);
			Array.Copy(buff, 0, arr, offset, buff.Length);
		}

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
	}
}