// 
//  DeveelDbBinary.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Data.SqlTypes;
using System.IO;

namespace Deveel.Data.Client {
	public struct DeveelDbBinary : INullable, ISizeable {
		private DeveelDbBinary(byte[] data, int offset, int count, bool isNull) {
			if (isNull) {
				this.data = null;
			} else {
				this.data = new byte[count];
				Array.Copy(data, offset, this.data, 0, count);
			}
			this.isNull = isNull;
		}

		public DeveelDbBinary(byte[] data, int offset, int count)
			: this(data, offset, count, data == null) {
		}

		public DeveelDbBinary(byte[] data)
			: this(data, 0, (data == null ? 0 : data.Length), (data == null)) {
		}

		public DeveelDbBinary(Stream stream, int count)
			: this(ReadFromStream(stream, count), 0, count) {
		}

		private readonly bool isNull;
		private readonly byte[] data;

		public static readonly DeveelDbBinary Null = new DeveelDbBinary(null, 0, 0, true);

		public byte this[int index] {
			get {
				if (isNull)
					throw new InvalidOperationException();
				return data[index];
			}
		}

		public int Length {
			get {
				if (isNull)
					throw new InvalidOperationException();
				return data.Length;
			}
		}

		int ISizeable.Size {
			get { return Length; }
		}

		public byte [] Value {
			get { return data == null ? new byte[0] : (byte[]) data.Clone(); }
		}

		private bool ByteArrayEqual(byte[] array) {
			if (data == null && array == null)
				return true;
			if (data == null)
				return false;
			if (data.Length != array.Length)
				return false;

			for (int i = 0; i < data.Length; i++) {
				byte v1 = data[i];
				byte v2 = array[i];
				if (v1 != v2)
					return false;
			}

			return true;
		}

		public override bool Equals(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull;
			if (obj is byte[])
				return ByteArrayEqual((byte[]) obj);
			if (!(obj is DeveelDbBinary))
				throw new ArgumentException("Cannot test the equality between a DeveelDB binary and " + obj.GetType());

			DeveelDbBinary other = (DeveelDbBinary) obj;
			if (isNull && other.isNull)
				return true;
			if (isNull)
				return false;
			return ByteArrayEqual(other.data);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		private static byte [] ReadFromStream(Stream stream, int count) {
			if (stream == null || stream == Stream.Null)
				return null;

			byte[] result = new byte[count];
			int i = 0;
			while (i < count) {
				int read = stream.Read(result, i, count - i);
				if (read == 0)
					throw new IOException("Premature end of stream.");
				i += read;
			}

			return result;
		}

		public static DeveelDbBoolean operator ==(DeveelDbBinary a, DeveelDbBinary b) {
			if (a.IsNull || b.IsNull)
				return DeveelDbBoolean.Null;
			return a.Equals(b);
		}

		public static DeveelDbBoolean operator !=(DeveelDbBinary a, DeveelDbBinary b) {
			return !(a == b);
		}

		public static implicit operator byte[](DeveelDbBinary binary) {
			return binary.Value;
		}

		public static implicit operator DeveelDbBinary(byte[] value) {
			return new DeveelDbBinary(value);
		}
	}
}