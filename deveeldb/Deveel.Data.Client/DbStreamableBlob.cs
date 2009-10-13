//  
//  DbStreamableBlob.cs
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
using System.Data;
using System.IO;

namespace Deveel.Data.Client {
	/// <summary>
	/// A <see cref="IBlob"/> that is a large object that may be streamed from 
	/// the server directly to this object.
	/// </summary>
	/// <remarks>
	/// A blob that is streamable is only alive for the lifetime of the result 
	/// set it is part of.  If the underlying result set that contains this 
	/// streamable blob is closed then this blob is no longer valid.
	/// </remarks>
	class DbStreamableBlob : StreamableObject, IBlob {
		internal DbStreamableBlob(DeveelDbConnection connection, int result_set_id, ReferenceType type, long streamable_object_id, long size)
			: base(connection, result_set_id, type, streamable_object_id, size) {
		}

		/// <inheritdoc/>
		public long Length {
			get { return RawSize; }
		}

		/// <inheritdoc/>
		public byte[] GetBytes(long pos, int length) {
			// First byte is at position 1 according to JDBC Spec.
			if (pos < 0 || pos + length > Length) {
				throw new DataException("Out of bounds.");
			}

			// The buffer we are reading into
			byte[] buf = new byte[length];
			Stream i_stream = GetStream();
			try {
				i_stream.Seek(pos, SeekOrigin.Begin);
				for (int i = 0; i < length; ++i) {
					buf[i] = (byte)i_stream.ReadByte();
				}
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new DataException("IO Error: " + e.Message);
			}

			return buf;
		}

		/// <inheritdoc/>
		public Stream GetStream() {
			return new StreamableObjectInputStream(this, RawSize);
		}

		/// <inheritdoc/>
		public long GetPosition(byte[] pattern, long start) {
			throw DbDataException.Unsupported();
		}

		/// <inheritdoc/>
		public long GetPosition(IBlob pattern, long start) {
			throw DbDataException.Unsupported();
		}
	}
}