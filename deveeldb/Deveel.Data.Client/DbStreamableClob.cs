//  
//  DbStreamableClob.cs
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
using System.Text;

namespace Deveel.Data.Client {
	/// <summary>
	/// A <see cref="IClob"/> that is a large object that may be streamed 
	/// from the server directly to this object.
	/// </summary>
	/// <remarks>
	/// A clob that is streamable is only alive for the lifetime of the 
	/// result set it is part of. If the underlying result set that contains
	/// this streamable clob is closed then this clob is no longer valid.
	/// </remarks>
	class DbStreamableClob : StreamableObject, IClob {
		internal DbStreamableClob(DeveelDbConnection connection, int result_set_id, ReferenceType type, long streamable_object_id, long size)
			: base(connection, result_set_id, type, streamable_object_id, size) {
		}

		/// <inheritdoc/>
		public long Length {
			get { return Type == ReferenceType.UnicodeText ? RawSize/2 : RawSize; }
		}

		public Encoding Encoding {
			get { return Type == ReferenceType.UnicodeText ? Encoding.Unicode : Encoding.ASCII; }
		}

		/// <inheritdoc/>
		public String GetString(long pos, int length) {
			//TODO: verify this...
			if (Type == ReferenceType.UnicodeText)
				pos = pos/2;

			Stream stream = GetStream();
			try {
				stream.Seek(pos, SeekOrigin.Current);

				StreamReader reader = new StreamReader(stream, Encoding);
				StringBuilder buf = new StringBuilder(length);
				for (int i = 0; i < length; ++i) {
					int c = reader.Read();
					if (c == -1)
						break;

					buf.Append((char)c);
				}
				return buf.ToString();
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message); 
				Console.Error.WriteLine(e.StackTrace);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public TextReader GetReader() {
			return new StreamReader(GetStream(), Encoding);
		}

		/// <inheritdoc/>
		public Stream GetStream() {
			/*
			TODO: check...
			if (Type == 3) {
				return new StreamableObjectInputStream(this, RawSize);
			} else if (Type == 4) {
				//TODO: check this...
				// return new AsciiInputStream(getCharacterStream());
				return new StreamableObjectInputStream(this, RawSize);
			} else {
				throw new DataException("Unknown type.");
			}
			*/
			return new StreamableObjectInputStream(this, RawSize);
		}

		/// <inheritdoc/>
		public long GetPosition(String searchstr, long start) {
			throw DbDataException.Unsupported();
		}

		/// <inheritdoc/>
		public long GetPosition(IClob searchstr, long start) {
			throw DbDataException.Unsupported();
		}
	}
}