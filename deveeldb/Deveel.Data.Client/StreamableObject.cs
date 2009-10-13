//  
//  StreamableObject.cs
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

using Deveel.Data.Util;

namespace Deveel.Data.Client {
	/// <summary>
	/// An abstract class that provides various convenience behaviour 
	/// for creating streamable <see cref="IBlob"/> and <see cref="IClob"/> 
	/// classes.
	/// </summary>
	/// <remarks>
	/// A streamable object is typically a large object that can be fetched 
	/// in separate pieces from the server.  A streamable object only survives
	/// for as long as the result-set that it is part of is open.
	/// </remarks>
	abstract class StreamableObject {
		/// <summary>
		/// The <see cref="DeveelDbConnection"/> object that this object was 
		/// returned as part of the result of.
		/// </summary>
		protected readonly DeveelDbConnection connection;

		/// <summary>
		/// The result_id of the ResultSet this clob is from.
		/// </summary>
		protected readonly int result_set_id;

		/// <summary>
		/// The streamable object identifier.
		/// </summary>
		private readonly long streamable_object_id;

		/// <summary>
		/// The type of encoding of the stream.
		/// </summary>
		private readonly ReferenceType type;

		/// <summary>
		/// The size of the streamable object.
		/// </summary>
		private readonly long size;

		internal StreamableObject(DeveelDbConnection connection, int result_set_id, 
			ReferenceType type, long streamable_object_id, long size) {
			this.connection = connection;
			this.result_set_id = result_set_id;
			this.type = type;
			this.streamable_object_id = streamable_object_id;
			this.size = size;
		}

		/// <summary>
		/// Returns the streamable object identifier for referencing this streamable 
		/// object on the server.
		/// </summary>
		protected long StreamableId {
			get { return streamable_object_id; }
		}

		/// <summary>
		/// Returns the encoding type of this object.
		/// </summary>
		protected ReferenceType Type {
			get { return type; }
		}

		/// <summary>
		/// Returns the number of bytes in this streamable object.
		/// </summary>
		/// <remarks>
		/// This may not represent the actual size of the object when it is 
		/// decoded.  For example, a IClob may be encoded as 2-byte per character 
		/// (unicode) so the actual length of the clob with be size / 2.
		/// </remarks>
		protected long RawSize {
			get { return size; }
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// A <see cref="Stream"/> that is used to read the data from 
		/// the streamable object as a basic byte encoding.
		/// </summary>
		/// <remarks>
		/// This maintains an internal buffer.
		/// </remarks>
		internal class StreamableObjectInputStream : PagedInputStream {
			private readonly StreamableObject obj;

			/// <summary>
			/// The default size of the buffer.
			/// </summary>
			private const int B_SIZE = 64 * 1024;

			public StreamableObjectInputStream(StreamableObject obj, long in_size)
				: base(B_SIZE, in_size) {
				this.obj = obj;
			}

			protected override void ReadPageContent(byte[] buf, long pos, int length) {
				try {
					// Request a part of the blob from the server
				    StreamableObjectPart part = obj.connection.RequestStreamableObjectPart(obj.result_set_id,
				                                                                           obj.streamable_object_id, pos, length);
					Array.Copy(part.Contents, 0, buf, 0, length);
				} catch (DataException e) {
					throw new IOException("SQL Error: " + e.Message);
				}
			}

		}

	}
}