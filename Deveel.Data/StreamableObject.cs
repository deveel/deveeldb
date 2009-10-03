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

using Deveel.Data.Client;

namespace Deveel.Data {
	/// <summary>
	/// An object that is streamable (such as a long binary object, or 
	/// a long string object).
	/// </summary>
	/// <remarks>
	/// This is passed between client and server and contains basic primitive 
	/// information about the object it represents.  The actual contents of the 
	/// object itself must be obtained through other means (see 
	/// <see cref="IDatabaseInterface"/>).
	/// </remarks>
	public sealed class StreamableObject {

        /// <summary>
        /// The type of the object.
        /// </summary>
		private readonly byte type;

        /// <summary>
        /// The size of the object in bytes.
        /// </summary>
		private readonly long size;

        /// <summary>
        /// The identifier that identifies this object.
        /// </summary>
		private readonly long id;

        /// <summary>
        /// Constructs the <see cref="StreamableObject"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="size"></param>
        /// <param name="id"></param>
		public StreamableObject(byte type, long size, long id) {
			this.type = type;
			this.size = size;
			this.id = id;
		}


		///<summary>
        /// Returns the type of object this stub represents.
		///</summary>
		/// <remarks>
		/// Returns 1 if it represents 2-byte unicde character object, 2 if it represents 
		/// binary data.
		/// </remarks>
		public byte Type {
			get { return type; }
		}

		///<summary>
        /// Returns the size of the object stream, or -1 if the size is unknown.
		///</summary>
		/// <remarks>
		/// If this represents a unicode character string, you would calculate the 
		/// total characters as size / 2.
		/// </remarks>
		public long Size {
			get { return size; }
		}

		///<summary>
        /// Returns an identifier that can identify this object within some context.
		///</summary>
		/// <remarks>
		/// For example, if this is a streamable object on the client side, then the
		/// identifier might be the value that is able to retreive a section of the
		/// streamable object from the <see cref="IDatabaseInterface"/>.
		/// </remarks>
		public long Identifier {
			get { return id; }
		}
	}
}