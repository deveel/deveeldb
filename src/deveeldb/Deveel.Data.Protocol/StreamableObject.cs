// 
//  Copyright 2010  Deveel
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

using System;

namespace Deveel.Data.Protocol {
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
		private readonly ReferenceType type;

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
		public StreamableObject(ReferenceType type, long size, long id) {
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
		public ReferenceType Type {
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