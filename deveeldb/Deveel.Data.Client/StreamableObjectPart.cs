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

namespace Deveel.Data.Client {
	///<summary>
    /// Represents a response from the server for a section of a streamable object.
	///</summary>
	/// <remarks>
	/// A streamable object can always be represented as a byte[] array and is limited 
	/// to String (as 2-byte unicode) and binary data types.
	/// </remarks>
	public class StreamableObjectPart {

        /// <summary>
        /// The byte[] array that is the contents of the cell from the server.
        /// </summary>
		private readonly byte[] part_contents;

        /// <summary>
        /// Constructs the <see cref="StreamableObjectPart"/>.
        /// </summary>
        /// <param name="contents">The contents of the part: this must be <i>immutable</i>.</param>
		public StreamableObjectPart(byte[] contents) {
			part_contents = contents;
		}

	    ///<summary>
	    /// Returns the contents of the current <see cref="StreamableObjectPart"/>.
	    ///</summary>
	    ///<returns></returns>
	    public byte[] Contents {
	        get { return part_contents; }
	    }
	}
}