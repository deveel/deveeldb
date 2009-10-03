// 
//  StreamableObjectPart.cs
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