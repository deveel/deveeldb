// 
//  IRef.cs
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

using Deveel.Data.Store;

namespace Deveel.Data {
	/// <summary>
	/// An interface that represents a reference to a object that 
	/// isn't stored in main memory.
	/// </summary>
	/// <remarks>
	/// The reference to the object is made through the id value returned 
	/// by the <see cref="Id"/> property.
	/// </remarks>
	public interface IRef {
		/// <summary>
		/// Gets an id used to reference this object in the context of the 
		/// database.
		/// </summary>
		/// <returns></returns>
		/// <remarks>
		/// Once a static reference is made (or removed) to/from this object, the
		/// <see cref="IBlobStore"/> should be notified of the reference. The store 
		/// will remove an large object that has no references to it.
		/// </remarks>
		/// <seealso cref="IBlobStore.EstablishReference"/>
		/// <seealso cref="IBlobStore.GetLargeObject"/>
		/// <seealso cref="IBlobStore.ReleaseReference"/>
		long Id { get; }

		/// <summary>
		/// Gets the type of the large object that is being referenced.
		/// </summary>
		/// <remarks>
		/// <list type="table">
		///		<item>
		///			<term>2</term>
		///			<description>binary object</description>
		///		</item>
		///		<item>
		///			<term>3</term>
		///			<description>ASCII character object</description>
		///		</item>
		///		<item>
		///			<term>4</term>
		///			<description>Unicode character object</description>
		///		</item>
		/// </list>
		/// </remarks>
		byte Type { get; }

		/// <summary>
		/// Gets the <i>raw</i> size of this large object in bytes when it is in its 
		/// binary form.
		/// </summary>
		/// <remarks>
		/// This value allows the system to know how many bytes can be read from the
		/// large object referenced when it's being transferred to the client.
		/// </remarks>
		long RawSize { get; }


		/// <summary>
		/// Reads a part of this large object from the store into the given 
		/// byte buffer.
		/// </summary>
		/// <param name="offset"></param>
		/// <param name="buf"></param>
		/// <param name="length"></param>
		/// <remarks>
		/// This method should only be used when reading a large object to transfer to 
		/// the client. It represents the byte[] representation of the object only and 
		/// is only useful for transferral of the large object.
		/// </remarks>
		void Read(long offset, byte[] buf, int length);

		/// <summary>
		/// This method is used to write the contents of the large object into the 
		/// backing store.
		/// </summary>
		/// <param name="offset"></param>
		/// <param name="buf"></param>
		/// <param name="length"></param>
		/// <remarks>
		/// This method will only work when the large object is in an initial <i>write</i>
		/// phase in which the client is pushing the contents of the large object onto 
		/// the server to be stored.
		/// </remarks>
		void Write(long offset, byte[] buf, int length);

		/// <summary>
		/// This method is called when the write phrase has completed, 
		/// and it marks this large object as complete.
		/// </summary>
		/// <remarks>
		/// After this method is called the large object reference is a static object 
		/// that can not be changed.
		/// </remarks>
		void Complete();
	}
}