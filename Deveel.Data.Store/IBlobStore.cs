// 
//  IBlobStore.cs
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

namespace Deveel.Data.Store {
	/// <summary>
	/// A very restricted interface for accessing a blob store.
	/// </summary>
	/// <remarks>
	/// This is used by a <see cref="MasterTableDataSource"/> implementation 
	/// to query and resolve blob information.
	/// </remarks>
	public interface IBlobStore {
		/// <summary>
		/// Given a large object reference identifier, generates a <see cref="IRef"/>
		/// implementation that provides access to the information in the large object.
		/// </summary>
		/// <param name="reference_id">The identifier pointing to the <see cref="IRef"/>
		/// within the blob store to return.</param>
		/// <returns>
		/// Returns a Read-only static instance of <see cref="IRef"/> (either <see cref="IClobRef"/>
		/// or <see cref="IBlobRef"/> depending on the iformation in the large object).
		/// </returns>
		/// <seealso cref="IRef"/>
		/// <seealso cref="IBlobRef"/>
		/// <seealso cref="IClobRef"/>
		IRef GetLargeObject(long reference_id);

		/// <summary>
		/// Tells the <see cref="IBlobStore">blob store</see> that a static 
		/// reference has been established in a table to the blob referenced by the 
		/// given id.
		/// </summary>
		/// <param name="reference_id">The identifier of the reference to establish
		/// into the blob store.</param>
		/// <remarks>
		/// This is used to count references to a blob, and possibly clean up a blob 
		/// if there are no references remaining to it.
		/// </remarks>
		void EstablishReference(long reference_id);

		/// <summary>
		/// Tells the <see cref="IBlobStore">blob store</see> that a static 
		/// reference has been released to the given blob.
		/// </summary>
		/// <param name="reference_id">The identifier of the reference to release
		/// from the blob store.</param>
		/// <remarks>
		/// This would typically be called when the row in the database is removed.
		/// </remarks>
		void ReleaseReference(long reference_id);
	}
}