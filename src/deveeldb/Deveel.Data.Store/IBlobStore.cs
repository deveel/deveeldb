// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.DbSystem;

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