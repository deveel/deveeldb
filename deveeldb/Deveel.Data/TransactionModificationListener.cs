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

namespace Deveel.Data {
	///<summary>
	/// A listener that is notified of table modification events made by a
	/// transaction, both immediately inside a transaction and when a 
	/// transaction commits.
	///</summary>
	/// <remarks>
	/// These events can occur either immediately before or immediately
	/// after the data is modified or during a commit.
	/// </remarks>
	public interface TransactionModificationListener {
		///<summary>
		/// An action for when changes to a table are committed.
		///</summary>
		///<param name="e"></param>
		/// <remarks>
		/// This event occurs after constraint checks, and before the change is 
		/// actually committed to the database.  If this method generates an 
		/// exception then the change is rolled back and any changes made by the 
		/// transaction are lost.  This action is generated inside a 'commit Lock' 
		/// of the conglomerate, and therefore care should be taken with the 
		/// performance of this method.
		/// <para>
		/// The event object provides access to a <see cref="SimpleTransaction"/>
		/// object that is a read-only view of the database in its committed state 
		/// (if this operation is successful). The transaction can be used to perform 
		/// any last minute deferred constraint checks.
		/// </para>
		/// <para>
		/// This action is useful for last minute abortion of a transaction, or for
		/// updating cache information.  It can not be used as a triggering mechanism
		/// and should never call back to user code.
		/// </para>
		/// </remarks>
		void OnTableCommitChange(TableCommitModificationEvent e);

	}
}