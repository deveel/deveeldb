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

namespace Deveel.Data.Transactions {
	///<summary>
	/// Thrown when a transaction error happens.
	///</summary>
	/// <remarks>
	/// This can only be thrown during the commit process of a transaction.
	/// </remarks>
	public class TransactionException : Exception {

		// The types of transaction exceptions.

		///<summary>
		/// Thrown when a transaction deletes or updates a row that another
		/// transaction has committed a change to.
		///</summary>
		public const int RowRemoveClash = 1;

		///<summary>
		/// Thrown when a transaction drops or alters a table that another 
		/// transaction has committed a change to.
		///</summary>
		public const int TableRemoveClash = 2;

		///<summary>
		/// Thrown when a transaction adds/removes/modifies rows from a table 
		/// that has been dropped by another transaction.
		///</summary>
		public const int TableDropped = 3;

		///<summary>
		/// Thrown when a transaction selects data from a table that has committed
		/// changes to it from another transaction.
		///</summary>
		public const int DirtyTableSelect = 4;

		///<summary>
		/// Thrown when a transaction conflict occurs and would cause duplicate 
		/// tables to be created.
		///</summary>
		public const int DuplicateTable = 5;


		/// <summary>
		///  The type of error.
		/// </summary>
		private readonly int type;

		public TransactionException(int type, String message)
			: base(message) {
			this.type = type;
		}

		/// <summary>
		/// Returns the type of transaction error this is.
		/// </summary>
		public int Type {
			get { return type; }
		}
	}
}