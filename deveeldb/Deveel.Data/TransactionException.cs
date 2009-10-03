// 
//  TransactionException.cs
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

namespace Deveel.Data {
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