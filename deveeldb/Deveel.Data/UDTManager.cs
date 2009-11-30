//  
//  UDTManager.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// Manages the creation, removal and retrieval of UDTs.
	/// </summary>
	internal class UDTManager {
		/// <summary>
		/// The TableDataConglomerate object.
		/// </summary>
		private readonly TableDataConglomerate conglomerate;

		/// <summary>
		/// A map from the names (TableName) of the UDTs to the
		/// implementation (UserDefinedType).
		/// </summary>
		private Hashtable udt_map;

		public UDTManager(TableDataConglomerate conglomerate) {
			this.conglomerate = conglomerate;
			udt_map = new Hashtable();
		}


		/// <summary>
		/// Returns a new Transaction object for manipulating and querying the system state.
		/// </summary>
		private Transaction GetTransaction() {
			// Should this transaction be optimized for the access patterns we generate
			// here?
			return conglomerate.CreateTransaction();
		}

		public static void CreateType(Transaction transaction, UserTypeDef tableDef) {
			// If the UDT_TABLE or UDT_COLS_TABLE tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.UDT_TABLE) ||
				!transaction.TableExists(TableDataConglomerate.UDT_COLS_TABLE)) {
				throw new Exception("UDT tables do not exist.");
			}

			// the name of the type to create
			TableName typeName = tableDef.Name;

			// The UDT_TABLE and UDT_COLS_TABLE table
			IMutableTableDataSource udt = transaction.GetTable(TableDataConglomerate.UDT_TABLE);
			IMutableTableDataSource udtCols = transaction.GetTable(TableDataConglomerate.UDT_COLS_TABLE);

			// let's check to see if another type with the same name 
			// already exists within this schema...
			using (SimpleTableQuery query = new SimpleTableQuery(udt)) {
				IntegerVector ivec = query.SelectEqual(1, TObject.GetString(typeName.Schema), 2, TObject.GetString(typeName.Name));
				if (ivec.Count > 0)
					throw new Exception("User-defined type with name '" + typeName + "' already exists.");
			}

			// Generate a unique id for the type
			long id = transaction.NextUniqueID(TableDataConglomerate.UDT_TABLE);

			// insert a new row for the type
			RowData row = new RowData(udt);
			row.SetColumnDataFromObject(0, id);
			row.SetColumnDataFromObject(1, typeName.Schema);
			row.SetColumnDataFromObject(2, typeName.Name);
			row.SetColumnDataFromObject(3, 0);      // final/not final: reserved for future use...
			row.SetColumnDataFromObject(4, -1);		// parent type: reserved for future use...

			int count = tableDef.MemberCount;
			for (int i = 0; i < count; i++) {
				UserTypeMemberDef memberDef = tableDef[i];
				//TODO:
			}
		}

		private class UserDefinedType : IUserDefinedType {
			#region Implementation of IUserDefinedType

			public UserTypeDef TypeDef {
				get { throw new NotImplementedException(); }
			}

			public object GetValue(int index) {
				throw new NotImplementedException();
			}

			public void SetValue(int index, object value) {
				throw new NotImplementedException();
			}

			#endregion
		}
	}
}