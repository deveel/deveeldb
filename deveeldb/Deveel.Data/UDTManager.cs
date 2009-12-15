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
using Deveel.Data.Text;

namespace Deveel.Data {
	/// <summary>
	/// Manages the creation, removal and retrieval of UDTs.
	/// </summary>
	internal class UDTManager {
		public UDTManager(TableDataConglomerate conglomerate) {
			this.conglomerate = conglomerate;
		}

		private readonly TableDataConglomerate conglomerate;

		private const int TypeNotFound = -20;

		/// <summary>
		/// A map from the names (TableName) of the UDTs to the
		/// implementation (UserDefinedType).
		/// </summary>
		private static readonly Hashtable udt_map = new Hashtable();

		public static void CreateType(Transaction transaction, UserType type) {
			// If the UDT_TABLE or UDT_COLS_TABLE tables don't exist then 
			// we can't create the sequence generator
			if (!transaction.TableExists(TableDataConglomerate.UDT_TABLE) ||
				!transaction.TableExists(TableDataConglomerate.UDT_COLS_TABLE)) {
				throw new Exception("UDT tables do not exist.");
			}

			// the name of the type to create
			TableName typeName = type.Name;

			// the parent type (if defined)
			int parentTypeId = TypeNotFound;
			UserType parentType = type.ParentType;
			if (parentType != null)
				parentTypeId = GetTypeId(transaction, parentType.Name);

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
			row.SetColumnDataFromObject(3, (int)type.Attributes);
			if (parentTypeId != TypeNotFound)
				row.SetColumnDataFromObject(4, parentTypeId);		// parent type
			else
				row.SetColumnDataFromObject(4, null);
			if (type.IsExternal)
				row.SetColumnDataFromObject(5, type.ExternalTypeString);

			udt.AddRow(row);

			int count = type.MemberCount;
			for (int i = 0; i < count; i++) {
				UserTypeAttribute attribute = type.GetAttribute(i);

				int type_id = GetTTypeId(transaction, attribute.Type);

				RowData cols_row = new RowData(udtCols);
				cols_row.SetColumnDataFromObject(0, id);				// defining type id
				cols_row.SetColumnDataFromObject(1, attribute.Name);	// member name
				cols_row.SetColumnDataFromObject(2, type_id);			// member type
				cols_row.SetColumnDataFromObject(3, attribute.Size);	// type size
				cols_row.SetColumnDataFromObject(4, attribute.Scale);	// type scale (in case of numerics)
				cols_row.SetColumnDataFromObject(5, attribute.Nullable ? 1 : 0);	// nullable
				udtCols.AddRow(cols_row);
			}
		}

		public static void DropType(Transaction transaction, TableName typeName) {
			// If the UDT_TABLE or UDT_COLS_TABLE tables don't exist then 
			// we can't drop the type
			if (!transaction.TableExists(TableDataConglomerate.UDT_TABLE) ||
				!transaction.TableExists(TableDataConglomerate.UDT_COLS_TABLE)) {
				throw new Exception("System UDT tables do not exist.");
			}

			// The UDT_TABLE and UDT_COLS_TABLE table
			IMutableTableDataSource udt = transaction.GetTable(TableDataConglomerate.UDT_TABLE);
			IMutableTableDataSource udtCols = transaction.GetTable(TableDataConglomerate.UDT_COLS_TABLE);

			// get the id of the type
			int id = GetTypeId(transaction, typeName);

			// first we must check if the type we're trying to delete a type
			// referenced by other subtypes...
			using(SimpleTableQuery query = new SimpleTableQuery(udt)) {
				IntegerVector ivec = query.SelectEqual(4, id);
				if (ivec.Count > 0)
					throw new Exception("Cannot drop a type that is the parent of other types.");
			}

			using(SimpleTableQuery query = new SimpleTableQuery(udt)) {
				// remove all the columns referencing the type we have
				// are trying to delete
				using(SimpleTableQuery cols_query = new SimpleTableQuery(udtCols)) {
					cols_query.Delete(0, id);
				}

				// and finally delete the type itself
				query.Delete(0, id);
			}
		}

		public static UserType GetUserTypeDef(Transaction transaction, TableName typeName) {
			// If the UDT_TABLE or UDT_COLS_TABLE tables don't exist then 
			// we can't drop the type
			if (!transaction.TableExists(TableDataConglomerate.UDT_TABLE) ||
			    !transaction.TableExists(TableDataConglomerate.UDT_COLS_TABLE)) {
				throw new Exception("System UDT tables do not exist.");
			}

			UserType type = udt_map[typeName] as UserType;
			if (type != null)
				return type;

			// type unique identificator
			int id = GetTypeId(transaction, typeName);

			// The UDT_TABLE and UDT_COLS_TABLE table
			IMutableTableDataSource udt = transaction.GetTable(TableDataConglomerate.UDT_TABLE);
			IMutableTableDataSource udtCols = transaction.GetTable(TableDataConglomerate.UDT_COLS_TABLE);

			UserTypeAttributes attributes;
			UserType parentType = null;
			int parent_id = TypeNotFound;

			using(SimpleTableQuery query = new SimpleTableQuery(udt)) {
				// get the row identified by the id returned earlier
				IntegerVector ivec = query.SelectEqual(0, id);
				int row = ivec[0];

				// whether the type is final
				attributes = (UserTypeAttributes) query.Get(3, row).ToBigNumber().ToInt32();

				// the parent type (if any)
				TObject ob = query.Get(4, row);
				if (!ob.IsNull)
					parent_id = ob.ToBigNumber().ToInt32();
			}

			if (parent_id != TypeNotFound)
				parentType = GetUserTypeDef(transaction, parent_id);

			// finally we build the type...
			type = new UserType(parentType, typeName, attributes);

			using(SimpleTableQuery query = new SimpleTableQuery(udtCols)) {
				IntegerVector ivec = query.SelectEqual(0, parent_id);
				for (int i = 0; i < ivec.Count; i++) {
					string name = (string) query.Get(1, i);

					// the information used to rebuild the TType
					int type_id = (int) query.Get(2, i);
					int size = (int) query.Get(3, i);
					int scale = (int) query.Get(4, i);

					// reconstruct the type (whether is primitive or a UDT)
					TType ttype = GetTType(transaction, type_id, size, scale);

					// whether the member can accept nullable values
					bool nullable = (int) query.Get(5, i) == 1;

					// so we finally add the member
					type.AddAttribute(name, ttype, nullable);
				}
			}

			udt_map[typeName] = type;

			return type;
		}

		private static UserType GetUserTypeDef(Transaction transaction, int id) {
			TableName typeName;

			IMutableTableDataSource udt = transaction.GetTable(TableDataConglomerate.UDT_TABLE);

			using (SimpleTableQuery query = new SimpleTableQuery(udt)) {
				IntegerVector ivec = query.SelectEqual(0, id);
				if (ivec.Count == 0)
					throw new Exception("The type with id '" + id + "' was not found.");

				int row = ivec[0];

				string schema_name = (string) query.Get(1, row);
				string type_name = (string) query.Get(2, row);

				typeName = new TableName(schema_name, type_name);
			}

			return GetUserTypeDef(transaction, typeName);
		}

		private static TType GetTType(Transaction transaction, int typeId, int size, int scale) {
			if (typeId == -1)
				return TType.GetNumericType(SqlType.Numeric, size, scale);
			if (typeId == -2)
				return TType.GetStringType(SqlType.VarChar, size, null, CollationStrength.None, CollationDecomposition.None);
			if (typeId == -3)
				return TType.GetBooleanType(SqlType.Boolean);
			if (typeId == -4)
				return TType.GetBinaryType(SqlType.Binary, size);
			if (typeId == -5)
				return TType.GetDateType(SqlType.Date);
			if (typeId == -6)
				return TType.GetIntervalType(SqlType.Interval);

			// if it is not one of the primitive types, it must be one
			// of those defined by users...
			UserType type = GetUserTypeDef(transaction, typeId);
			return new TUserDefinedType(type);
		}

		private static int GetTTypeId(Transaction transaction, TType type) {
			// primitive types have an id smaller than 0 to avoid
			// being confused with UDTs, having an id assigned
			// dynamicaly
			if (type is TNumericType) return -1;
			if (type is TStringType) return -2;
			if (type is TBooleanType) return -3;
			if (type is TBinaryType) return -4;
			if (type is TDateType) return -5;
			if (type is TIntervalType) return -6;

			// if it is not a valid primitive type and it's not a
			// user-defined type it cannot be managed...
			if (!(type is TUserDefinedType))
				throw new ArgumentException();

			TUserDefinedType udt = (TUserDefinedType) type;

			// the name of the type to check
			TableName typeName = udt.UserType.Name;
			return GetTypeId(transaction, typeName);
		}

		private static int GetTypeId(Transaction transaction, TableName typeName) {
			if (typeName == UserType.NumericTypeName) return -1;
			if (typeName == UserType.StringTypeName) return -2;
			if (typeName == UserType.BooleanTypeName) return -3;
			if (typeName == UserType.BinaryTypeName) return -4;
			if (typeName == UserType.TimeTypeName) return -5;
			if (typeName == UserType.IntervalTypeName) return -6;

			// The UDT_TABLE
			IMutableTableDataSource udt_table = transaction.GetTable(TableDataConglomerate.UDT_TABLE);

			// let's check to see if another type with the same name 
			// already exists within this schema...
			int id;
			using (SimpleTableQuery query = new SimpleTableQuery(udt_table)) {
				IntegerVector ivec = query.SelectEqual(1, TObject.GetString(typeName.Schema), 2, TObject.GetString(typeName.Name));
				if (ivec.Count == 0)
					throw new Exception("User-defined type with name '" + typeName + "' not found.");

				TObject ob = query.Get(0, ivec[0]);
				if (ob.IsNull)
					throw new InvalidOperationException();

				id = ob.ToBigNumber().ToInt32();
			}

			return id;
		}

		public static bool TypeExists(Transaction transaction, TableName typeName) {
			// The UDT_TABLE and table
			IMutableTableDataSource udt = transaction.GetTable(TableDataConglomerate.UDT_TABLE);

			// let's check to see if another type with the same name 
			// already exists within this schema...
			using (SimpleTableQuery query = new SimpleTableQuery(udt)) {
				IntegerVector ivec = query.SelectEqual(1, TObject.GetString(typeName.Schema), 2, TObject.GetString(typeName.Name));
				return ivec.Count > 0;
			}
		}
	}
}