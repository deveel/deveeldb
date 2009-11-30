//  
//  UserDefinedType.cs
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
using System.Reflection;

namespace Deveel.Data.Mapping {
	/// <summary>
	/// The default implementation of <see cref="IUserDefinedType"/> that
	/// is backed by a <see cref="Type"/>.
	/// </summary>
	public sealed class UserDefinedType : IUserDefinedType {
		public UserDefinedType(Type type, string typeName) {
			this.type = type;
			BuildDataTableDef(typeName);
		}

		/// <summary>
		/// The <see cref="Type"/> that is used as definition
		/// for the UDT.
		/// </summary>
		private readonly Type type;

		/// <summary>
		/// The definition of the data-table represented by the UDT.
		/// </summary>
		private UserTypeDef tableDef;

		/// <summary>
		/// A dictionary containing a key/value pair set of the values
		/// 
		/// </summary>
		private Hashtable values;

		/// <summary>
		/// A map from the column name to a member of the underlying type.
		/// </summary>
		private Hashtable membersMap = new Hashtable();

		/// <summary>
		/// Builds the UDT by using reflection.
		/// </summary>
		/// <param name="typeName">The user defined type of the UDT. If this
		/// is <c>null</c>, the system will use the type name.</param>
		private void BuildDataTableDef(string typeName) {
			if (typeName == null)
				typeName = type.Name;

			tableDef = new UserTypeDef(new TableName(typeName), type.IsSealed);

			MemberInfo[] memberInfos = type.FindMembers(MemberTypes.Field | MemberTypes.Property,
			                                            BindingFlags.Instance | BindingFlags.Public, new MemberFilter(FilterMember), null);
			if (memberInfos.Length == 0)
				throw new InvalidOperationException("No public fields or properties where found in the type '" + type + "'.");

			for (int i = 0; i < memberInfos.Length; i++) {
				UserTypeMemberDef columnDef = BuildColumnDef(memberInfos[i]);
				tableDef.AddMember(columnDef);
			}

			tableDef.SetReadOnly();
		}

		private static UserTypeMemberDef BuildColumnDef(MemberInfo memberInfo) {
			throw new NotImplementedException();
		}

		private static bool FilterMember(MemberInfo memberInfo, object criteria) {
			if (Attribute.IsDefined(memberInfo, typeof(IgnoreAttribute)))
				return false;

			if (memberInfo is FieldInfo)
				return true;

			// we can only use readable and writeable properties...
			PropertyInfo propertyInfo = (PropertyInfo) memberInfo;
			return propertyInfo.CanRead && propertyInfo.CanWrite;
		}

		#region Implementation of IUserDefinedType

		public UserTypeDef TypeDef {
			get { return tableDef; }
		}

		public object GetValue(int index) {
			throw new NotImplementedException();
		}

		public void SetValue(int index, object value) {
			throw new NotImplementedException();
		}

		#endregion

		/// <summary>
		/// Converts the UDT into an instance of the underlying type.
		/// </summary>
		/// <returns>
		/// Returns an <see cref="object"/> that is an instance of the
		/// underlying <see cref="Type"/>, initialized with the field and
		/// property values stored in this instance.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the underlying <see cref="Type"/> of this UDT does not
		/// define 
		/// </exception>
		public object ToType() {
			object ob;

			try {
				ob = Activator.CreateInstance(type, true);
			} catch(Exception) {
				throw new InvalidOperationException("Unable to build a new instance of '" + type + "' type.");
			}

			//TODO:

			return ob;
		}
	}
}