// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Types {
	public sealed class TypeManager : IObjectManager, ITypeResolver {
		public TypeManager(ITransaction transaction) {
			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		public void Dispose() {
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Type; }
		}

		public static readonly ObjectName TypeTableName = new ObjectName(SystemSchema.SchemaName, "type");
		public static readonly ObjectName TypeMemberTableName = new ObjectName(SystemSchema.SchemaName, "type_member");

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			CreateType((UserTypeInfo) objInfo);
		}

		public void CreateType(UserTypeInfo typeInfo) {
			if (typeInfo == null)
				throw new ArgumentNullException("typeInfo");

			var id = Transaction.NextTableId(TypeTableName);

			var typeTable = Transaction.GetMutableTable(TypeTableName);
			var typeMemberTable = Transaction.GetMutableTable(TypeMemberTableName);

			var parentName = typeInfo.ParentType != null ? typeInfo.ParentType.ToString() : null;

			var row = typeTable.NewRow();
			row.SetValue(0, id);
			row.SetValue(1, typeInfo.TypeName.ParentName);
			row.SetValue(2, typeInfo.TypeName.Name);
			row.SetValue(3, parentName);
			row.SetValue(4, typeInfo.IsSealed);
			row.SetValue(5, typeInfo.IsAbstract);
			row.SetValue(6, typeInfo.Owner);

			typeTable.AddRow(row);

			for (int i = 0; i < typeInfo.MemberCount; i++) {
				var member = typeInfo[i];

				row = typeMemberTable.NewRow();
				row.SetValue(0, id);
				row.SetValue(1, member.MemberName);
				row.SetValue(2, member.MemberType.ToString());

				typeMemberTable.AddRow(row);
			}

			Transaction.OnObjectCreated(DbObjectType.Type, typeInfo.TypeName);
		}

		public bool TypeExists(ObjectName typeName) {
			var table = Transaction.GetTable(TypeTableName);

			var schemaName = typeName.ParentName;
			var name = typeName.Name;

			var schemaCol = table.GetResolvedColumnName(1);
			var nameCol = table.GetResolvedColumnName(2);

			using (var session = new SystemSession(Transaction)) {
				using (var query = session.CreateQuery()) {
					var t = table.SimpleSelect(query, schemaCol, SqlExpressionType.Equal, SqlExpression.Constant(schemaName));

					t = t.ExhaustiveSelect(query, SqlExpression.Equal(SqlExpression.Reference(nameCol), SqlExpression.Constant(name)));

					if (t.RowCount > 1)
						throw new InvalidOperationException(String.Format("Name '{0}' resolves to multiple types.", typeName));

					return t.RowCount == 1;
				}
			}
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return TypeExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return TypeExists(objName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetUserType(objName);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			return AlterType((UserTypeInfo) objInfo);
		}

		public bool AlterType(UserTypeInfo typeInfo) {
			if (!DropType(typeInfo.TypeName))
				return false;

			CreateType(typeInfo);
			return true;
		}

		public bool DropType(ObjectName typeName) {
			var typeTable = Transaction.GetMutableTable(TypeTableName);
			var memberTable = Transaction.GetMutableTable(TypeMemberTableName);

			var schemaName = typeName.ParentName;
			var name = typeName.Name;

			var list = typeTable.SelectRowsEqual(2, Field.VarChar(name), 1, Field.VarChar(schemaName));

			bool deleted = false;

			foreach (var rowIndex in list) {
				var id = typeTable.GetValue(rowIndex, 0);

				var list2 = memberTable.SelectRowsEqual(0, id);
				foreach (var rowIndex2 in list2) {
					memberTable.RemoveRow(rowIndex2);
				}

				typeTable.RemoveRow(rowIndex);
				deleted = true;
			}

			return deleted;
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropType(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			var table = Transaction.GetTable(TypeTableName);

			var schemaName = objName.ParentName;
			var name = objName.Name;
			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

			foreach (var row in table) {
				var schemaValue = row.GetValue(1).Value.ToString();
				var nameValue = row.GetValue(2).Value.ToString();

				if (!String.Equals(name, nameValue, comparison) ||
					!String.Equals(schemaName, schemaValue, comparison))
					continue;

				return new ObjectName(ObjectName.Parse(schemaValue), nameValue);
			}

			return null;
		}

		SqlType ITypeResolver.ResolveType(TypeResolveContext context) {
			var fullTypeName = ResolveName(ObjectName.Parse(context.TypeName), true);
			if (fullTypeName == null)
				return null;

			return GetUserType(fullTypeName);
		}

		public UserType GetUserType(ObjectName typeName) {
			var typeTable = Transaction.GetTable(TypeTableName);
			var membersTable = Transaction.GetTable(TypeMemberTableName);

			var schemaName = typeName.ParentName;
			var name = typeName.Name;

			var schemaColumn = typeTable.GetResolvedColumnName(1);
			var nameColumn = typeTable.GetResolvedColumnName(2);

			var idColumn = membersTable.GetResolvedColumnName(0);

			UserTypeInfo typeInfo;

			using (var session = new SystemSession(Transaction)) {
				using (var query = session.CreateQuery()) {
					var t = typeTable.SimpleSelect(query, schemaColumn, SqlExpressionType.Equal, SqlExpression.Constant(schemaName));

					t = t.ExhaustiveSelect(query,
						SqlExpression.Equal(SqlExpression.Reference(nameColumn), SqlExpression.Constant(name)));

					if (t.RowCount == 0)
						return null;

					var id = t.GetValue(0, 0);

					var parentField = t.GetValue(0, 3);
					ObjectName parentType = null;
					if (!Field.IsNullField(parentField)) {
						parentType = ObjectName.Parse(parentField.Value.ToString());
					}

					typeInfo = new UserTypeInfo(typeName, parentType);


					var isSealedField = t.GetValue(0, 4);
					var isAbstractField = t.GetValue(0, 5);

					if (!Field.IsNullField(isSealedField)) {
						typeInfo.IsSealed = (SqlBoolean) isSealedField.AsBoolean().Value;
					}

					if (!Field.IsNullField(isAbstractField)) {
						typeInfo.IsAbstract = (SqlBoolean) isAbstractField.AsBoolean().Value;
					}

					var owner = t.GetValue(0, 6).Value.ToString();

					typeInfo.Owner = owner;

					var t2 = membersTable.SimpleSelect(query, idColumn, SqlExpressionType.Equal, SqlExpression.Constant(id));

					foreach (var row in t2) {
						var memberName = row.GetValue(1).Value.ToString();
						var memberTypeString = row.GetValue(2).Value.ToString();

						var memberType = SqlType.Parse(Transaction.Context, memberTypeString);

						if (memberType == null)
							throw new InvalidOperationException(String.Format("Cannot find the type '{0}' for member '{1}' of type '{2}'.",
								memberTypeString, memberName, typeName));

						typeInfo.AddMember(memberName, memberType);
					}
				}
			}

			return new UserType(typeInfo);
		}

		public IEnumerable<ObjectName> GetChildTypes(ObjectName typeName) {
			// TODO:
			return new ObjectName[0];
		}
	}
}
