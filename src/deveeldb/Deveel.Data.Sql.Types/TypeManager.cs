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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Types {
	public sealed class TypeManager : IObjectManager, ITypeResolver, ISystemCreateCallback {
		public TypeManager(ITransaction transaction) {
			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		public void Dispose() {
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Type; }
		}

		private static readonly ObjectName TypeTableName = new ObjectName(SystemSchema.SchemaName, "type");
		private static readonly ObjectName TypeMemberTableName = new ObjectName(SystemSchema.SchemaName, "type_member");

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemCreate)
				Create();
		}

		public void Create() {
			var tableInfo = new TableInfo(TypeTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("schema", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("parent", PrimitiveTypes.String());
			tableInfo.AddColumn("sealed", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("owner", PrimitiveTypes.String());
			Transaction.CreateTable(tableInfo);

			tableInfo = new TableInfo(TypeMemberTableName);
			tableInfo.AddColumn("type_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			Transaction.CreateTable(tableInfo);

			Transaction.AddPrimaryKey(TypeTableName, new [] {"id"}, "PK_TYPE");
			Transaction.AddForeignKey(TypeMemberTableName, new []{"type_id"}, TypeTableName, new[] {"id"}, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, "FK_MEMBER_TYPE");
		}

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
			row.SetValue(5, typeInfo.Owner);

			typeTable.AddRow(row);

			for (int i = 0; i < typeInfo.MemberCount; i++) {
				var member = typeInfo[i];

				row = typeMemberTable.NewRow();
				row.SetValue(0, id);
				row.SetValue(1, member.MemberName);
				row.SetValue(2, member.MemberType.ToString());

				typeMemberTable.AddRow(row);
			}
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
			// TODO:
			return false;
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
			var fullTypeName = Transaction.ResolveObjectName(context.TypeName);
			if (fullTypeName == null)
				return null;

			return GetUserType(fullTypeName);
		}

		public UserType GetUserType(ObjectName typeName) {
			throw new NotImplementedException();
		}
	}
}
