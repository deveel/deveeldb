// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.Types {
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

		public void Create() {
			// TODO:
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			throw new NotImplementedException();
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			throw new NotImplementedException();
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			throw new NotImplementedException();
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetUserType(objName);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotImplementedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			throw new NotImplementedException();
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			throw new NotImplementedException();
		}

		DataType ITypeResolver.ResolveType(TypeResolveContext context) {
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
