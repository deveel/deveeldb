using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.Types {
	public sealed class UserTypeManager : IObjectManager, IUserTypeResolver {
		public UserTypeManager(ITransaction transaction) {
			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		public void Dispose() {
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Type; }
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

		UserType IUserTypeResolver.ResolveType(ObjectName typeName) {
			return GetUserType(typeName);
		}

		public UserType GetUserType(ObjectName typeName) {
			throw new NotImplementedException();
		}
	}
}
