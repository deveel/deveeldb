using System;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public interface IObjectManager : IDisposable {
		DbObjectType ObjectType { get; }

		void CreateObject(IObjectInfo objInfo);

		bool RealObjectExists(ObjectName objName);

		bool ObjectExists(ObjectName objName);

		IDbObject GetObject(ObjectName objName);

		bool AlterObject(IObjectInfo objInfo);

		bool DropObject(ObjectName objName);
	}
}
