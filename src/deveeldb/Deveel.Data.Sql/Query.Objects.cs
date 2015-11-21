using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql {
	public static class QueryExtensions {
		public static bool ObjectExists(this IQuery query, ObjectName objectName) {
			return query.Session.ObjectExists(objectName);
		}

		public static bool ObjectExists(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.Session.ObjectExists(objectType, objectName);
		}

		public static IDbObject GetObject(this IQuery query, DbObjectType objType, ObjectName objName) {
			return GetObject(query, objType, objName, AccessType.ReadWrite);
		}

		public static IDbObject GetObject(this IQuery query, DbObjectType objType, ObjectName objName, AccessType accessType) {
			// TODO: throw a specialized exception
			if (!query.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return query.Session.GetObject(objType, objName, accessType);
		}

		internal static void CreateObject(this IQuery query, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!query.UserCanCreateObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			query.Session.CreateObject(objectInfo);
		}

		public static bool DropObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			if (!query.UserCanDropObject(objectType, objectName))
				throw new MissingPrivilegesException(query.UserName(), objectName, Privileges.Drop);

			query.Session.DropObject(objectType, objectName);
			return true;
		}

		public static void AlterObject(this IQuery query, IObjectInfo objectInfo) {
			if (objectInfo == null)
				throw new ArgumentNullException("objectInfo");

			if (!query.UserCanAlterObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new MissingPrivilegesException(query.UserName(), objectInfo.FullName, Privileges.Alter);

			query.Session.AlterObject(objectInfo);
		}

		public static ObjectName ResolveObjectName(this IQuery query, string name) {
			return query.Session.ResolveObjectName(name);
		}

		public static ObjectName ResolveObjectName(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.Session.ResolveObjectName(objectType, objectName);
		}

		public static IDbObject FindObject(this IQuery query, ObjectName objectName) {
			return query.Session.FindObject(objectName);
		}
	}
}