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
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql {
	public static class QueryExtensions {
		public static bool ObjectExists(this IQuery query, ObjectName objectName) {
			if (query.Context.VariableExists(objectName.Name))
				return true;
			if (query.Context.VariableExists(objectName.Name))
				return true;

			return query.Session.ObjectExists(objectName);
		}

		public static bool ObjectExists(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor &&
			    query.Context.CursorExists(objectName.Name))
				return true;
			if (objectType == DbObjectType.Variable &&
			    query.Context.VariableExists(objectName.Name))
				return true;

			return query.Session.ObjectExists(objectType, objectName);
		}

		public static IDbObject GetObject(this IQuery query, DbObjectType objType, ObjectName objName) {
			return GetObject(query, objType, objName, AccessType.ReadWrite);
		}

		public static IDbObject GetObject(this IQuery query, DbObjectType objType, ObjectName objName, AccessType accessType) {
			if (objType == DbObjectType.Cursor)
				return query.Context.FindCursor(objName.Name);
			if (objType == DbObjectType.Variable)
				return query.Context.FindVariable(objName.Name);

			// TODO: throw a specialized exception
			if (!query.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return query.Session.GetObject(objType, objName, accessType);
		}

		public static void CreateObject(this IQuery query, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!query.UserCanCreateObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			query.Session.CreateObject(objectInfo);
		}

		public static bool DropObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Cursor)
				return query.Context.DropCursor(objectName.Name);
			if (objectType == DbObjectType.Variable)
				return query.Context.DropVariable(objectName.Name);

			if (!query.UserCanDropObject(objectType, objectName))
				throw new MissingPrivilegesException(query.UserName(), objectName, Privileges.Drop);

			query.Session.DropObject(objectType, objectName);
			return true;
		}

		public static void AlterObject(this IQuery query, IObjectInfo objectInfo) {
			if (objectInfo == null)
				throw new ArgumentNullException("objectInfo");

			if (objectInfo.ObjectType == DbObjectType.Cursor ||
				objectInfo.ObjectType == DbObjectType.Variable) {
				throw new NotSupportedException();
			}

			if (!query.UserCanAlterObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new MissingPrivilegesException(query.UserName(), objectInfo.FullName, Privileges.Alter);

			query.Session.AlterObject(objectInfo);
		}

		public static ObjectName ResolveObjectName(this IQuery query, string name) {
			if (query.Context.CursorExists(name))
				return new ObjectName(name);
			if (query.Context.VariableExists(name))
				return new ObjectName(name);

			return query.Session.ResolveObjectName(name);
		}

		public static ObjectName ResolveObjectName(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			if (objectType == DbObjectType.Variable  &&
				query.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);
			if (objectType == DbObjectType.Cursor &&
				query.Context.VariableExists(objectName.Name))
				return new ObjectName(objectName.Name);

			return query.Session.ResolveObjectName(objectType, objectName);
		}

		public static IDbObject FindObject(this IQuery query, ObjectName objectName) {
			if (query.Context.CursorExists(objectName.Name))
				return query.Context.FindCursor(objectName.Name);
			if (query.Context.VariableExists(objectName.Name))
				return query.Context.FindVariable(objectName.Name);

			return query.Session.FindObject(objectName);
		}
	}
}