using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public interface IObjectManagerResolver {
		IEnumerable<IObjectManager> GetManagers();
			
		IObjectManager ResolveForType(DbObjectType objType);
	}
}
