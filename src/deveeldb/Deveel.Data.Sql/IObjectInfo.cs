using System;

namespace Deveel.Data.Sql {
	public interface IObjectInfo {
		DbObjectType ObjectType { get; }

		ObjectName FullName { get; }
	}
}
