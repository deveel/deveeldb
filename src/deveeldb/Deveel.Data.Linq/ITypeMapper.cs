using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq {
	public interface ITypeMapper : ITypeResolver {
		SqlType MapToSqlType(Type type);
	}
}
