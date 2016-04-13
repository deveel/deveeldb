using System;

namespace Deveel.Data.Sql.Types {
	public interface IDataTypeParser {
		DataTypeInfo Parse(string s);
	}
}
