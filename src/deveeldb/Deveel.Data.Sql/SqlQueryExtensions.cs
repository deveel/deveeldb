using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql {
	public static class SqlQueryExtensions {
		public static QueryParameter FindParameter(this ICollection<QueryParameter> collection, string paramName) {
			return collection.FirstOrDefault(x => x.Name == paramName);
		} 
	}
}
