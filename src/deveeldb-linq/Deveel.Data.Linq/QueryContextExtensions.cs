using System;
using System.Linq;

using Deveel.Data.Mapping;

using IQToolkit.Data;

namespace Deveel.Data.Linq {
	public static class QueryContextExtensions {
		public static  IQueryProvider GetQueryProvider(this IQueryContext context, MappingModel model) {
			var queryMapping = model.CreateQueryMapping();
			return new DeveelDbProvider(context, queryMapping, new EntityPolicy());
		}
	}
}
