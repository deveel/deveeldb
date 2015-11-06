using System;
using System.Data.Common;
using System.Linq;

using Deveel.Data.Client;

using IQToolkit.Data;

namespace Deveel.Data.Linq {
	public static class DatabaseExtensions {
		public static IQueryProvider GetQueryProvider(this IDatabase database, ProviderSettings settings) {
			var userName = settings.UserName;
			var password = settings.Password;
			var connection = (DbConnection) database.CreateDbConnection(userName, password);
			var queryMapping = settings.MappingModel.CreateQueryMapping();

			return new DeveelDbProvider(connection, queryMapping, new EntityPolicy());
		}
	}
}
