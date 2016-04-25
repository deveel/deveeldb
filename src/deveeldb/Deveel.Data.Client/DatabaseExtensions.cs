// 
//  Copyright 2010-2016 Deveel
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
using System.Data;

using Deveel.Data;
using Deveel.Data.Configuration;
using Deveel.Data.Protocol;
using Deveel.Data.Sql;

namespace Deveel.Data.Client {
	public static class DatabaseExtensions {
		public static IDbConnection CreateDbConnection(this IDatabase database, string userName, string password) {
			return CreateDbConnection(database, userName, password, new Configuration.Configuration());
		}

		public static IDbConnection CreateDbConnection(this IDatabase database, string userName, string password, IConfiguration configuration) {
			if (configuration == null)
				throw new ArgumentNullException("configuration");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			configuration.SetValue("connection.userName", userName);
			configuration.SetValue("connection.password", password);

			return database.CreateDbConnection(configuration);
		}

		public static IDbConnection CreateDbConnection(this IDatabase database, IConfiguration configuration) {
			if (database == null)
				throw new ArgumentNullException("database");
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			var dbName = configuration.GetString("database.name");
			if (!String.IsNullOrEmpty(dbName) &&
			    !String.Equals(dbName, database.Name))
				throw new ArgumentException(String.Format("The specified database name '{0}' differs from the original database name '{1}'.", 
					dbName, database.Name));

			var dbHandler = database.System as IDatabaseHandler;
			if (dbHandler == null)
				dbHandler = new SingleDatabaseHandler(database);

			var serverConnector = new EmbeddedServerConnector(dbHandler);
			var clientConnector = new EmbeddedClientConnector(serverConnector);

			var settings = BuildConnectionString(configuration);
			if (String.IsNullOrEmpty(settings.Database))
				settings.Database = database.Name;

			return new DeveelDbConnection(clientConnector, settings);
		}


		private static DeveelDbConnectionStringBuilder BuildConnectionString(IConfiguration configuration) {
			var settings = new DeveelDbConnectionStringBuilder();

			foreach (var pair in configuration) {
				try {
					switch (pair.Key.ToUpperInvariant()) {
						case "DATABASE.NAME":
						case "DBNAME":
							settings.Database = configuration.GetString(pair.Key);
							break;
						case "USERNAME":
						case "USERID":
						case "USER.NAME":
						case "USER.ID":
						case "CONNECTION.USERNAME":
						case "CONNECTION.USER":
						case "CONNECTION.USERID":
							settings.UserName = configuration.GetString(pair.Key);
							break;
						case "PASSWORD":
						case "PASS":
						case "USER.PASSWORD":
						case "SECRET":
						case "CONNECTION.PASSWORD":
						case "CONNECTION.PASS":
							settings.Password = configuration.GetString(pair.Key);
							break;
						case "PARAMETERSTYLE":
						case "PARAMSTYLE":
						case "CONNECTION.PARAMETERSTYLE":
							settings.ParameterStyle = configuration.GetValue<QueryParameterStyle>(pair.Key);
							break;
						case "IGNORECASE":
						case "CONNECTION.IGNORECASE":
						case "IGNOREIDENTIFIERSCASE":
						case "CONNECTION.IGNOREIDENTIFIERSCASE":
							settings.IgnoreIdentifiersCase = configuration.GetBoolean(pair.Key);
							break;
					}
				} catch (Exception ex) {
					throw new ArgumentException(String.Format("An error occurred while setting the key '{0}' into the connection string.", pair.Key));
				}
			}

			return settings;
		}

		private static QueryParameterStyle ParseParameterStyle(object value) {
			if (value is QueryParameterStyle)
				return (QueryParameterStyle) value;

			return (QueryParameterStyle) Enum.Parse(typeof(QueryParameterStyle), Convert.ToString(value), true);
		}
	}
}
