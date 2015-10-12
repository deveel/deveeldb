using System;

namespace Deveel.Data {
	 class SingleDatabaseHandler : IDatabaseHandler {
		 private readonly IDatabase database;

		 public SingleDatabaseHandler(IDatabase database) {
			 if (database == null)
				 throw new ArgumentNullException("database");

			 this.database = database;
		 }

		 public IDatabase GetDatabase(string databaseName) {
			 if (String.IsNullOrEmpty(databaseName))
				 throw new ArgumentNullException("databaseName");

			 if (!String.Equals(databaseName, database.Name()))
				 return null;

			 return database;
		 }
	 }
}
