using System;

using Deveel.Data.Client;
using Deveel.Data.DbModel;

namespace Deveel.Data {
	public interface ISettings {
		event EventHandler ConnectionReset;

		event EventHandler ConnectionStringsChanged;


		DbConnectionString ConnectionString { get; }

		DbConnectionStrings ConnectionStrings { get; set; }

		DeveelDbConnection Connection { get; }


		void SetProperty(string key, object value);

		object GetProperty(string key);

		void ResetConnection();

		void CloseConnection();

		int CountUntitled();
	}
}