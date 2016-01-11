using System;
using System.Data.Common;
using Deveel.Data;

namespace Deveel.Data.Client {
	public class DeveelDbClientFactory : DbProviderFactory {
		public static DeveelDbClientFactory Instance = new DeveelDbClientFactory();

		/// <summary>
		/// Returns a strongly typed <see cref="DbCommandBuilder"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbCommandBuilder</b>.</returns>
		public override DbCommandBuilder CreateCommandBuilder() {
			return new DeveelDbCommandBuilder();
		}

		/// <summary>
		/// Returns a strongly typed <see cref="DbDataAdapter"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbDataAdapter</b>. </returns>
		public override DbDataAdapter CreateDataAdapter() {
			return new DeveelDbDataAdapter();
		}

		/// <summary>
		/// Returns a strongly typed <see cref="DbConnection"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbConnection</b>.</returns>
		public override DbConnection CreateConnection() {
			return new DeveelDbConnection();
		}

		/// <summary>
		/// Returns a strongly typed <see cref="DbCommand"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbCommand</b>.</returns>
		public override DbCommand CreateCommand() {
			return new DeveelDbCommand();
		}

		/// <summary>
		/// Returns a strongly typed <see cref="DbParameter"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbParameter</b>.</returns>
		public override DbParameter CreateParameter() {
			return new DeveelDbParameter();
		}
		/// <summary>
		/// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance. 
		/// </summary>
		/// <returns>A new strongly typed instance of <b>DbConnectionStringBuilder</b>.</returns>
		public override DbConnectionStringBuilder CreateConnectionStringBuilder() {
			return new DeveelDbConnectionStringBuilder();
		}
		/// <summary>
		/// Returns true if a <b>DeveelDbDataSourceEnumerator</b> can be created; 
		/// otherwise false. 
		/// </summary>
		public override bool CanCreateDataSourceEnumerator {
			get {
				return false;
				// new DeveelDbDataSourceEnumerator ();
			}
		}
	}
}