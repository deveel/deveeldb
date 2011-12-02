using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;

using Deveel.Data.Client;

namespace Deveel.Data.Entity {
	internal class DeveelDbProviderServices : DbProviderServices {
		protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, DbCommandTree commandTree) {
			if (commandTree == null)
				throw new ArgumentNullException("commandTree");

			ExpressionVisitor visitor;
			if (commandTree is DbQueryCommandTree)
				visitor = new SelectExpressionVisitor();
			//TODO: continue...
			else
				throw new ArgumentException();

			string sqlCommandText = visitor.BuildSqlStatement(commandTree);
			DeveelDbCommand cmd = new DeveelDbCommand(sqlCommandText);


			//TODO: support for function calls?

			foreach (KeyValuePair<string, TypeUsage> queryParameter in commandTree.Parameters) {
				DbParameter parameter = cmd.CreateParameter();
				parameter.ParameterName = queryParameter.Key;
				parameter.Direction = ParameterDirection.Input;
				parameter.DbType = Metadata.GetDbType(queryParameter.Value);
				cmd.Parameters.Add(parameter);
			}

			foreach (DbParameter p in visitor.Parameters)
				cmd.Parameters.Add(p);

			return CreateCommandDefinition(cmd);
		}

		protected override string GetDbProviderManifestToken(DbConnection connection) {
			bool toClose = false;
			if (connection.State == ConnectionState.Closed) {
				connection.Open();
				toClose = true;
			}

			string version = connection.ServerVersion;

			if (toClose)
				connection.Close();

			if (version != "1")
				throw new InvalidOperationException();

			return "1";
		}

		protected override DbProviderManifest GetDbProviderManifest(string manifestToken) {
			if (manifestToken != "1")
				throw new ArgumentException();

			return new DeveelDbProviderManifest();
		}
	}
}