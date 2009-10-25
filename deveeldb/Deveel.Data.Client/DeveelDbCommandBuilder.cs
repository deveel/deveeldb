using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommandBuilder : DbCommandBuilder {
		public DeveelDbCommandBuilder() {
		}

		public DeveelDbCommandBuilder(DeveelDbDataAdapter adapter) {
			DataAdapter = adapter;
		}

		public override string QuotePrefix {
			get { return "\""; }
			set {
				if (value != "\"")
					throw new ArgumentException();
			}
		}

		public override string QuoteSuffix {
			get { return "\""; }
			set {
				if (value != "\"")
					throw new ArgumentException();
			}
		}

		public new DeveelDbDataAdapter DataAdapter {
			get { return (DeveelDbDataAdapter) base.DataAdapter; }
			set { base.DataAdapter = value; }
		}

		public new DeveelDbCommand GetDeleteCommand() {
			return (DeveelDbCommand)base.GetDeleteCommand();
		}

		/// <summary>
		/// Gets the update command.
		/// </summary>
		/// <returns></returns>
		public new DeveelDbCommand GetUpdateCommand() {
			return (DeveelDbCommand)base.GetUpdateCommand();
		}

		/// <summary>
		/// Gets the insert command.
		/// </summary>
		/// <returns></returns>
		public new DeveelDbCommand GetInsertCommand() {
			return (DeveelDbCommand)GetInsertCommand(false);
		}

		public override string QuoteIdentifier(string unquotedIdentifier) {
			if (unquotedIdentifier == null)
				throw new ArgumentNullException("unquotedIdentifier");

			if (unquotedIdentifier[0] == '\"' &&
				unquotedIdentifier[unquotedIdentifier.Length - 1] == '\"')
				return unquotedIdentifier;

			// prevent the quoted identifier to have " in it...
			unquotedIdentifier.Replace("\"", "\"\"");

			return "\"" + unquotedIdentifier + "\"";
		}

		public override string UnquoteIdentifier(string quotedIdentifier) {
			if (quotedIdentifier == null)
				throw new ArgumentNullException("quotedIdentifier");

			if (quotedIdentifier[0] != '\"' ||
				quotedIdentifier[quotedIdentifier.Length - 1] != '\"')
				return quotedIdentifier;

			quotedIdentifier = quotedIdentifier.Substring(1, quotedIdentifier.Length - 2);
			quotedIdentifier = quotedIdentifier.Replace("\"\"", "\"");
			return quotedIdentifier;
		}

		protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause) {
			//TODO:
			DeveelDbParameter dbParameter = (DeveelDbParameter) parameter;
			dbParameter.SqlType = (SQLTypes) row["DATA_TYPE"];
		}

		protected override string GetParameterName(int parameterOrdinal) {
			return "@" + parameterOrdinal;
		}

		protected override string GetParameterName(string parameterName) {
			// TODO: should we check for invalid characters here?
			return "@" + parameterName;
		}

		protected override string GetParameterPlaceholder(int parameterOrdinal) {
			return "?";
		}

		protected override void SetRowUpdatingHandler(DbDataAdapter adapter) {
			DeveelDbDataAdapter dbAdapter = (DeveelDbDataAdapter)adapter;
			if (adapter != base.DataAdapter)
				dbAdapter.RowUpdating += new DeveelDbRowUpdatingEventHandler(OnRowUpdating);
			else
				dbAdapter.RowUpdating -= new DeveelDbRowUpdatingEventHandler(OnRowUpdating);

		}

		private void OnRowUpdating(object sender, DeveelDbRowUpdatingEventArgs args) {
			base.RowUpdatingHandler(args);
		}
	}
}