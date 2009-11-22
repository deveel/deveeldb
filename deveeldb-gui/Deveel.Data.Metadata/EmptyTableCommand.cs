using System;
using System.Data.Common;
using System.Windows.Forms;

using Deveel.Data.Client;
using Deveel.Data.Commands;

namespace Deveel.Data.Metadata {
	[Command("EmptyTable", "Empty Table")]
	[CommandImage("Deveel.Data.Images.table_delete.png")]
	public sealed class EmptyTableCommand : Command {
		#region Overrides of Command

		public override void Execute() {
			IHostWindow hostWindow = Services.HostWindow;
			string tableName = hostWindow.MetadataProvider.SelectedTable;

			if (tableName != null) {
				DialogResult result = hostWindow.DisplayMessageBox(hostWindow.Form,
				                                                   "This will delete all rows in the table '" + tableName +
				                                                   "': are you sure you want to continue?",
				                                                   "Confirm Empty Table", MessageBoxButtons.YesNoCancel,
				                                                   MessageBoxIcon.Question, MessageBoxDefaultButton.Button2, 0, null,
				                                                   null);
				if (result != DialogResult.Yes)
					return;

				DeveelDbConnection dbConnection;
				DeveelDbCommand cmd = null;

				try {
					hostWindow.SetPointerState(Cursors.WaitCursor);
					dbConnection = Settings.Connection;
					cmd = dbConnection.CreateCommand("DELETE FROM " + tableName);
					cmd.ExecuteNonQuery();
				} catch (DbException dbExp) {
					hostWindow.DisplaySimpleMessageBox(null, dbExp.Message, "Error");
				} catch (InvalidOperationException invalidExp) {
					hostWindow.DisplaySimpleMessageBox(null, invalidExp.Message, "Error");
				} finally {
					if (cmd != null)
						cmd.Dispose();

					hostWindow.SetPointerState(Cursors.Default);
				}
			}
		}

		#endregion
	}
}