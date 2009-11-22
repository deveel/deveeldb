using System;
using System.IO;
using System.Windows.Forms;

using Deveel.Data.Client;
using Deveel.Data.DbModel;

namespace Deveel.Data.ConnectionStrings {
	public partial class ConnectionStringForm : Form {
		public ConnectionStringForm() {
			InitializeComponent();
		}

		public ConnectionStringForm(IApplicationServices services)
			: this() {
			this.services = services;
		}

		private readonly IApplicationServices services;
		private string connFileName;
		private DbConnectionStrings connStrings;
		private bool dirty;
		private int unnamedConns = -1;

		private void ConnectionStringForm_Shown(object sender, EventArgs e) {
			connFileName = (string) services.Settings.GetProperty(SettingsProperties.ConnectionStringsFile);
			if (connFileName == null) {
				services.HostWindow.DisplayMessageBox(this, "No configuration file specified for the: please review the options.",
				                                      "File Not Found", MessageBoxButtons.OK, MessageBoxIcon.Error,
				                                      MessageBoxDefaultButton.Button1, 0, null, null);
				Close();
				return;
			}

			connStrings = File.Exists(connFileName)
			              	? DbConnectionStrings.CreateFromFile(connFileName)
			              	: new DbConnectionStrings();

			UpdateTreeView();
		}

		private void ConnectionStringForm_FormClosing(object sender, FormClosingEventArgs e) {

		}

		private void btnApply_Click(object sender, EventArgs e) {
			connStrings.SaveToFile(connFileName);
			UpdateTreeView();
		}

		private void btnDiscard_Click(object sender, EventArgs e) {
			connStrings = DbConnectionStrings.CreateFromFile(connFileName);
			UpdateTreeView();
		}

		private void btnNew_Click(object sender, EventArgs e) {
			string newConnectionName = "New Connection";
			if (++unnamedConns > 0)
				newConnectionName += " " + unnamedConns;

			DbConnectionString connectionString = new DbConnectionString();
			connectionString.Name = newConnectionName;

			TreeNode node = new TreeNode(newConnectionName);
			node.ImageIndex = 0;
			node.Tag = connectionString;

			connsTreeView.Nodes.Add(node);
			connsTreeView.SelectedNode = node;
		}

		private void btnTest_Click(object sender, EventArgs e) {
			TreeNode node = connsTreeView.SelectedNode;
			if (node == null) {
				services.HostWindow.DisplaySimpleMessageBox(this, "No connection was selected.", "Error");
				return;
			}

			DbConnectionString connectionString = node.Tag as DbConnectionString;
			if (connectionString == null) {
				services.HostWindow.DisplayMessageBox(this, "No connection was set for the node.", "Error", MessageBoxButtons.OK,
				                                      MessageBoxIcon.Error, MessageBoxDefaultButton.Button1, 0, null, null);
				return;
			}

			try {
				DeveelDbConnection connection = new DeveelDbConnection(connectionString.ConnectionString);
				connection.Open();
				connection.Close();

				services.HostWindow.DisplayMessageBox(this, "The connection was tested successfully and is valid.", "Success",
				                                      MessageBoxButtons.OK, MessageBoxIcon.Information,
				                                      MessageBoxDefaultButton.Button1, 0, null, null);
			} catch(Exception ex) {
				services.HostWindow.DisplayMessageBox(this, "The connection is invalid: " + ex.Message, "Error",
				                                      MessageBoxButtons.OK, MessageBoxIcon.Error, MessageBoxDefaultButton.Button1, 0,
				                                      null, null);
			}
		}

		private void btnDuplicate_Click(object sender, EventArgs e) {

		}

		private void btnClose_Click(object sender, EventArgs e) {
			if (dirty && PromptModificationsChanges())
				return;

			Close();
		}

		private void btnDelete_Click(object sender, EventArgs e) {
			TreeNode node = connsTreeView.SelectedNode;
			if (node == null) {
				services.HostWindow.DisplaySimpleMessageBox(services.HostWindow.Form, "No connection was selected.", "Error");
				return;
			}

			DbConnectionString connString = node.Tag as DbConnectionString;
			if (connString == null)
				return;

			DialogResult result = services.HostWindow.DisplayMessageBox(this,
			                                                            "Are you sure you want to delete the connection '" +
			                                                            connString.Name + "'?",
			                                                            "Confirm Deletion", MessageBoxButtons.YesNo,
			                                                            MessageBoxIcon.Question,
			                                                            MessageBoxDefaultButton.Button2, 0, null, null);

			if (result == DialogResult.No)
				return;

			try {
				connStrings.RemoveConnection(connString.Name);

				TreeNode nextNode = node.NextVisibleNode;
				node.Remove();

				if (nextNode != null) {
					connsTreeView.SelectedNode = node;
				} else {
					ClearForm();
				}
			} catch (Exception) {
				services.HostWindow.DisplayMessageBox(services.HostWindow.Form, "It was impossible to remove the connection.",
				                                      "Error", MessageBoxButtons.OK, MessageBoxIcon.Error,
				                                      MessageBoxDefaultButton.Button1, 0, null, null);
			}
		}

		private void ClearForm() {
			txtConnName.Clear();
			txtDatabase.Clear();
			txtSchema.Clear();
			txtUser.Clear();
			txtHost.Clear();
			txtPass.Clear();
			txtPort.Clear();
			tabParameters.Enabled = false;
		}

		private bool PromptModificationsChanges() {
			DialogResult result = services.HostWindow.DisplayMessageBox(services.HostWindow.Form,
			                                                            "There are some uncommitted changes in the current connection: do you want to save it?",
			                                                            "Save?", MessageBoxButtons.YesNo, MessageBoxIcon.Warning,
			                                                            MessageBoxDefaultButton.Button1, 0, null, null);
			return result == DialogResult.Yes;
		}

		private void UpdateTreeView() {
			foreach (DbConnectionString connectionString in connStrings.Strings) {
				TreeNode connNode = new TreeNode(connectionString.Name);
				connNode.ImageIndex = 0;
				connNode.Tag = connectionString;
				connNode.ToolTipText = connectionString.ConnectionString;

				ConnectionString connString = new ConnectionString(connectionString.ConnectionString);

				TreeNode child;
				if (connString.Database != null && connString.Database.Length > 0) {
					child = new TreeNode(connString.Database);
					child.ImageIndex = 1;
					connNode.Nodes.Add(child);
				}

				if (connString.Host != null && connString.Host.Length > 0) {
					child = new TreeNode(connString.Host);
					child.ImageIndex = 2;
					connNode.Nodes.Add(child);
				}

				if (connString.UserName != null && connString.UserName.Length > 0) {
					child = new TreeNode(connString.UserName);
					child.ImageIndex = 3;
					connNode.Nodes.Add(child);
				}

				connsTreeView.Nodes.Add(connNode);
			}
		}

		private void SetDirty(bool status) {
			if (dirty != status) {
				string text = Text;
				dirty = status;
				if (dirty) {
					text += " *";
					btnApply.Enabled = true;
					btnDiscard.Enabled = true;
					btnTest.Enabled = true;
				} else if (text.EndsWith(" *")) {
					text = text.Substring(0, text.Length - 2);
					btnApply.Enabled = false;
					btnDiscard.Enabled = false;
					btnTest.Enabled = false;
				}

				Text = text;
			}
		}

		private void txtConnName_TextChanged(object sender, EventArgs e) {
			SetDirty(true);
		}

		private void txtUser_TextChanged(object sender, EventArgs e) {
			SetDirty(true);
		}

		private void txtHost_TextChanged(object sender, EventArgs e) {
			SetDirty(true);
		}

		private void txtDatabase_TextChanged(object sender, EventArgs e) {
			SetDirty(true);
		}

		private void connsTreeView_BeforeSelect(object sender, TreeViewCancelEventArgs e) {
			if (dirty) {
				if (!PromptModificationsChanges()) {
					SaveCurrentConnection();
					SetDirty(false);
				} else {
					e.Cancel = true;
				}
			}
		}

		private void SaveCurrentConnection() {
			TreeNode node = connsTreeView.SelectedNode;
			if (node == null)
				return;

			DbConnectionString connString = node.Tag as DbConnectionString;
			if (connString == null)
				return;

			connString.Name = txtComment.Text;
			connString.Comment = txtComment.Text;

			ConnectionString connectionString = new ConnectionString();
			connectionString.UserName = txtUser.Text;
			connectionString.Password = txtPass.Text;
			connectionString.Database = txtDatabase.Text;
			connectionString.Host = txtHost.Text;
			if (txtPort.Text.Length > 0)
				connectionString.Port = Int32.Parse(txtPort.Text);
			connectionString.Schema = txtSchema.Text;

			connString.ConnectionString = connectionString.ToString();

			connStrings.RemoveConnection(connString.Name);
			connStrings.AddConnection(connString);
		}

		private void connsTreeView_AfterSelect(object sender, TreeViewEventArgs e) {
			DbConnectionString connectionString = e.Node.Tag as DbConnectionString;
			if (connectionString == null)
				return;

			if (!tabParameters.Enabled)
				tabParameters.Enabled = true;

			BindConnection(connectionString);
		}

		private void BindConnection(DbConnectionString connectionString) {
			txtConnName.Text = connectionString.Name;
			txtComment.Text = connectionString.Comment;

			string connStringText = connectionString.ConnectionString;

			if (connStringText != null && connStringText.Length > 0) {
				ConnectionString connString = new ConnectionString(connectionString.ConnectionString);
				txtUser.Text = connString.UserName;
				txtPass.Text = connString.Password;
				txtHost.Text = connString.Host;
				txtDatabase.Text = connString.Database;
				txtSchema.Text = connString.Schema;
				if (connString.Port > 0)
					txtPort.Text = connString.Port.ToString();
			}

			btnDelete.Enabled = true;
			btnTest.Enabled = true;
		}
	}
}
