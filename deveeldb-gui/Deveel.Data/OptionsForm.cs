using System;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;

namespace Deveel.Data {
	public partial class OptionsForm : Form {
		public OptionsForm(IApplicationServices services, IHostWindow hostWindow) {
			InitializeComponent();

			this.services = services;
			this.hostWindow = hostWindow;
		}

		private readonly IApplicationServices services;
		private readonly IHostWindow hostWindow;
		private readonly ArrayList configurations = new ArrayList();

		private IConfiguration Configuration {
			get {
				return lstSettingsProviders.SelectedIndex != -1
				       	? configurations[lstSettingsProviders.SelectedIndex] as IConfiguration
				       	: null;
			}
		}

		private void OptionsForm_Load(object sender, EventArgs e) {
			Type[] cofigTypes = services.ConfigurationTypes;

			foreach (Type cofigType in cofigTypes) {
				configurations.Add(services.Resolve(cofigType.FullName, typeof(IConfiguration)));
			}

			foreach (IConfiguration configuration in configurations) {
				configuration.PropertyChanged += Configuration_PropertyChanged;
				lstSettingsProviders.Items.Add(configuration.Name);
			}

			if (lstSettingsProviders.Items.Count > 0)
				lstSettingsProviders.SelectedIndex = 0;
		}

		private void Configuration_PropertyChanged(object sender, PropertyChangedEventArgs e) {
			if (e.PropertyName == "HasChanges") {
				string title = "Options";
				if (((IConfiguration)sender).HasChanges)
					title += "*";
				Text = title;
			}
		}

		private void btnOk_Click(object sender, EventArgs e) {
			if (Configuration != null)
				Configuration.Save();

			DialogResult = DialogResult.OK;
			Close();
		}

		private void btnCancel_Click(object sender, EventArgs e) {
			DialogResult = DialogResult.Cancel;
			Close();
		}

		private void lstSettingsProviders_SelectedIndexChanged(object sender, EventArgs e) {
			if (Configuration != null) {
				bool change = true;
				if (Configuration.HasChanges) {
					DialogResult result = SaveChanges();
					if (result == DialogResult.Yes) {
						Configuration.Save();
					} else {
						change = false;
					}
				}

				if (change) {
					propertyGrid.SelectedObject = Configuration.Context;
				}
			}
		}

		private void OptionsForm_FormClosing(object sender, FormClosingEventArgs e) {
			if (Configuration != null) {
				if (Configuration.HasChanges) {
					DialogResult result = SaveChanges();
					if (result == DialogResult.Yes) {
						Configuration.Save();
					} else if (result == DialogResult.Cancel) {
						e.Cancel = true;
					}
				}
			}
		}

		private DialogResult SaveChanges() {
			return hostWindow.DisplayMessageBox(null, "There are some uncommitted changes: do you want to save them now?",
			                                    "Save Changes", MessageBoxButtons.YesNo,
			                                    MessageBoxIcon.Question, MessageBoxDefaultButton.Button1,
			                                    MessageBoxOptions.ServiceNotification, null, null);
		}
	}
}
