using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace Deveel.Data {
	public partial class NewFileForm : Form {
		public NewFileForm(IFileEditorResolver resolver) {
			InitializeComponent();
			this.resolver = resolver;
		}

		private readonly IFileEditorResolver resolver;

		public FileEditorInfo FileEditorInfo {
			get { return lstFileTypes.SelectedItem as FileEditorInfo; }
		}

		public bool IsValid {
			get { return lstFileTypes.SelectedItem != null; }
		}

		private void NewFileForm_Load(object sender, EventArgs e) {
			lstFileTypes.DataSource = resolver.GetFileTypes();
		}

		private void lstFileTypes_DoubleClick(object sender, EventArgs e) {
			if (IsValid) {
				DialogResult = DialogResult.OK;
				Close();
			}
		}

		private void btnOk_Click(object sender, EventArgs e) {
			DialogResult = DialogResult.OK;
			Close();
		}

		private void lstFileTypes_SelectedValueChanged(object sender, EventArgs e) {
			btnOk.Enabled = IsValid;
		}

		private void btnCancel_Click(object sender, EventArgs e) {
			DialogResult = DialogResult.Cancel;
			Close();
		}
	}
}
