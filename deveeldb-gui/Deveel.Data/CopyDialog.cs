using System;
using System.Windows.Forms;

namespace Deveel.Data {
	public partial class CopyDialog : Form {
		public CopyDialog() {
			InitializeComponent();
		}

		public bool IncludeHeaders {
			get { return chkIncludeHeaders.Checked; }
		}

		public string Separator {
			get {
				if (rdbCsv.Checked)
					return ",";
				if (rdbTab.Checked)
					return "\t";
				if (rdbOther.Checked)
					return txtOther.Text;
				return String.Empty;
			}
		}
	}
}
