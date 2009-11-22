using System;
using System.Media;
using System.Windows.Forms;

namespace Deveel.Data.Search {
	public partial class GotoLineForm : Form {
		public GotoLineForm(IApplicationServices services) {
			InitializeComponent();

			this.services = services;
		}

		private readonly IApplicationServices services;
		private int totalLines;
		private int selectedLine;

		public int TotalLines {
			get { return totalLines; }
		}

		public int SelectedLine {
			get { return selectedLine; }
		}

		private void btnGo_Click(object sender, EventArgs e) {
			IBrowsableDocument doc = services.HostWindow.ActiveChild as IBrowsableDocument;
			if (doc != null) {
				int line;

				if (Int32.TryParse(txtLineNumber.Text, out line)) {
					int column = 0;
					line = System.Math.Abs(line - 1);
					if (doc.SetCursorPosition(line, column)) {
						Close();
					}
				}

				SystemSounds.Beep.Play();
			}
		}

		private void GotoLineForm_Load(object sender, EventArgs e) {
			IBrowsableDocument doc = services.HostWindow.ActiveChild as IBrowsableDocument;
			if (doc != null) {
				totalLines = doc.TotalLines;
				selectedLine = (doc.Line + 1);
				Text = string.Format("{0} (1-{1})", Text, totalLines);
			}
		}
	}
}
