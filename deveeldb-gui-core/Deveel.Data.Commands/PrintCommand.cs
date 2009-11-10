using System;
using System.Drawing.Printing;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public sealed class PrintCommand : Command {
		public PrintCommand() 
			: base("Print...") {
		}

		public override bool Enabled {
			get {
				IPrintable printable = HostWindow.ActiveChild as IPrintable;
				return (printable != null && printable.PrintDocument != null);
			}
		}

		public override void Execute() {
			IPrintable printable = HostWindow.ActiveChild as IPrintable;
			if (printable == null)
				return;

			PrintDocument document = printable.PrintDocument;
			if (document == null)
				return;

			PrintDialog dialog = new PrintDialog();

			try {
				dialog.Document = document;
				dialog.AllowSomePages = true;

				DialogResult result = dialog.ShowDialog(HostWindow.Form);
				if (result == DialogResult.OK)
					document.Print();
			} finally {
				dialog.Dispose();
			}
		}
	}
}