using System;
using System.Windows.Forms;

namespace Deveel.Data.Deveel.Data.Controls {
	public partial class ExceptionControl : UserControl {
		public ExceptionControl() {
			InitializeComponent();
		}

		public void SetError(Exception e) {
			lblMessage.Text = e.GetType().FullName;
			txtMessage.Text = e.Message;
			txtStackTrace.Text = e.StackTrace;
		}
	}
}
