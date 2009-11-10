using System;
using System.Windows.Forms;

namespace Deveel.Data {
	static class Program {
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main() {
			Application.EnableVisualStyles();
			Application.SetCompatibleTextRenderingDefault(false);

			IApplicationServices services = ApplicationServices.Current;

			RegisterComponents(services);
		}

		private static void RegisterComponents(IApplicationServices services) {
		}
	}
}
