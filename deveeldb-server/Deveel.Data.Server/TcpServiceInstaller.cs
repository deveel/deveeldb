using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Deveel.Data.Server {
	[RunInstaller(true)]
	public sealed class TcpServiceInstaller : Installer {
		public TcpServiceInstaller() {
			processInstaller = new ServiceProcessInstaller();
			ServiceInstaller installer = new ServiceInstaller();

			processInstaller.Account = ServiceAccount.NetworkService;
			processInstaller.Password = null;
			processInstaller.Username = null;

			installer.DisplayName = "DeveelDB TCP Service";
			installer.StartType = ServiceStartMode.Automatic;

			installer.ServiceName = "DeveelDB TCP Service";

			Installers.Add(processInstaller);
			Installers.Add(installer);
		}

		private ServiceProcessInstaller processInstaller;

		protected override void OnBeforeInstall(IDictionary savedState) {
			string userName = (string) savedState["user"];
			string pass = (string) savedState["pass"];

			processInstaller.Username = userName;
			processInstaller.Password = pass;

			base.OnBeforeInstall(savedState);
		}
	}
}