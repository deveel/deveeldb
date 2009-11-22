//  
//  DeveelDbClientInstaller.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.


using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration.Install;

namespace Deveel.Data.Client {
	[RunInstaller(true)]
	public sealed class DeveelDbClientInstaller : Installer {

		private void InstallIntoMachine() {
			//TODO:
		}

		private void UninstallFromMachine() {
			
		}

		public override void Install(IDictionary stateSaver) {
			base.Install(stateSaver);

			InstallIntoMachine();
		}

		public override void Uninstall(IDictionary savedState) {
			base.Uninstall(savedState);

			UninstallFromMachine();
		}
	}
}