// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration.Install;
using System.IO;
using System.Reflection;
using System.Xml;

using Microsoft.Win32;

namespace Deveel.Data.Client {
	[RunInstaller(true)]
	public sealed class DeveelDbClientInstaller : Installer {
		private static string GetMachineConfigFile() {
			object value = Registry.GetValue("HKEY_LOCAL_MACHINE\\Software\\Microsoft\\.NETFramework\\", "InstallRoot", null);
			if (value == null)
				throw new InvalidOperationException("No .NET Framework installed in this machine.");

			string installRoot = value.ToString();
			string configFile = Path.Combine("CONFIG", "machine.config");

			// is it there any 2.0 installed?
			string machineConfigFile = Path.Combine(installRoot, Path.Combine("2.0", configFile));
			if (!File.Exists(machineConfigFile))
				throw new InvalidOperationException("Unable to find the machine.conf file.");

			return machineConfigFile;
		}

		private static void AddToDocument(XmlDocument doc) {
			XmlElement factoryNode = doc.CreateElement("add");
			factoryNode.SetAttribute("name", "DeveelDB Embedded Client");
			factoryNode.SetAttribute("invariant", "deveeldb");
			factoryNode.SetAttribute("description", "Embedded ADO.NET client for DeveelDB");

			Assembly assembly = Assembly.GetExecutingAssembly();
			string typeName = "Deveel.Data.Client.DeveelDbProviderFactory, " + assembly.FullName;
			factoryNode.SetAttribute("type", typeName);

			XmlNode dbProvidersNode = doc.SelectSingleNode("/configuration/system.data/DbProviderFactories");
			if (dbProvidersNode == null)
				throw new InvalidOperationException(
					"Cannot find the <system.data>/<DbProviderFactories> element in the machine.config.");

			for (int i = 0; i < dbProvidersNode.ChildNodes.Count; i++) {
				XmlElement addNode = (XmlElement) dbProvidersNode.ChildNodes[i];
				if (addNode.Attributes == null)
					continue;

				XmlAttribute invariantAttr = addNode.Attributes["invariant"];
				if (invariantAttr == null)
					continue;

				if (invariantAttr.Value == "deveeldb") {
					dbProvidersNode.RemoveChild(addNode);
					break;
				}
			}

			dbProvidersNode.AppendChild(factoryNode);
		}

		private static void RemoveFromDocument(XmlDocument doc) {
			XmlNode dbProvidersNode = doc.SelectSingleNode("/configuration/system.data/DbProviderFactories");
			if (dbProvidersNode == null)
				throw new InvalidOperationException(
					"Cannot find the <system.data>/<DbProviderFactories> element in the machine.config.");

			for (int i = 0; i < dbProvidersNode.ChildNodes.Count; i++) {
				XmlElement addNode = (XmlElement)dbProvidersNode.ChildNodes[i];
				if (addNode.Attributes == null)
					continue;

				XmlAttribute invariantAttr = addNode.Attributes["invariant"];
				if (invariantAttr == null)
					continue;

				if (invariantAttr.Value == "deveeldb") {
					dbProvidersNode.RemoveChild(addNode);
					break;
				}
			}
		}

		private static void InstallIntoMachine() {
			string machineConfigFile = GetMachineConfigFile();

			FileStream fileStream = null;

			try {
				fileStream = new FileStream(machineConfigFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
				XmlDocument doc = new XmlDocument();
				doc.Load(fileStream);

				AddToDocument(doc);

				fileStream.Seek(0, SeekOrigin.Begin);
				doc.Save(fileStream);
				fileStream.Flush();
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
		}

		private void UninstallFromMachine() {
			string machineConfigFile = GetMachineConfigFile();

			FileStream fileStream = null;

			try {
				fileStream = new FileStream(machineConfigFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
				XmlDocument doc = new XmlDocument();
				doc.Load(fileStream);

				RemoveFromDocument(doc);

				fileStream.Seek(0, SeekOrigin.Begin);
				doc.Save(fileStream);
				fileStream.Flush();
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
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